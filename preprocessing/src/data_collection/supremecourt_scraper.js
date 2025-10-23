/**
 * Supreme Court Judgments Scraper
 * Downloads judgment PDFs from supremecourt.gov.pk using API requests
 * with detailed metadata tracking in a centralized JSON file
 * 
 * Usage: npm run scrape:supremecourt
 */

import axios from 'axios';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { 
  initializeMetadata, 
  addDocument, 
  documentExists,
  getDocumentBySourceUrl,
  validateDocumentEntry,
  updateDocumentFields,
  getStats 
} from '../utils/metadata_manager.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Helper function to wait
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Simple concurrency limiter
 * Limits the number of concurrent async operations
 */
class ConcurrencyLimiter {
  constructor(limit) {
    this.limit = limit;
    this.running = 0;
    this.queue = [];
  }

  async run(fn) {
    while (this.running >= this.limit) {
      await new Promise(resolve => this.queue.push(resolve));
    }
    this.running++;
    try {
      return await fn();
    } finally {
      this.running--;
      const resolve = this.queue.shift();
      if (resolve) resolve();
    }
  }
}

// Configuration
const CONFIG = {
  apiUrl: 'https://www.supremecourt.gov.pk/wp-content/plugins/my-plugin/online_judgments.php',
  downloadBaseUrl: 'https://www.supremecourt.gov.pk/downloads_judgements/',
  sourcePage: 'https://www.supremecourt.gov.pk/judgement-search/',
  downloadDir: path.resolve(__dirname, '../../../data/raw/supremecourt-judgments'),
  sourceWebsite: 'supremecourt',
  delay: 500, // Delay between requests in ms
  concurrentLimit: 3, // Number of concurrent downloads
  caseTypes: [
    'C.A.',
    'C.M.A.',
    'C.M.Appeal.',
    'C.P.L.A.',
    'C.R.P.',
    'C.Sh.A.',
    'C.Sh.P.',
    'C.Sh.R.P.',
    'C.P.',
    'D.S.A.',
    'H.R.C.',
    'H.R.M.A.',
    'I.C.A.',
    'Reference.',
    'S.M.C.',
    'S.M.R.P.',
    'C.U.O.'
  ],
  headers: {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://www.supremecourt.gov.pk',
    'referer': 'https://www.supremecourt.gov.pk/judgement-search/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
  }
};

/**
 * Ensure download directory exists
 */
function ensureDownloadDir() {
  if (!fs.existsSync(CONFIG.downloadDir)) {
    fs.mkdirSync(CONFIG.downloadDir, { recursive: true });
    console.log(`✅ Created download directory: ${CONFIG.downloadDir}`);
  }
}

/**
 * Sanitize filename for safe file system operations
 */
function sanitizeFilename(filename) {
  return filename
    .replace(/[<>:"/\\|?*]/g, '_') // Replace invalid characters
    .replace(/\s+/g, '_') // Replace spaces with underscores
    .replace(/_{2,}/g, '_') // Remove multiple underscores
    .replace(/\./g, '_') // Replace dots with underscores (except file extension)
    .substring(0, 200); // Limit length
}

/**
 * Download a file using axios with proper headers
 */
async function downloadFile(url, filepath) {
  try {
    const response = await axios({
      method: 'GET',
      url: url,
      responseType: 'stream',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.supremecourt.gov.pk/judgement-search/',
        'Origin': 'https://www.supremecourt.gov.pk'
      },
      timeout: 60000, // 60 seconds timeout
      maxRedirects: 5
    });

    const writer = fs.createWriteStream(filepath);
    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', () => resolve(filepath));
      writer.on('error', (err) => {
        fs.unlink(filepath, () => {}); // Delete incomplete file
        reject(err);
      });
    });
  } catch (error) {
    // Clean up incomplete file
    if (fs.existsSync(filepath)) {
      fs.unlinkSync(filepath);
    }
    throw error;
  }
}

/**
 * Extract year from case number or date
 */
function extractYear(text) {
  // Try to extract from case number format (e.g., C.A.1195/2015)
  const caseYearMatch = text.match(/\/(\d{4})/);
  if (caseYearMatch) {
    return parseInt(caseYearMatch[1]);
  }
  
  // Try to extract from date format (DD-MM-YYYY)
  const dateYearMatch = text.match(/\d{2}-\d{2}-(\d{4})/);
  if (dateYearMatch) {
    return parseInt(dateYearMatch[1]);
  }
  
  // Match 4-digit years
  const yearMatch = text.match(/\b(19|20)\d{2}\b/);
  return yearMatch ? parseInt(yearMatch[0]) : null;
}

/**
 * Fetch judgments for a specific case type
 */
async function fetchJudgments(caseType) {
  try {
    const response = await axios.post(
      CONFIG.apiUrl,
      new URLSearchParams({
        case_type: caseType,
        case_number: '',
        case_year: '',
        author_judge: '',
        doa: '',
        keywords: '',
        parties_name: '',
        tagline: '',
        citation: '',
        SCCitation: '',
        reported: ''
      }),
      {
        headers: CONFIG.headers,
        timeout: 30000 // 30 seconds timeout
      }
    );
    
    if (response.data && Array.isArray(response.data)) {
      return response.data;
    }
    
    return [];
  } catch (error) {
    console.error(`❌ Error fetching judgments for ${caseType}: ${error.message}`);
    return [];
  }
}

/**
 * Process and download a single judgment
 */
async function processJudgment(judgment, caseType) {
  const pdfUrl = `${CONFIG.downloadBaseUrl}${judgment.caseFileName}`;
  
  // Check if document exists and validate it
  const BASE_DIR = path.resolve(__dirname, '../../..');
  let shouldDownload = false;
  let isUpdate = false;
  
  if (documentExists(pdfUrl)) {
    // Document exists, validate it
    const validation = validateDocumentEntry(pdfUrl, BASE_DIR);
    
    if (validation.isValid) {
      // Document is valid and file exists, skip
      return { status: 'skipped', judgment };
    } else {
      // Document needs update or re-download
      isUpdate = true;
      shouldDownload = validation.needsRedownload;
      
      if (validation.needsRedownload) {
        console.log(`\n🔄 Re-downloading "${judgment.caseTitle}": ${validation.reason}`);
      } else if (validation.missingFields.length > 0) {
        console.log(`\n🔧 Updating metadata for "${judgment.caseTitle}": missing fields [${validation.missingFields.join(', ')}]`);
      }
    }
  } else {
    // New document, download it
    shouldDownload = true;
  }
  
  try {
    // Create filename from caseSubject + caseNumber + caseTitle
    const filenameParts = [
      judgment.caseSubject || 'Unknown',
      judgment.caseNumber || 'No_Number',
      judgment.caseTitle || 'Untitled'
    ];
    
    const sanitizedFilename = sanitizeFilename(filenameParts.join('_'));
    const filename = `${sanitizedFilename}.pdf`;
    const filepath = path.join(CONFIG.downloadDir, filename);
    
    // Download the PDF if needed
    if (shouldDownload) {
      await downloadFile(pdfUrl, filepath);
      await wait(CONFIG.delay); // Polite delay between downloads
    }
    
    // Get file size
    const stats = fs.statSync(filepath);
    
    // Extract year from case number or date
    const year = extractYear(judgment.caseNumber || judgment.dateOfAnnouncement || '');
    
    // Prepare document data
    const docData = {
      title: judgment.caseTitle || 'Untitled Case',
      source_page: CONFIG.sourcePage,
      source_url: pdfUrl,
      source_website: CONFIG.sourceWebsite,
      raw_path: path.relative(BASE_DIR, filepath),
      text_path: null,
      download_date: null, // Will be set by metadata_manager
      content_type: 'judgment',
      section: judgment.caseNumber || null,
      year: year,
      court: 'supremecourt',
      file_size: stats.size,
      file_format: 'pdf',
      language: 'english',
      status: 'downloaded'
    };
    
    // Add new document or update existing
    if (isUpdate) {
      updateDocumentFields(pdfUrl, docData);
      return { status: 'updated', judgment };
    } else {
      addDocument(docData);
      return { status: 'downloaded', judgment };
    }
    
  } catch (error) {
    console.error(`\n❌ Error processing "${judgment.caseTitle}": ${error.message}`);
    return { status: 'error', judgment, error: error.message };
  }
}

/**
 * Main scraping function
 */
async function scrapeSupremeCourt() {
  console.log('🚀 Starting Supreme Court Judgments Scraper...\n');
  console.log(`⚖️  Total case types to scrape: ${CONFIG.caseTypes.length}\n`);
  
  // Initialize metadata system
  initializeMetadata();
  ensureDownloadDir();
  
  let totalDownloaded = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let totalUpdated = 0;
  let totalRedownloaded = 0;
  let totalJudgments = 0;
  
  try {
    // Loop through each case type
    for (let i = 0; i < CONFIG.caseTypes.length; i++) {
      const caseType = CONFIG.caseTypes[i];
      
      console.log(`\n📂 Case Type ${i + 1}/${CONFIG.caseTypes.length}: ${caseType}`);
      
      // Fetch judgments for this case type
      const judgments = await fetchJudgments(caseType);
      console.log(`   Found ${judgments.length} judgments`);
      
      if (judgments.length === 0) {
        console.log('   ⚠️  No judgments found for this case type');
        continue;
      }
      
      totalJudgments += judgments.length;
      
      let downloadedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      let updatedCount = 0;
      let redownloadedCount = 0;
      
      // Create concurrency limiter
      const limiter = new ConcurrencyLimiter(CONFIG.concurrentLimit);
      
      // Process judgments in parallel with concurrency limit
      const processPromises = judgments.map(judgment =>
        limiter.run(async () => {
          const result = await processJudgment(judgment, caseType);
          
          if (result.status === 'downloaded') {
            downloadedCount++;
          } else if (result.status === 'updated') {
            updatedCount++;
          } else if (result.status === 'skipped') {
            skippedCount++;
          } else if (result.status === 'error') {
            errorCount++;
          }
          
          // Check if it was a re-download
          if (result.status === 'downloaded' && documentExists(result.judgment.caseFileName)) {
            redownloadedCount++;
          }
          
          // Show progress every 10 downloads/updates
          if ((downloadedCount + updatedCount) % 10 === 0) {
            process.stdout.write(`\r📥 Progress: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped`);
          }
          
          return result;
        })
      );
      
      // Wait for all parallel tasks to complete
      await Promise.all(processPromises);
      
      // Clear progress line and print case type summary
      process.stdout.write('\r' + ' '.repeat(80) + '\r');
      console.log(`✅ Case Type ${i + 1} complete: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped | ${errorCount} errors`);
      
      totalDownloaded += downloadedCount;
      totalUpdated += updatedCount;
      totalRedownloaded += redownloadedCount;
      totalSkipped += skippedCount;
      totalErrors += errorCount;
      
      // Delay between case types
      await wait(CONFIG.delay);
    }
    
  } catch (error) {
    console.error(`\n❌ Fatal error: ${error.message}`);
    console.error(error.stack);
  }
  
  // Print summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 FINAL SCRAPING SUMMARY');
  console.log('='.repeat(80));
  console.log(`⚖️  Total case types scraped: ${CONFIG.caseTypes.length}`);
  console.log(`📄 Total judgments found: ${totalJudgments}`);
  console.log(`✅ New downloads: ${totalDownloaded} files`);
  console.log(`🔄 Re-downloaded (missing files): ${totalRedownloaded} files`);
  console.log(`🔧 Metadata updated: ${totalUpdated} entries`);
  console.log(`⏭️  Skipped (already valid): ${totalSkipped} files`);
  console.log(`❌ Errors encountered: ${totalErrors}`);
  console.log(`📁 Download directory: ${CONFIG.downloadDir}`);
  
  // Print metadata statistics
  const stats = getStats();
  console.log('\n📈 METADATA STATISTICS:');
  console.log(`   Total documents: ${stats.total}`);
  const supremeCourtSource = stats.by_source['supremecourt'];
  if (supremeCourtSource) {
    console.log(`   Supreme Court documents: ${supremeCourtSource.count} (${supremeCourtSource.domain || 'domain unknown'})`);
  } else {
    console.log(`   Supreme Court documents: 0`);
  }
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapeSupremeCourt().catch(console.error);
