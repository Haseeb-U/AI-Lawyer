/**
 * Punjab Code PDF Scraper
 * Automatically downloads PDF files from punjabcode.punjab.gov.pk
 * with detailed metadata tracking in a centralized JSON file
 * 
 * Usage: npm run scrape:punjab-code
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import https from 'https';
import http from 'http';
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

// Helper function to wait (replacement for page.waitForTimeout)
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
  // Multiple category URLs to scrape
  categoryUrls: [
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/1',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/4',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/5',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/6',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/10',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/12',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/16',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/19',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/20',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/22',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/24',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/26',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/31',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/38',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/40',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/41',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/43',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/54',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/56',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/60',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/61',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/62',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/64',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/66',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/70',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/71',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/73',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/75',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/77',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/80',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/84',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/86',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/89',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/93',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/98',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/101',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/102',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/103',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/108',
    'https://punjabcode.punjab.gov.pk/en/articles_by_category/110'
  ],
  baseUrl: 'https://punjabcode.punjab.gov.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/punjab-code'),
  linkPatterns: {
    lawPagePrefix: 'https://punjabcode.punjab.gov.pk/en/show_article/',
    pdfSuffix: '.pdf'
  },
  headless: false, // Visible browser
  sourceWebsite: 'punjab-code',
  delay: 300, // 300ms delay between downloads
  concurrentLimit: 3 // Process 3 documents concurrently
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
 * Sanitize filename for safe file system operations while preserving original name
 * Only replaces truly invalid characters
 */
function sanitizeFilename(filename) {
  return filename
    .replace(/[<>:"/\\|?*]/g, '_') // Replace only invalid filesystem characters
    .substring(0, 200); // Limit length
}

/**
 * Extract filename from URL
 */
function extractFilenameFromUrl(url) {
  const urlParts = url.split('/');
  const filename = urlParts[urlParts.length - 1];
  return decodeURIComponent(filename);
}

/**
 * Download a file using Node.js https/http module
 */
function downloadFile(url, filepath) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const file = fs.createWriteStream(filepath);
    
    protocol.get(url, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      
      response.pipe(file);
      
      file.on('finish', () => {
        file.close();
        resolve(filepath);
      });
    }).on('error', (err) => {
      fs.unlink(filepath, () => {}); // Delete incomplete file
      reject(err);
    });
    
    file.on('error', (err) => {
      fs.unlink(filepath, () => {});
      reject(err);
    });
  });
}

/**
 * Extract year from text using regex
 * Supports years from 1800s onwards (1800-2099)
 */
function extractYear(text) {
  // Match 4-digit years from 1800-2099
  const yearMatch = text.match(/\b(1[8-9]\d{2}|20\d{2})\b/);
  return yearMatch ? parseInt(yearMatch[0]) : null;
}

/**
 * Extract section number from text
 */
function extractSection(text) {
  const sectionMatch = text.match(/(?:Section|Sec\.?)\s*(\d+[A-Za-z]?)/i);
  return sectionMatch ? sectionMatch[1] : null;
}

/**
 * Main scraping function
 */
async function scrapePunjabCode() {
  console.log('🚀 Starting Punjab Code PDF Scraper...\n');
  console.log(`📚 Total categories to scrape: ${CONFIG.categoryUrls.length}\n`);
  
  // Initialize metadata system
  initializeMetadata();
  ensureDownloadDir();
  
  let browser;
  let totalDownloaded = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let totalUpdated = 0;
  let totalRedownloaded = 0;
  
  // Base directory for validation
  const BASE_DIR = path.resolve(__dirname, '../../..');
  
  try {
    // Launch browser in visible mode
    console.log('🌐 Launching browser (visible mode)...');
    browser = await puppeteer.launch({
      headless: CONFIG.headless,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      defaultViewport: { width: 1280, height: 800 }
    });
    
    const page = await browser.newPage();
    
    // Set a realistic user agent
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    
    // Loop through each category
    for (let catIndex = 0; catIndex < CONFIG.categoryUrls.length; catIndex++) {
      const categoryUrl = CONFIG.categoryUrls[catIndex];
      const catId = categoryUrl.match(/category\/(\d+)/)?.[1] || 'unknown';
      
      console.log(`\n📂 Category ${catIndex + 1}/${CONFIG.categoryUrls.length} (ID: ${catId})`);
      
      let downloadedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      let updatedCount = 0;
      let redownloadedCount = 0;
    
      try {
        await page.goto(categoryUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        await wait(2000);
      
      // Find all law page links matching the pattern
      const lawLinks = await page.evaluate((prefix) => {
        const links = Array.from(document.querySelectorAll('a'));
        const matchedLinks = [];
        
        links.forEach(link => {
          const href = link.href;
          const text = link.textContent.trim();
          
          // Check if link matches the law page pattern
          if (href && href.startsWith(prefix)) {
            matchedLinks.push({
              url: href,
              text: text,
              title: link.title || text
            });
          }
        });
        
        return matchedLinks;
      }, CONFIG.linkPatterns.lawPagePrefix);
  
      console.log(`✅ Found ${lawLinks.length} law documents\n`);
  
      if (lawLinks.length === 0) {
        console.log('⚠️  No documents found on category page');
        return;
      }
  
      // Create concurrency limiter
      const limiter = new ConcurrencyLimiter(CONFIG.concurrentLimit);
  
      // Process law pages in parallel with concurrency limit
      const processPromises = lawLinks.map((lawLink, i) => 
        limiter.run(async () => {
          try {
            // Create a new page for each concurrent task
            const taskPage = await browser.newPage();
            await taskPage.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
            // Navigate to law page
            await taskPage.goto(lawLink.url, { waitUntil: 'networkidle2', timeout: 60000 });
            await wait(500);
        
            // Find all PDF links on this page
            const pdfLinks = await taskPage.evaluate((pdfSuffix) => {
              const links = Array.from(document.querySelectorAll('a'));
              const pdfs = [];
          
              links.forEach(link => {
                const href = link.href;
                if (href && href.toLowerCase().endsWith(pdfSuffix)) {
                  pdfs.push({
                    url: href,
                    text: link.textContent.trim(),
                    title: link.title || link.textContent.trim()
                  });
                }
              });
          
              return pdfs;
            }, CONFIG.linkPatterns.pdfSuffix);
        
            // Download each PDF
            for (let j = 0; j < pdfLinks.length; j++) {
              const pdfLink = pdfLinks[j];
              const pdfUrl = pdfLink.url.startsWith('http') 
                ? pdfLink.url 
                : `${CONFIG.baseUrl}${pdfLink.url}`;
          
              // Check if document exists and validate it
              let shouldDownload = false;
              let isUpdate = false;
              
              if (documentExists(pdfUrl)) {
                // Document exists, validate it
                const validation = validateDocumentEntry(pdfUrl, BASE_DIR);
                
                if (validation.isValid) {
                  // Document is valid and file exists, skip
                  skippedCount++;
                  continue;
                } else {
                  // Document needs update or re-download
                  isUpdate = true;
                  shouldDownload = validation.needsRedownload;
                  
                  if (validation.needsRedownload) {
                    console.log(`\n🔄 Re-downloading "${lawLink.title}": ${validation.reason}`);
                  } else if (validation.missingFields.length > 0) {
                    console.log(`\n🔧 Updating metadata for "${lawLink.title}": missing fields [${validation.missingFields.join(', ')}]`);
                  }
                }
              } else {
                // New document, download it
                shouldDownload = true;
              }
          
              try {
                // Extract original filename from URL
                const originalFilename = extractFilenameFromUrl(pdfUrl);
                const sanitizedFilename = sanitizeFilename(originalFilename);
            
                const filepath = path.join(CONFIG.downloadDir, sanitizedFilename);
            
                // Download the PDF if needed
                if (shouldDownload) {
                  await downloadFile(pdfUrl, filepath);
                  if (isUpdate) {
                    redownloadedCount++;
                  }
                }
            
                // Get file size (from existing or newly downloaded file)
                const stats = fs.statSync(filepath);
            
                // Use law page title for document title, fallback to filename
                const documentTitle = lawLink.title || lawLink.text || originalFilename.replace('.pdf', '');
                
                // Extract metadata from document title
                const year = extractYear(documentTitle);
                const section = extractSection(documentTitle);
            
                // Prepare document data
                const docData = {
                  title: documentTitle,
                  source_page: lawLink.url,
                  source_url: pdfUrl,
                  source_website: CONFIG.sourceWebsite,
                  raw_path: path.relative(path.resolve(__dirname, '../../../'), filepath),
                  text_path: null,
                  download_date: null, // Will be set by metadata_manager with proper format
                  content_type: 'statute',
                  section: section,
                  year: year,
                  court: null,
                  file_size: stats.size,
                  file_format: 'pdf',
                  language: 'english',
                  status: 'downloaded'
                };
                
                // Add new document or update existing
                if (isUpdate) {
                  updateDocumentFields(pdfUrl, docData);
                  updatedCount++;
                } else {
                  addDocument(docData);
                  downloadedCount++;
                }
                
                // Show progress every 5 downloads/updates
                if ((downloadedCount + updatedCount) % 5 === 0) {
                  process.stdout.write(`\r📥 Progress: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped`);
                }
            
                // Small delay between downloads
                await wait(CONFIG.delay);
            
              } catch (downloadError) {
                console.error(`\n❌ Error processing "${lawLink.title}": ${downloadError.message}`);
                errorCount++;
              }
            }
        
            // Close the task page
            await taskPage.close();
        
          } catch (pageError) {
            console.error(`\n❌ Error processing "${lawLink.title}": ${pageError.message}`);
            errorCount++;
          }
        })
      );
  
      // Wait for all parallel tasks to complete
      await Promise.all(processPromises);
      
      // Clear progress line and print category summary
      process.stdout.write('\r' + ' '.repeat(80) + '\r');
      console.log(`✅ Category ${catIndex + 1} complete: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped | ${errorCount} errors`);
      totalDownloaded += downloadedCount;
      totalUpdated += updatedCount;
      totalRedownloaded += redownloadedCount;
      totalSkipped += skippedCount;
      totalErrors += errorCount;
      
    } catch (categoryError) {
      console.error(`\n❌ Error processing category ${catId}: ${categoryError.message}`);
      totalErrors++;
    }
  }
    
  } catch (error) {
    console.error(`\n❌ Fatal error: ${error.message}`);
    console.error(error.stack);
  } finally {
    if (browser) {
      await browser.close();
      console.log('\n🌐 Browser closed');
    }
  }
  
  // Print summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 FINAL SCRAPING SUMMARY');
  console.log('='.repeat(80));
  console.log(`📚 Total categories scraped: ${CONFIG.categoryUrls.length}`);
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
  const punjabSource = stats.by_source['punjab-code'];
  if (punjabSource) {
    console.log(`   Punjab Code documents: ${punjabSource.count} (${punjabSource.domain || 'domain unknown'})`);
  } else {
    console.log(`   Punjab Code documents: 0`);
  }
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapePunjabCode().catch(console.error);
