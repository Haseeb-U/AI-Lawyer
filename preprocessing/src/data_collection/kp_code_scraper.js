/**
 * KP Code PDF Scraper
 * Automatically downloads PDF files from kpcode.kp.gov.pk
 * with detailed metadata tracking in a centralized JSON file
 * 
 * Usage: npm run scrape:kp-code
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
  // Department URLs to scrape
  departmentUrls: [
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/16', name: 'Agriculture, Livestock and Cooperation Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/55', name: 'Agriculture & Cooperative / Livestock, Fisheries and Dairy Development' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/42', name: 'Board of Revenue' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/19', name: 'Finance Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/24', name: 'Health Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/37', name: 'Housing Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/25', name: 'Industries, Commerce and Technical Education Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/29', name: 'Irrigation Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/17', name: 'Law, Parliamentary Affairs and Human Rights Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/18', name: 'Local Government and Rural Development Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/45', name: 'Labour Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/56', name: 'Livestock, Fisheries and Cooperative Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/39', name: 'Planning and Development Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/13', name: 'Revenue and Estate Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/46', name: 'Transport Department' },
    { url: 'https://kpcode.kp.gov.pk/homepage/search_by_dept/44', name: 'Zakat, Usher, Social Welfare, Special Education & Women Empowerment Department' }
  ],
  baseUrl: 'https://kpcode.kp.gov.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/kp-code'),
  lawDetailsPattern: 'https://kpcode.kp.gov.pk/homepage/lawDetails/',
  pdfPattern: /\.pdf$/i,
  headless: false, // Visible browser
  sourceWebsite: 'kp-code',
  delay: 300, // 300ms delay for faster processing
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
 * Extract original filename from URL
 */
function extractFilename(url) {
  try {
    const urlObj = new URL(url);
    const pathname = urlObj.pathname;
    const filename = pathname.split('/').pop();
    return decodeURIComponent(filename);
  } catch (error) {
    // Fallback to basic extraction
    const parts = url.split('/');
    return decodeURIComponent(parts[parts.length - 1]);
  }
}

/**
 * Download a file using Node.js https/http module
 */
function downloadFile(url, filepath) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const file = fs.createWriteStream(filepath);
    
    protocol.get(url, (response) => {
      // Handle redirects
      if (response.statusCode === 301 || response.statusCode === 302) {
        const redirectUrl = response.headers.location;
        file.close();
        fs.unlink(filepath, () => {});
        downloadFile(redirectUrl, filepath).then(resolve).catch(reject);
        return;
      }
      
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
async function scrapeKPCode() {
  console.log('🚀 Starting KP Code PDF Scraper...\n');
  console.log(`📚 Total departments to scrape: ${CONFIG.departmentUrls.length}\n`);
  
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
    
    // Loop through each department
    for (let deptIndex = 0; deptIndex < CONFIG.departmentUrls.length; deptIndex++) {
      const department = CONFIG.departmentUrls[deptIndex];
      const deptId = department.url.match(/search_by_dept\/(\d+)/)?.[1] || 'unknown';
      
      console.log(`\n📂 Department ${deptIndex + 1}/${CONFIG.departmentUrls.length} (ID: ${deptId})`);
      console.log(`   ${department.name}`);
      
      let downloadedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      let updatedCount = 0;
      let redownloadedCount = 0;
      
      try {
        await page.goto(department.url, { waitUntil: 'networkidle2', timeout: 60000 });
        await wait(2000);
        
        // Find all law detail links
        const lawLinks = await page.evaluate((lawDetailsPattern) => {
          const links = Array.from(document.querySelectorAll('a'));
          const matchedLinks = [];
          
          links.forEach(link => {
            const href = link.href;
            const text = link.textContent.trim();
            
            // Check if link starts with lawDetails pattern
            if (href && href.startsWith(lawDetailsPattern)) {
              matchedLinks.push({
                url: href,
                text: text,
                title: link.title || text
              });
            }
          });
          
          // Remove duplicates based on URL
          const uniqueLinks = [];
          const seenUrls = new Set();
          matchedLinks.forEach(link => {
            if (!seenUrls.has(link.url)) {
              seenUrls.add(link.url);
              uniqueLinks.push(link);
            }
          });
          
          return uniqueLinks;
        }, CONFIG.lawDetailsPattern);
    
        console.log(`   Found ${lawLinks.length} law documents`);
    
        if (lawLinks.length === 0) {
          console.log('   ⚠️  No documents found in this department');
          continue;
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
              const pdfLinks = await taskPage.evaluate(() => {
                const links = Array.from(document.querySelectorAll('a'));
                const pdfs = [];
            
                links.forEach(link => {
                  const href = link.href;
                  if (href && href.toLowerCase().endsWith('.pdf')) {
                    pdfs.push({
                      url: href,
                      text: link.textContent.trim(),
                      title: link.title || link.textContent.trim()
                    });
                  }
                });
            
                return pdfs;
              });
          
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
                  const originalFilename = extractFilename(pdfUrl);
                  const filepath = path.join(CONFIG.downloadDir, originalFilename);
              
                  // Download the PDF if needed
                  if (shouldDownload) {
                    await downloadFile(pdfUrl, filepath);
                    if (isUpdate) {
                      redownloadedCount++;
                    }
                  }
              
                  // Get file size (from existing or newly downloaded file)
                  const stats = fs.statSync(filepath);
              
                  // Use law page title as document title
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
              
                  // Show progress every 10 downloads/updates
                  if ((downloadedCount + updatedCount) % 10 === 0) {
                    process.stdout.write(`\r📥 Progress: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped`);
                  }
              
                  // Small delay between downloads
                  await wait(CONFIG.delay);
              
                } catch (downloadError) {
                  console.error(`\n❌ Error processing PDF "${originalFilename}": ${downloadError.message}`);
                  errorCount++;
                }
              }
          
              // Close the task page
              await taskPage.close();
          
            } catch (pageError) {
              console.error(`\n❌ Error processing law page "${lawLink.title}": ${pageError.message}`);
              errorCount++;
            }
          })
        );
    
        // Wait for all parallel tasks to complete
        await Promise.all(processPromises);
        
        // Clear progress line and print department summary
        process.stdout.write('\r' + ' '.repeat(80) + '\r');
        console.log(`✅ Department ${deptIndex + 1} complete: ${downloadedCount} new | ${updatedCount} updated | ${skippedCount} skipped | ${errorCount} errors`);
        totalDownloaded += downloadedCount;
        totalUpdated += updatedCount;
        totalRedownloaded += redownloadedCount;
        totalSkipped += skippedCount;
        totalErrors += errorCount;
        
      } catch (departmentError) {
        console.error(`\n❌ Error processing department ${deptId}: ${departmentError.message}`);
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
  console.log(`📚 Total departments scraped: ${CONFIG.departmentUrls.length}`);
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
  const kpSource = stats.by_source['kp-code'];
  if (kpSource) {
    console.log(`   KP Code documents: ${kpSource.count} (${kpSource.domain || 'domain unknown'})`);
  } else {
    console.log(`   KP Code documents: 0`);
  }
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapeKPCode().catch(console.error);
