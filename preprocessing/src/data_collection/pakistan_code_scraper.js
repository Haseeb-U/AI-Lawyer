/**
 * Pakistan Code PDF Scraper
 * Automatically downloads PDF files from pakistancode.gov.pk
 * with detailed metadata tracking in a centralized JSON file
 * 
 * Usage: npm run scrape:pakistan-code
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
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-aw%3D%3D&action=primary&catid=2',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-bA%3D%3D&action=primary&catid=3',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-bg%3D%3D&action=primary&catid=5',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-cA%3D%3D&action=primary&catid=7',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-cQ%3D%3D&action=primary&catid=8',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-apY%3D&action=primary&catid=10',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-apc%3D&action=primary&catid=11',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-apg%3D&action=primary&catid=12',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-ap0%3D&action=primary&catid=17',
    'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-ap8%3D&action=primary&catid=19'
  ],
  baseUrl: 'https://pakistancode.gov.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/pakistan-code'),
  linkPatterns: {
    startsWith: 'https://pakistancode.gov.pk/english/UY2FqaJw1-apaUY2Fqa',
    endsWith: 'sg-jjjjjjjjjjjjj'
  },
  headless: false, // Visible browser as requested
  sourceWebsite: 'pakistan-code',
  delay: 300, // Reduced delay to 300ms for faster processing
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
 * Sanitize filename for safe file system operations
 */
function sanitizeFilename(filename) {
  return filename
    .replace(/[<>:"/\\|?*]/g, '_') // Replace invalid characters
    .replace(/\s+/g, '_') // Replace spaces with underscores
    .replace(/_{2,}/g, '_') // Remove multiple underscores
    .substring(0, 200); // Limit length
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
 */
function extractYear(text) {
  const yearMatch = text.match(/\b(19|20)\d{2}\b/);
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
async function scrapePakistanCode() {
  console.log('🚀 Starting Pakistan Code PDF Scraper...\n');
  console.log(`📚 Total categories to scrape: ${CONFIG.categoryUrls.length}\n`);
  
  // Initialize metadata system
  initializeMetadata();
  ensureDownloadDir();
  
  let browser;
  let totalDownloaded = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  
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
      const catId = categoryUrl.match(/catid=(\d+)/)?.[1] || 'unknown';
      
      console.log(`\n📂 Category ${catIndex + 1}/${CONFIG.categoryUrls.length} (ID: ${catId})`);
      
      let downloadedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      
      try {
        await page.goto(categoryUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        await wait(2000);
        
        // Find all links matching the patterns
        const lawLinks = await page.evaluate((patterns) => {
          const links = Array.from(document.querySelectorAll('a'));
          const matchedLinks = [];
          
          links.forEach(link => {
            const href = link.href;
            const text = link.textContent.trim();
            
            // Check if link matches either pattern
            if (href && (
              href.startsWith(patterns.startsWith) || 
              href.endsWith(patterns.endsWith)
            )) {
              matchedLinks.push({
                url: href,
                text: text,
                title: link.title || text
              });
            }
          });
          
          return matchedLinks;
        }, CONFIG.linkPatterns);
    
        console.log(`   Found ${lawLinks.length} documents`);
    
        if (lawLinks.length === 0) {
          console.log('   ⚠️  No documents found in this category');
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
            
                // Check if already downloaded
                if (documentExists(pdfUrl)) {
                  skippedCount++;
                  continue;
                }
            
                try {
                  // Use law page title for filename and document title
                  const documentTitle = lawLink.title || lawLink.text || `document_${Date.now()}`;
                  const sanitizedTitle = sanitizeFilename(documentTitle);
              
                  // Add index if multiple PDFs on same page
                  const filename = pdfLinks.length > 1 
                    ? `${sanitizedTitle}_${j + 1}.pdf` 
                    : `${sanitizedTitle}.pdf`;
              
                  const filepath = path.join(CONFIG.downloadDir, filename);
              
                  // Download the PDF
                  await downloadFile(pdfUrl, filepath);
              
                  // Get file size
                  const stats = fs.statSync(filepath);
              
                  // Extract metadata from document title (law page title)
                  // const year = extractYear(documentTitle);
                  const section = extractSection(documentTitle);
              
                  // Add to metadata
                  addDocument({
                    title: documentTitle,
                    source_page: lawLink.url,  // URL of the HTML page where PDF link was found
                    source_url: pdfUrl,
                    source_website: CONFIG.sourceWebsite,
                    raw_path: path.relative(path.resolve(__dirname, '../../../'), filepath),
                    text_path: null,
                    download_date: null, // Will be set by metadata_manager with proper format
                    content_type: 'statute',
                    section: section,
                    year: null, // Year extraction commented out
                    court: null,
                    file_size: stats.size,
                    file_format: 'pdf',
                    language: 'english',
                    status: 'downloaded'
                  });
              
                  downloadedCount++;
                  
                  // Show progress every 10 downloads
                  if (downloadedCount % 10 === 0) {
                    process.stdout.write(`\r📥 Progress: ${downloadedCount} downloaded | ${skippedCount} skipped`);
                  }
              
                  // Small delay between downloads
                  await wait(CONFIG.delay);
              
                } catch (downloadError) {
                  console.error(`\n❌ Error downloading "${lawLink.title}": ${downloadError.message}`);
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
        console.log(`✅ Category ${catIndex + 1} complete: ${downloadedCount} downloaded | ${skippedCount} skipped | ${errorCount} errors`);
        totalDownloaded += downloadedCount;
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
  console.log(`✅ Successfully downloaded: ${totalDownloaded} files`);
  console.log(`⏭️  Skipped (already exists): ${totalSkipped} files`);
  console.log(`❌ Errors encountered: ${totalErrors}`);
  console.log(`📁 Download directory: ${CONFIG.downloadDir}`);
  
  // Print metadata statistics
  const stats = getStats();
  console.log('\n📈 METADATA STATISTICS:');
  console.log(`   Total documents: ${stats.total}`);
  console.log(`   Pakistan Code documents: ${stats.by_source['pakistan-code'] || 0}`);
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapePakistanCode().catch(console.error);
