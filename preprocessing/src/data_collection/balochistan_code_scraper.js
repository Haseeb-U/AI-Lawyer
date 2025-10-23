/**
 * Balochistan Code PDF Scraper
 * Automatically downloads PDF files from balochistancode.gob.pk
 * with detailed metadata tracking in a centralized JSON file
 * 
 * Usage: npm run scrape:balochistan-code
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
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=4&dtyid=2',
      name: 'Agriculture and Cooperatives',
      id: 4
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=37&dtyid=2',
      name: 'Board of Revenue',
      id: 37
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=8&dtyid=2',
      name: 'Construction of Buildings, Physical Planning and Housing',
      id: 8
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=3&dtyid=2',
      name: 'Finance',
      id: 3
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=17&dtyid=2',
      name: 'Health',
      id: 17
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=19&dtyid=2',
      name: 'Industries and Commerce',
      id: 19
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=21&dtyid=2',
      name: 'Irrigation',
      id: 21
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=22&dtyid=2',
      name: 'Labour and Manpower',
      id: 22
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=2&dtyid=2',
      name: 'Law and Parliamentary Affairs',
      id: 2
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=24&dtyid=2',
      name: 'Local Government and Rural Development',
      id: 24
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=25&dtyid=2',
      name: 'Mines and Minerals Development',
      id: 25
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=26&dtyid=2',
      name: 'Planning and Development',
      id: 26
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=32&dtyid=2',
      name: 'Services and General Administration',
      id: 32
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=33&dtyid=2',
      name: 'Social Welfare, Special Education, Literacy, Non-Formal Education and Human Rights',
      id: 33
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=34&dtyid=2',
      name: 'Transport',
      id: 34
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=35&dtyid=2',
      name: 'Urban & Planning',
      id: 35
    },
    {
      url: 'https://balochistancode.gob.pk/Document.aspx?wise=srbdr&dtid=36&dtyid=2',
      name: 'Women Development',
      id: 36
    }
  ],
  baseUrl: 'https://balochistancode.gob.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/balochistan-code'),
  linkPatterns: {
    startsWith: 'https://balochistancode.gob.pk/Document.aspx?wise=opendoc&docid',
  },
  pdfPattern: {
    endsWith: '.pdf'
  },
  headless: false, // Visible browser
  sourceWebsite: 'balochistan-code',
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
 * Sanitize filename for safe file system operations
 */
function sanitizeFilename(filename) {
  return filename
    .replace(/[<>:"/\\|?*]/g, '_') // Replace invalid characters
    .replace(/\s+/g, '_') // Replace spaces with underscores
    .replace(/_{2,}/g, '_') // Remove multiple underscores
    .replace(/\.doc$/, '') // Remove .doc extension if present
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
 * Extract document title from h2 element
 * Format: <h2>The Baln Agricultural Produce Markets  Act I of 1991.doc, <span>1991</span></h2>
 */
function extractTitleFromH2(h2Text) {
  if (!h2Text) return null;
  
  // Remove the year span content
  let title = h2Text.replace(/<span>.*?<\/span>/gi, '').trim();
  
  // Remove .doc extension if present
  title = title.replace(/\.doc$/i, '').trim();
  
  // Remove trailing comma if present
  title = title.replace(/,\s*$/, '').trim();
  
  return title;
}

/**
 * Main scraping function
 */
async function scrapeBalochistanCode() {
  console.log('🚀 Starting Balochistan Code PDF Scraper...\n');
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
      const category = CONFIG.categoryUrls[catIndex];
      
      console.log(`\n📂 Category ${catIndex + 1}/${CONFIG.categoryUrls.length}: ${category.name} (ID: ${category.id})`);
      
      let downloadedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      let updatedCount = 0;
      let redownloadedCount = 0;
      
      try {
        await page.goto(category.url, { waitUntil: 'networkidle2', timeout: 60000 });
        await wait(2000);
        
        // Find all law links matching the pattern
        const lawLinks = await page.evaluate((pattern) => {
          const links = Array.from(document.querySelectorAll('a'));
          const matchedLinks = [];
          
          links.forEach(link => {
            const href = link.href;
            
            // Check if link matches the pattern
            if (href && href.startsWith(pattern.startsWith)) {
              matchedLinks.push({
                url: href,
                text: link.textContent.trim(),
                title: link.title || link.textContent.trim()
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
          
              // Extract document title from h2 element
              const h2Title = await taskPage.evaluate(() => {
                const h2 = document.querySelector('h2');
                if (h2) {
                  return h2.innerHTML;
                }
                return null;
              });
              
              const documentTitle = extractTitleFromH2(h2Title) || lawLink.title || lawLink.text || `document_${Date.now()}`;
          
              // Find all PDF links on this page
              const pdfLinks = await taskPage.evaluate((pdfPattern) => {
                const links = Array.from(document.querySelectorAll('a'));
                const pdfs = [];
            
                links.forEach(link => {
                  const href = link.href;
                  if (href && href.toLowerCase().endsWith(pdfPattern.endsWith)) {
                    pdfs.push({
                      url: href,
                      text: link.textContent.trim(),
                      title: link.title || link.textContent.trim()
                    });
                  }
                });
            
                return pdfs;
              }, CONFIG.pdfPattern);
          
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
                      console.log(`\n🔄 Re-downloading "${documentTitle}": ${validation.reason}`);
                    } else if (validation.missingFields.length > 0) {
                      console.log(`\n🔧 Updating metadata for "${documentTitle}": missing fields [${validation.missingFields.join(', ')}]`);
                    }
                  }
                } else {
                  // New document, download it
                  shouldDownload = true;
                }
            
                try {
                  // Use extracted title from h2 for filename
                  const sanitizedTitle = sanitizeFilename(documentTitle);
              
                  // Add index if multiple PDFs on same page
                  const filename = pdfLinks.length > 1 
                    ? `${sanitizedTitle}_${j + 1}.pdf` 
                    : `${sanitizedTitle}.pdf`;
              
                  const filepath = path.join(CONFIG.downloadDir, filename);
              
                  // Download the PDF if needed
                  if (shouldDownload) {
                    await downloadFile(pdfUrl, filepath);
                    if (isUpdate) {
                      redownloadedCount++;
                    }
                  }
              
                  // Get file size (from existing or newly downloaded file)
                  const stats = fs.statSync(filepath);
              
                  // Extract metadata from document title
                  const year = extractYear(documentTitle);
                  const section = extractSection(documentTitle);
              
                  // Prepare document data
                  const docData = {
                    title: documentTitle,
                    source_page: lawLink.url,
                    source_url: pdfUrl,
                    source_website: CONFIG.sourceWebsite,
                    category: category.name,
                    category_id: category.id,
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
                  console.error(`\n❌ Error processing "${documentTitle}": ${downloadError.message}`);
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
        console.error(`\n❌ Error processing category ${category.name}: ${categoryError.message}`);
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
  const balochistanSource = stats.by_source['balochistan-code'];
  if (balochistanSource) {
    console.log(`   Balochistan Code documents: ${balochistanSource.count} (${balochistanSource.domain || 'domain unknown'})`);
  } else {
    console.log(`   Balochistan Code documents: 0`);
  }
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapeBalochistanCode().catch(console.error);
