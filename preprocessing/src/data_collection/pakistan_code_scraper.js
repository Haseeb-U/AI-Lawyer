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

// Configuration
const CONFIG = {
  startUrl: 'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-ap0%3D&action=primary&catid=17',
  baseUrl: 'https://pakistancode.gov.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/pakistan-code'),
  linkPatterns: {
    startsWith: 'https://pakistancode.gov.pk/english/UY2FqaJw1-apaUY2Fqa',
    endsWith: 'sg-jjjjjjjjjjjjj'
  },
  headless: false, // Visible browser as requested
  sourceWebsite: 'pakistan-code',
  delay: 2000 // Delay between downloads (2 seconds)
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
  
  // Initialize metadata system
  initializeMetadata();
  ensureDownloadDir();
  
  let browser;
  let downloadedCount = 0;
  let skippedCount = 0;
  let errorCount = 0;
  
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
    
    console.log(`📄 Navigating to: ${CONFIG.startUrl}\n`);
    await page.goto(CONFIG.startUrl, { waitUntil: 'networkidle2', timeout: 60000 });
    
    // Wait for page to load
    await wait(3000);
    
    console.log('🔍 Searching for law page links...\n');
    
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
    
    console.log(`✅ Found ${lawLinks.length} law page links\n`);
    
    if (lawLinks.length === 0) {
      console.log('⚠️  No links found matching the patterns. Please check the URL patterns.');
      return;
    }
    
    // Visit each law page and find PDF links
    for (let i = 0; i < lawLinks.length; i++) {
      const lawLink = lawLinks[i];
      console.log(`\n${'='.repeat(80)}`);
      console.log(`📖 [${i + 1}/${lawLinks.length}] Processing: ${lawLink.title}`);
      console.log(`🔗 URL: ${lawLink.url}`);
      console.log(`${'='.repeat(80)}\n`);
      
      try {
        // Navigate to law page
        await page.goto(lawLink.url, { waitUntil: 'networkidle2', timeout: 60000 });
        await wait(2000);
        
        // Find all PDF links on this page
        const pdfLinks = await page.evaluate(() => {
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
        
        console.log(`   📎 Found ${pdfLinks.length} PDF link(s) on this page\n`);
        
        // Download each PDF
        for (let j = 0; j < pdfLinks.length; j++) {
          const pdfLink = pdfLinks[j];
          const pdfUrl = pdfLink.url.startsWith('http') 
            ? pdfLink.url 
            : `${CONFIG.baseUrl}${pdfLink.url}`;
          
          // Check if already downloaded
          if (documentExists(pdfUrl)) {
            console.log(`   ⏭️  [${j + 1}/${pdfLinks.length}] Already downloaded: ${lawLink.title}`);
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
            
            console.log(`   ⬇️  [${j + 1}/${pdfLinks.length}] Downloading: ${documentTitle}`);
            console.log(`       URL: ${pdfUrl}`);
            console.log(`       File: ${filename}`);
            
            // Download the PDF
            await downloadFile(pdfUrl, filepath);
            
            // Get file size
            const stats = fs.statSync(filepath);
            const fileSizeKB = (stats.size / 1024).toFixed(2);
            
            console.log(`   ✅ Downloaded successfully (${fileSizeKB} KB)`);
            
            // Extract metadata from document title (law page title)
            const year = extractYear(documentTitle);
            const section = extractSection(documentTitle);
            
            // Add to metadata
            const docId = addDocument({
              title: documentTitle,
              source_page: lawLink.url,  // URL of the HTML page where PDF link was found
              source_url: pdfUrl,
              source_website: CONFIG.sourceWebsite,
              raw_path: path.relative(path.resolve(__dirname, '../../../'), filepath),
              text_path: null,
              download_date: new Date().toISOString(),
              content_type: 'statute',
              section: section,
              year: year,
              court: null,
              file_size: stats.size,
              file_format: 'pdf',
              language: 'english',
              status: 'downloaded'
            });
            
            console.log(`   📝 Metadata saved (ID: ${docId})\n`);
            downloadedCount++;
            
            // Delay between downloads
            await wait(CONFIG.delay);
            
          } catch (downloadError) {
            console.error(`   ❌ Error downloading PDF: ${downloadError.message}\n`);
            errorCount++;
          }
        }
        
      } catch (pageError) {
        console.error(`❌ Error processing law page: ${pageError.message}\n`);
        errorCount++;
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
  console.log('📊 SCRAPING SUMMARY');
  console.log('='.repeat(80));
  console.log(`✅ Successfully downloaded: ${downloadedCount} files`);
  console.log(`⏭️  Skipped (already exists): ${skippedCount} files`);
  console.log(`❌ Errors encountered: ${errorCount}`);
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
