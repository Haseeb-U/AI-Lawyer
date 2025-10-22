/**
 * Sindh Human Rights Commission PDF Scraper
 * Downloads PDF files from shrc.org.pk with detailed metadata tracking
 * 
 * Usage: npm run scrape:shrc
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import https from 'https';
import http from 'http';
import { 
  initializeMetadata, 
  addDocument, 
  documentExists,
  validateDocumentEntry,
  updateDocumentFields,
  getStats 
} from '../utils/metadata_manager.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Helper function to wait
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Configuration
const CONFIG = {
  // Direct PDF URLs from SHRC website
  pdfUrls: [
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Protection-of-Parents-Ordinance-2021.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/Sindh-Empowerment-of-Persons-with-Disabilities-Act-2018.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Charities-Registration-and-Regulation-Act-2019.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Information-of-Temporary-Residents-Act-2015.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Protection-of-Communal-Properties-of-Minorities-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Sound-System-(Regulation)-Act-2015.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Gorakh-Hills-Development-Authority-Amendment-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Gorakh-Hills-Development-Authority-Act-2008.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Injured-Persons-Medical-Aid-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Injured-Persons-Compulsory-Medical-Treatment-Amal-Umer-Act-2019.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Newborn-Screening-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Prohibition-of-Corporal-Punishment-Act-2016.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Public-Private-Partnership-Amendment-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/The-Sindh-Public-Private-Partnership-Act-2010.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/Sindh-Letters-of-Administration-and-Succession-Certificates-Act.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/Sindh-Letters-of-Administration-and-Succession-Certificates-Rules.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/00-Sindh-Covid-19-Emergency-Relief-Ordinance-2020.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/Sindh-Protection-of-Human-Rights-(Amendment)-Act-2023.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/02-SHRC-Rules-of-Business.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/03-Sindh-Hindu-Marriage-(Amendment)-Act-2018.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/04-The-Sindh-Commission-on-the-Status-of-Women-Act-2015.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/05-The-Sindh-Senior-Citizens-Welfare-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/06-The-Sindh-Environmental-Protection-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/07-Sindh-Consumer-Protection-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/07a-Sindh-Consumer-Protection-Rules-2017.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/08-Sindh-Workers-Welfare-Fund-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/09-Sindh-Employees-Old-Age-Benefits-Act-2014.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/10-The-Sindh-Revenue-Board-Act-2010.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/11-The-Shaheed-Zulfiqar-Ali-Bhutto-University-of-Law-Karachi-Act-2012.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/12-The-Sindh-Mental-Health-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/13-The-Sindh-Protection-and-Promotion-of-Breast-Feeding-and-Child-Nutrituin-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/14-Sindh-Public-Procurement-(Amendment)-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/15-The-Hyderabad-Institute-of-Arts-Sciene-and-Technology-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/16-The-Qalandar-Shahbaz-University-of-Modern-Sciences-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/17-The-Shaheed-Benazir-Bhutto-Dewan-University-Act-2011.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/18-The-Sindh-Transplantation-of-Human-Organs-and-Tissues-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/19-The-Sindh-Peoples-Local-Government-Act-2012.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/20-The-Sindh-Civil-Servants-(Amendment)-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/21-The-Sindh-High-Density-Development-Board-(Amendment)-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/22-The-Sindh-Higher-Education-Commission-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/23-Sindh-Witness-Protection-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/24-Sindh-Regularization-of-Adhoc-&-Contract-Employees Act, 2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/25-Sindh-Domestic-Violence-(Prevention-and-Protection)-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/26-The-Sindh-Child-Marriages-Restraint-Act-2013.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/27-The-Sindh-Child-Marriages-Restraint-Rules-2016.pdf',
    'https://www.shrc.org.pk/downloads/laws/Sindh/29-Sindh-Child-Protection-Authority-Act-2011.pdf'
  ],
  baseUrl: 'https://www.shrc.org.pk',
  downloadDir: path.resolve(__dirname, '../../../data/raw/shrc-sindh'),
  sourceWebsite: 'shrc-sindh',
  delay: 500 // Delay between downloads in ms
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
 * Extract filename from URL keeping original name
 */
function getFilenameFromUrl(url) {
  const urlPath = new URL(url).pathname;
  const filename = path.basename(urlPath);
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
      // Handle redirects
      if (response.statusCode === 301 || response.statusCode === 302) {
        const redirectUrl = response.headers.location;
        resolve(downloadFile(redirectUrl, filepath));
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
 * Extract document title from filename
 * Removes file extension and cleans up the name
 */
function extractTitle(filename) {
  // Remove .pdf extension
  let title = filename.replace(/\.pdf$/i, '');
  
  // Remove number prefixes like "00-", "02-", etc.
  title = title.replace(/^\d+-/, '');
  
  // Replace hyphens and underscores with spaces
  title = title.replace(/[-_]/g, ' ');
  
  // Clean up multiple spaces
  title = title.replace(/\s+/g, ' ').trim();
  
  return title;
}

/**
 * Determine content type based on filename
 */
function determineContentType(filename) {
  const lower = filename.toLowerCase();
  
  if (lower.includes('rules')) {
    return 'rules';
  } else if (lower.includes('ordinance')) {
    return 'ordinance';
  } else if (lower.includes('act')) {
    return 'statute';
  } else {
    return 'statute'; // Default to statute
  }
}

/**
 * Main scraping function
 */
async function scrapeSHRC() {
  console.log('🚀 Starting Sindh Human Rights Commission PDF Scraper...\n');
  console.log(`📚 Total PDFs to download: ${CONFIG.pdfUrls.length}\n`);
  
  // Initialize metadata system
  initializeMetadata();
  ensureDownloadDir();
  
  let totalDownloaded = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let totalUpdated = 0;
  let totalRedownloaded = 0;
  
  // Base directory for validation
  const BASE_DIR = path.resolve(__dirname, '../../..');
  
  try {
    // Process each PDF URL
    for (let i = 0; i < CONFIG.pdfUrls.length; i++) {
      const pdfUrl = CONFIG.pdfUrls[i];
      const filename = getFilenameFromUrl(pdfUrl);
      const filepath = path.join(CONFIG.downloadDir, filename);
      
      console.log(`\n[${i + 1}/${CONFIG.pdfUrls.length}] Processing: ${filename}`);
      
      try {
        // Check if document exists and validate it
        let shouldDownload = false;
        let isUpdate = false;
        
        if (documentExists(pdfUrl)) {
          // Document exists, validate it
          const validation = validateDocumentEntry(pdfUrl, BASE_DIR);
          
          if (validation.isValid) {
            // Document is valid and file exists, skip
            console.log(`   ✅ Already exists and valid - skipping`);
            totalSkipped++;
            continue;
          } else {
            // Document needs update or re-download
            isUpdate = true;
            shouldDownload = validation.needsRedownload;
            
            if (validation.needsRedownload) {
              console.log(`   🔄 Re-downloading: ${validation.reason}`);
            } else if (validation.missingFields.length > 0) {
              console.log(`   🔧 Updating metadata: missing fields [${validation.missingFields.join(', ')}]`);
            }
          }
        } else {
          // New document, download it
          shouldDownload = true;
        }
        
        // Download the PDF if needed
        if (shouldDownload) {
          console.log(`   📥 Downloading...`);
          await downloadFile(pdfUrl, filepath);
          console.log(`   ✅ Downloaded successfully`);
          if (isUpdate) {
            totalRedownloaded++;
          }
        }
        
        // Get file size
        const stats = fs.statSync(filepath);
        
        // Extract metadata
        const title = extractTitle(filename);
        const year = extractYear(title);
        const section = extractSection(title);
        const contentType = determineContentType(filename);
        
        // Prepare document data
        const docData = {
          title: title,
          source_page: 'https://www.shrc.org.pk/laws-and-orders/',
          source_url: pdfUrl,
          source_website: CONFIG.sourceWebsite,
          raw_path: path.relative(path.resolve(__dirname, '../../../'), filepath),
          text_path: null,
          download_date: null, // Will be set by metadata_manager
          content_type: contentType,
          section: section,
          year: year,
          court: null,
          jurisdiction: 'Sindh',
          file_size: stats.size,
          file_format: 'pdf',
          language: 'english',
          status: 'downloaded'
        };
        
        // Add new document or update existing
        if (isUpdate) {
          updateDocumentFields(pdfUrl, docData);
          totalUpdated++;
          console.log(`   🔧 Metadata updated`);
        } else {
          addDocument(docData);
          totalDownloaded++;
          console.log(`   ✅ Added to metadata`);
        }
        
        // Small delay between downloads
        await wait(CONFIG.delay);
        
      } catch (error) {
        console.error(`   ❌ Error: ${error.message}`);
        totalErrors++;
      }
    }
    
  } catch (error) {
    console.error(`\n❌ Fatal error: ${error.message}`);
    console.error(error.stack);
  }
  
  // Print summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 FINAL SCRAPING SUMMARY');
  console.log('='.repeat(80));
  console.log(`📚 Total PDFs processed: ${CONFIG.pdfUrls.length}`);
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
  console.log(`   SHRC Sindh documents: ${stats.by_source['shrc-sindh'] || 0}`);
  console.log('='.repeat(80) + '\n');
}

// Run the scraper
scrapeSHRC().catch(console.error);
