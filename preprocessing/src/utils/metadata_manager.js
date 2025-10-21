/**
 * Metadata Manager
 * Manages a centralized JSON metadata file for all downloaded legal documents
 * across different data sources (Pakistan Code, Gazettes, Court Judgments, etc.)
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path to the centralized metadata file
const METADATA_FILE = path.resolve(__dirname, '../../../data/metadata/documents_metadata.json');

/**
 * Initialize metadata file if it doesn't exist
 */
export function initializeMetadata() {
  const metadataDir = path.dirname(METADATA_FILE);
  
  // Create metadata directory if it doesn't exist
  if (!fs.existsSync(metadataDir)) {
    fs.mkdirSync(metadataDir, { recursive: true });
  }
  
  // Create metadata file with initial structure if it doesn't exist
  if (!fs.existsSync(METADATA_FILE)) {
    const initialData = {
      version: "1.0",
      last_updated: new Date().toISOString(),
      total_documents: 0,
      sources: {
        "pakistan-code": 0,
        "supreme-court": 0,
        "high-court": 0,
        "gazettes": 0,
        "other": 0
      },
      documents: []
    };
    fs.writeFileSync(METADATA_FILE, JSON.stringify(initialData, null, 2));
    console.log(`✅ Metadata file initialized at: ${METADATA_FILE}`);
  }
}

/**
 * Load existing metadata
 */
export function loadMetadata() {
  if (!fs.existsSync(METADATA_FILE)) {
    initializeMetadata();
  }
  const data = fs.readFileSync(METADATA_FILE, 'utf-8');
  return JSON.parse(data);
}

/**
 * Save metadata to file
 */
export function saveMetadata(metadata) {
  metadata.last_updated = new Date().toISOString();
  metadata.total_documents = metadata.documents.length;
  
  // Update source counts
  const sourceCounts = {};
  metadata.documents.forEach(doc => {
    const source = doc.source_website || 'other';
    sourceCounts[source] = (sourceCounts[source] || 0) + 1;
  });
  metadata.sources = sourceCounts;
  
  fs.writeFileSync(METADATA_FILE, JSON.stringify(metadata, null, 2));
}

/**
 * Add a new document to metadata
 * @param {Object} docInfo - Document information
 * @returns {string} - Document ID
 */
export function addDocument(docInfo) {
  const metadata = loadMetadata();
  
  // Generate unique document ID
  const docId = `doc_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  // Create document entry with all required fields
  const documentEntry = {
    id: docId,
    title: docInfo.title || 'Untitled',
    source_page: docInfo.source_page || '',
    source_url: docInfo.source_url || '',
    source_website: docInfo.source_website || 'pakistan-code',
    raw_path: docInfo.raw_path || '',
    text_path: docInfo.text_path || null,
    download_date: docInfo.download_date || new Date().toISOString(),
    content_type: docInfo.content_type || 'statute',
    section: docInfo.section || null,
    year: docInfo.year || null,
    court: docInfo.court || null,
    file_size: docInfo.file_size || null,
    file_format: docInfo.file_format || 'pdf',
    language: docInfo.language || 'english',
    status: docInfo.status || 'downloaded',
    processing_status: {
      text_extracted: false,
      cleaned: false,
      chunked: false,
      embedded: false
    }
  };
  
  // Add to documents array
  metadata.documents.push(documentEntry);
  
  // Save updated metadata
  saveMetadata(metadata);
  
  console.log(`✅ Added document to metadata: ${documentEntry.title} (ID: ${docId})`);
  return docId;
}

/**
 * Update document metadata
 */
export function updateDocument(docId, updates) {
  const metadata = loadMetadata();
  const docIndex = metadata.documents.findIndex(doc => doc.id === docId);
  
  if (docIndex === -1) {
    console.error(`❌ Document not found: ${docId}`);
    return false;
  }
  
  // Update document fields
  metadata.documents[docIndex] = {
    ...metadata.documents[docIndex],
    ...updates,
    last_modified: new Date().toISOString()
  };
  
  saveMetadata(metadata);
  console.log(`✅ Updated document: ${docId}`);
  return true;
}

/**
 * Check if a document already exists (by source URL)
 */
export function documentExists(sourceUrl) {
  const metadata = loadMetadata();
  return metadata.documents.some(doc => doc.source_url === sourceUrl);
}

/**
 * Get document by ID
 */
export function getDocument(docId) {
  const metadata = loadMetadata();
  return metadata.documents.find(doc => doc.id === docId);
}

/**
 * Get all documents from a specific source
 */
export function getDocumentsBySource(source) {
  const metadata = loadMetadata();
  return metadata.documents.filter(doc => doc.source_website === source);
}

/**
 * Get metadata statistics
 */
export function getStats() {
  const metadata = loadMetadata();
  return {
    total: metadata.total_documents,
    by_source: metadata.sources,
    by_content_type: metadata.documents.reduce((acc, doc) => {
      acc[doc.content_type] = (acc[doc.content_type] || 0) + 1;
      return acc;
    }, {}),
    by_status: metadata.documents.reduce((acc, doc) => {
      acc[doc.status] = (acc[doc.status] || 0) + 1;
      return acc;
    }, {})
  };
}

export default {
  initializeMetadata,
  loadMetadata,
  saveMetadata,
  addDocument,
  updateDocument,
  documentExists,
  getDocument,
  getDocumentsBySource,
  getStats
};
