"""
Text Extraction Script for Smart Lawyer
Extracts text from PDF files in raw folder and saves to text folder
Handles both digital PDFs and scanned documents using OCR
Uses: PyMuPDF, pdfminer, and Tesseract (as mentioned in proposal)
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import fitz  # PyMuPDF
from pdfminer.high_level import extract_text as pdfminer_extract
from PIL import Image
import pytesseract
import io

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
TEXT_DIR = BASE_DIR / "data" / "text"
METADATA_FILE = BASE_DIR / "data" / "metadata" / "documents_metadata.json"

# Supported file extensions
SUPPORTED_EXTENSIONS = ['.pdf']


class TextExtractor:
    """Handles text extraction from various document formats"""
    
    def __init__(self):
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'ocr_used': 0
        }
    
    def extract_from_pdf(self, pdf_path):
        """
        Extract text from PDF using multiple methods
        1. Try PyMuPDF (fast, good for digital PDFs)
        2. Fall back to pdfminer if needed
        3. Use OCR for scanned pages
        """
        text_content = []
        ocr_used = False
        
        try:
            # Method 1: Try PyMuPDF first (fastest)
            doc = fitz.open(pdf_path)
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                page_text = page.get_text()
                
                # Check if page has extractable text
                if page_text.strip():
                    text_content.append(page_text)
                else:
                    # Page appears to be scanned - use OCR
                    ocr_text = self._ocr_page(page)
                    if ocr_text:
                        text_content.append(ocr_text)
                        ocr_used = True
            
            doc.close()
            
            # If no text was extracted, try pdfminer as backup
            if not text_content or len(''.join(text_content).strip()) < 50:
                pdfminer_text = pdfminer_extract(str(pdf_path))
                if pdfminer_text and len(pdfminer_text.strip()) > 50:
                    text_content = [pdfminer_text]
            
            return '\n\n'.join(text_content), ocr_used
            
        except Exception as e:
            logger.error(f"Error extracting from {pdf_path.name}: {str(e)}")
            return None, False
    
    def _ocr_page(self, page):
        """
        Perform OCR on a PDF page using Tesseract
        Supports English text only
        """
        try:
            # Render page as a high-resolution image
            pix = page.get_pixmap(matrix=fitz.Matrix(400/72, 400/72))  # 400 DPI for better quality
            img_data = pix.tobytes("png")
            image = Image.open(io.BytesIO(img_data))

            # Pre-process the image for better OCR results
            # Convert to grayscale
            image = image.convert('L')
            
            # Perform OCR with improved configuration
            # --psm 4: Assume a single column of text of variable sizes.
            # --psm 11: Sparse text. Find as much text as possible in no particular order.
            custom_config = r'--oem 3 --psm 4'
            text = pytesseract.image_to_string(
                image, 
                lang='eng',
                config=custom_config
            )
            
            return text
            
        except Exception as e:
            logger.error(f"OCR error: {str(e)}")
            return ""
    
    def process_file(self, file_path):
        """Process a single file and extract text"""
        try:
            file_ext = file_path.suffix.lower()
            
            if file_ext == '.pdf':
                text, ocr_used = self.extract_from_pdf(file_path)
                if ocr_used:
                    self.stats['ocr_used'] += 1
                return text, ocr_used
            else:
                logger.warning(f"Unsupported file type: {file_ext}")
                return None, False
                
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            return None, False
    
    def save_text(self, text, original_path, output_dir):
        """Save extracted text to text folder"""
        # Create output path maintaining directory structure
        relative_path = original_path.relative_to(RAW_DIR)
        output_path = output_dir / relative_path.with_suffix('.txt')
        
        # Create parent directories if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save text file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(text)
        
        return output_path
    
    def extract_all(self):
        """Extract text from files that have entries in metadata"""
        print("=" * 80)
        print("Starting text extraction process...")
        print("=" * 80)
        
        # Ensure text directory exists
        TEXT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load metadata to get list of files to process
        if not METADATA_FILE.exists():
            logger.error(f"❌ Metadata file not found: {METADATA_FILE}")
            logger.error("Please run the scrapers first to generate metadata.")
            return
        
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except Exception as e:
            logger.error(f"❌ Error loading metadata: {str(e)}")
            return
        
        # Get files from metadata that need text extraction
        files_to_process = []
        extraction_reasons = {
            'missing text_path': 0,
            'status not set to text_extracted': 0,
            'processing_status.text_extracted not set to true': 0,
            'text file missing on disk': 0,
            'already_complete': 0
        }
        
        for doc in metadata.get('documents', []):
            # Check if extraction fields are complete
            text_path = doc.get('text_path')
            status = doc.get('status')
            processing_status = doc.get('processing_status', {})
            text_extracted_flag = processing_status.get('text_extracted', False)
            
            # Determine if file needs (re)extraction
            needs_extraction = False
            reason = ""
            
            if not text_path or text_path == 'null':
                needs_extraction = True
                reason = "missing text_path"
            elif status != 'text_extracted':
                needs_extraction = True
                reason = "status not set to text_extracted"
            elif not text_extracted_flag:
                needs_extraction = True
                reason = "processing_status.text_extracted not set to true"
            elif text_path:
                # Check if text file actually exists
                text_file_path = BASE_DIR / text_path
                if not text_file_path.exists():
                    needs_extraction = True
                    reason = "text file missing on disk"
            
            if not needs_extraction:
                extraction_reasons['already_complete'] += 1
                continue
            
            # Track reason
            extraction_reasons[reason] += 1
            
            # Get raw file path
            raw_path = doc.get('raw_path')
            if not raw_path:
                logger.warning(f"⚠️  No raw_path found for: {doc.get('title', 'Unknown')}")
                continue
            
            # Convert to absolute path
            file_path = BASE_DIR / raw_path
            
            # Check if file exists
            if not file_path.exists():
                logger.warning(f"⚠️  File not found: {file_path}")
                continue
            
            # Check if file extension is supported
            if file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                files_to_process.append((file_path, doc))
            else:
                logger.warning(f"⚠️  Unsupported file type: {file_path.suffix} for {file_path.name}")
        
        self.stats['total_files'] = len(files_to_process)
        
        # Print analysis summary
        print(f"Total documents: {len(metadata.get('documents', []))}")
        print(f"Already complete: {extraction_reasons['already_complete']}")
        print(f"Need processing: {self.stats['total_files']}")
        print("=" * 80)
        
        if self.stats['total_files'] == 0:
            print("✅ All files already processed")
            return
        
        # Process each file
        extracted_documents = []
        
        for file_path, doc_metadata in files_to_process:
            print(f"Processing [{self.stats['successful'] + self.stats['failed'] + 1}/{self.stats['total_files']}]: {file_path.name}")
            
            # Extract text
            text, ocr_used = self.process_file(file_path)
            
            if text and text.strip():
                # Save text file
                output_path = self.save_text(text, file_path, TEXT_DIR)
                
                # Store extraction info to update metadata later
                extraction_info = {
                    'document_id': doc_metadata.get('id'),
                    'source_url': doc_metadata.get('source_url'),
                    'text_file': str(output_path.relative_to(BASE_DIR)),
                    'extracted_at': datetime.now().isoformat(),
                    'extraction_method': 'OCR + Text' if ocr_used else 'Text Only',
                    'text_length': len(text),
                    'word_count': len(text.split())
                }
                
                extracted_documents.append(extraction_info)
                self.stats['successful'] += 1
            else:
                logger.warning(f"❌ Failed to extract text from: {file_path.name}")
                self.stats['failed'] += 1
        
        # Update metadata file
        self._update_metadata(extracted_documents)
        
        # Print summary
        self._print_summary()
    
    def _update_metadata(self, extracted_documents):
        """Update the centralized metadata file - only updates existing fields"""
        if not extracted_documents:
            logger.info("No documents to update in metadata")
            return
            
        try:
            # Load existing metadata
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Update documents with text extraction info
            updated_count = 0
            for extraction_info in extracted_documents:
                # Find matching document by source_url (unique identifier)
                source_url = extraction_info.get('source_url')
                
                for doc in metadata['documents']:
                    if doc.get('source_url') == source_url:
                        # Update text_path (existing field)
                        doc['text_path'] = extraction_info['text_file']
                        
                        # Update extraction_method (new field)
                        doc['extraction_method'] = extraction_info['extraction_method']
                        
                        # Update status (existing field)
                        doc['status'] = 'text_extracted'
                        
                        # Update processing_status.text_extracted (existing field)
                        if 'processing_status' in doc:
                            doc['processing_status']['text_extracted'] = True
                        
                        updated_count += 1
                        break
            
            # Update metadata timestamp
            now = datetime.now()
            metadata['last_updated'] = now.strftime('%Y-%m-%d %I:%M:%S %p')
            
            # Recalculate extraction statistics for header
            ocr_count = 0
            normal_count = 0
            for doc in metadata['documents']:
                extraction_method = doc.get('extraction_method')
                if extraction_method:
                    if 'OCR' in extraction_method:
                        ocr_count += 1
                    elif 'Text' in extraction_method:
                        normal_count += 1
            
            # Update extraction_stats in header
            metadata['extraction_stats'] = {
                'ocr_used': ocr_count,
                'normal_extraction': normal_count
            }
            
            # Save updated metadata
            with open(METADATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ Updated {updated_count} document entries in metadata")
            print(f"📊 Extraction stats: OCR used: {ocr_count}, Normal extraction: {normal_count}")
            
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
    
    def _print_summary(self):
        """Print extraction summary"""
        print("\n" + "=" * 80)
        print("TEXT EXTRACTION SUMMARY")
        print("=" * 80)
        print(f"Total files processed: {self.stats['total_files']}")
        print(f"Successfully extracted: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"OCR used for: {self.stats['ocr_used']} files")
        print("=" * 80)


def main():
    """Main execution function"""
    try:
        extractor = TextExtractor()
        extractor.extract_all()
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
