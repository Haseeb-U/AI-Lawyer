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
    level=logging.INFO,
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
            logger.info(f"Attempting PyMuPDF extraction for: {pdf_path.name}")
            doc = fitz.open(pdf_path)
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                page_text = page.get_text()
                
                # Check if page has extractable text
                if page_text.strip():
                    text_content.append(page_text)
                else:
                    # Page appears to be scanned - use OCR
                    logger.info(f"Page {page_num + 1} appears scanned, using OCR...")
                    ocr_text = self._ocr_page(page)
                    if ocr_text:
                        text_content.append(ocr_text)
                        ocr_used = True
            
            doc.close()
            
            # If no text was extracted, try pdfminer as backup
            if not text_content or len(''.join(text_content).strip()) < 50:
                logger.info(f"Trying pdfminer as backup for: {pdf_path.name}")
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
        Supports both English and Urdu text
        """
        try:
            # Render page as image
            pix = page.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))  # 300 DPI
            img_data = pix.tobytes("png")
            image = Image.open(io.BytesIO(img_data))
            
            # Perform OCR with Urdu and English support
            # Tesseract languages: eng (English) + urd (Urdu)
            custom_config = r'--oem 3 --psm 6'
            text = pytesseract.image_to_string(
                image, 
                lang='eng+urd',
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
        
        logger.info(f"✅ Saved text to: {output_path}")
        return output_path
    
    def extract_all(self):
        """Extract text from all files in raw folder"""
        logger.info("=" * 80)
        logger.info("Starting text extraction process...")
        logger.info(f"Raw directory: {RAW_DIR}")
        logger.info(f"Text directory: {TEXT_DIR}")
        logger.info("=" * 80)
        
        # Ensure text directory exists
        TEXT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Find all supported files
        files_to_process = []
        for ext in SUPPORTED_EXTENSIONS:
            files_to_process.extend(RAW_DIR.rglob(f'*{ext}'))
        
        self.stats['total_files'] = len(files_to_process)
        logger.info(f"Found {self.stats['total_files']} files to process")
        
        # Process each file
        extracted_documents = []
        
        for file_path in files_to_process:
            logger.info(f"\nProcessing: {file_path.name}")
            
            # Extract text
            text, ocr_used = self.process_file(file_path)
            
            if text and text.strip():
                # Save text file
                output_path = self.save_text(text, file_path, TEXT_DIR)
                
                # Create metadata entry
                doc_metadata = {
                    'original_file': str(file_path.relative_to(BASE_DIR)),
                    'text_file': str(output_path.relative_to(BASE_DIR)),
                    'file_name': file_path.name,
                    'file_size': file_path.stat().st_size,
                    'extracted_at': datetime.now().isoformat(),
                    'extraction_method': 'OCR + Text' if ocr_used else 'Text Only',
                    'text_length': len(text),
                    'word_count': len(text.split()),
                    'status': 'extracted'
                }
                
                extracted_documents.append(doc_metadata)
                self.stats['successful'] += 1
            else:
                logger.warning(f"❌ Failed to extract text from: {file_path.name}")
                self.stats['failed'] += 1
        
        # Update metadata file
        self._update_metadata(extracted_documents)
        
        # Print summary
        self._print_summary()
    
    def _update_metadata(self, extracted_documents):
        """Update the centralized metadata file"""
        try:
            # Load existing metadata
            if METADATA_FILE.exists():
                with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            else:
                logger.error("Metadata file not found!")
                return
            
            # Update existing documents with text_path and processing status
            for extracted_doc in extracted_documents:
                # Find matching document by raw_path
                for doc in metadata['documents']:
                    raw_path = doc.get('raw_path', '').replace('\\', '/')
                    original_file = extracted_doc['original_file'].replace('\\', '/')
                    
                    if raw_path in original_file or original_file.endswith(Path(raw_path).name):
                        # Update text_path
                        doc['text_path'] = extracted_doc['text_file']
                        
                        # Update processing status
                        if 'processing_status' not in doc:
                            doc['processing_status'] = {}
                        doc['processing_status']['text_extracted'] = True
                        
                        logger.info(f"✅ Updated metadata for: {doc['title']}")
                        break
            
            # Update metadata timestamp with better format
            now = datetime.now()
            metadata['last_updated'] = now.strftime('%Y-%m-%d %I:%M:%S %p')
            
            # Save updated metadata
            with open(METADATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"\n✅ Metadata updated: {METADATA_FILE}")
            
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
    
    def _print_summary(self):
        """Print extraction summary"""
        logger.info("\n" + "=" * 80)
        logger.info("TEXT EXTRACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total files processed: {self.stats['total_files']}")
        logger.info(f"Successfully extracted: {self.stats['successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"OCR used for: {self.stats['ocr_used']} files")
        logger.info("=" * 80)


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
