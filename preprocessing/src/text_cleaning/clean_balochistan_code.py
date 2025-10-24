"""
Ultra-Conservative Text Cleaning Script for Balochistan Code
Minimal cleaning approach - preserves maximum legal information
- Removes only essential formatting noise (excessive whitespace)
- Preserves all section numbers, legal content, definitions, citations
- Updates metadata with cleaning information
- Most conservative cleaner of all provincial codes
"""

import os
import re
import json
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
TEXT_DIR = BASE_DIR / "data" / "text" / "balochistan-code"
CLEANED_DIR = BASE_DIR / "data" / "cleaned" / "balochistan-code"
METADATA_FILE = BASE_DIR / "data" / "metadata" / "documents_metadata.json"


class BalochistanCodeCleaner:
    """Ultra-conservative cleaner for Balochistan Code legal documents"""
    
    def __init__(self):
        self.stats = {
            'total_files': 0,
            'successfully_cleaned': 0,
            'failed': 0,
            'skipped_already_cleaned': 0,
            'total_size_before': 0,
            'total_size_after': 0
        }
        self.validation_errors = []
    
    def clean_text(self, text, filename):
        """
        Apply ultra-conservative cleaning while preserving legal content
        Minimal touch approach for Balochistan Code
        """
        original_text = text
        
        # Step 1: Normalize whitespace (ONLY 4+ newlines → 2)
        text = self._normalize_whitespace(text)
        
        # Step 2: Remove excessive separators (ONLY 10+ underscores)
        text = self._remove_separators(text)
        
        # Step 3: Format footnotes (preserve info, clean formatting)
        text = self._format_footnotes(text)
        
        # Step 4: Final cleanup
        text = text.strip()
        
        # Validation: Ensure we didn't lose important content
        if not self._validate_cleaning(original_text, text, filename):
            logger.warning(f"⚠️  Validation failed for {filename}, using original text")
            return original_text
        
        return text
    
    def _normalize_whitespace(self, text):
        """
        Normalize excessive whitespace (ONLY 4+ blank lines)
        Most conservative approach
        """
        # Replace 4+ newlines with 2 newlines
        text = re.sub(r'\n{4,}', '\n\n', text)
        
        # Remove trailing spaces from lines
        text = re.sub(r' +\n', '\n', text)
        
        # That's it! No other whitespace changes
        return text
    
    def _remove_separators(self, text):
        """Remove excessive separator lines (ONLY 10+ chars)"""
        # Remove lines with 10+ underscores or dashes
        text = re.sub(r'\n\s*[_\-]{10,}\s*\n', '\n\n', text)
        
        return text
    
    def _format_footnotes(self, text):
        """
        Format footnotes while preserving all information
        Balochistan Code footnotes contain valuable legislative history
        """
        # Pattern for legislative notes at the end of documents
        # Example: "1 This Act was passed by the Balochistan Assembly..."
        
        # Add clear marker for legislative notes while preserving content
        footnote_pattern = r'\n(\d+)\s+(This (?:Act|Ordinance) was (?:passed|enacted|promulgated)[^\n]{50,})\n'
        
        def format_legislative_note(match):
            note_num = match.group(1)
            note_text = match.group(2).strip()
            return f'\n\n[Legislative Note {note_num}: {note_text}]\n'
        
        text = re.sub(footnote_pattern, format_legislative_note, text)
        
        return text
    
    def _validate_cleaning(self, original, cleaned, filename):
        """
        Validate that cleaning didn't remove important content
        Strictest validation thresholds (98% section match, 80% content retention)
        """
        # Check 1: Section count should match (±2%)
        original_sections = len(re.findall(r'\n\s*\d+\.\s+[A-Z]', original))
        cleaned_sections = len(re.findall(r'\n\s*\d+\.\s+[A-Z]', cleaned))
        
        if original_sections > 0:
            section_diff = abs(original_sections - cleaned_sections) / original_sections
            if section_diff > 0.02:  # Allow only 2% variance
                self.validation_errors.append({
                    'file': filename,
                    'reason': 'Section count mismatch',
                    'original': original_sections,
                    'cleaned': cleaned_sections,
                    'diff_pct': f"{section_diff*100:.1f}%"
                })
                return False
        
        # Check 2: Content reduction shouldn't exceed 20%
        original_length = len(original)
        cleaned_length = len(cleaned)
        
        if cleaned_length < original_length * 0.80:  # Strictest: 80% threshold
            self.validation_errors.append({
                'file': filename,
                'reason': 'Excessive content reduction',
                'original_length': original_length,
                'cleaned_length': cleaned_length,
                'reduction': f"{((original_length - cleaned_length) / original_length * 100):.1f}%"
            })
            return False
        
        # Check 3: Minimum content length
        if cleaned_length < 100:
            self.validation_errors.append({
                'file': filename,
                'reason': 'Cleaned text too short',
                'cleaned_length': cleaned_length
            })
            return False
        
        return True
    
    def clean_file(self, input_path, output_path):
        """Clean a single file and save to output path"""
        try:
            # Read original file
            with open(input_path, 'r', encoding='utf-8') as f:
                original_text = f.read()
            
            original_size = len(original_text)
            
            # Clean the text
            cleaned_text = self.clean_text(original_text, input_path.name)
            
            cleaned_size = len(cleaned_text)
            
            # Create output directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save cleaned file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_text)
            
            # Calculate reduction
            reduction_percent = ((original_size - cleaned_size) / original_size * 100) if original_size > 0 else 0
            
            return {
                'success': True,
                'original_size': original_size,
                'cleaned_size': cleaned_size,
                'reduction_percent': reduction_percent
            }
            
        except Exception as e:
            logger.error(f"Error cleaning {input_path.name}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def process_all_files(self):
        """Process all Balochistan Code files that haven't been cleaned yet"""
        print("=" * 80)
        print("SMART TEXT CLEANING - BALOCHISTAN CODE (ULTRA-CONSERVATIVE)")
        print("=" * 80)
        
        # Ensure output directory exists
        CLEANED_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load metadata to determine which files need cleaning
        if not METADATA_FILE.exists():
            logger.error(f"❌ Metadata file not found: {METADATA_FILE}")
            return
        
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except Exception as e:
            logger.error(f"❌ Error loading metadata: {str(e)}")
            return
        
        # Find Balochistan Code documents that need cleaning
        files_to_clean = []
        
        for doc in metadata.get('documents', []):
            # Only process balochistan-code documents
            if doc.get('source_website') != 'balochistan-code':
                continue
            
            # Check if already cleaned
            cleaned_path = doc.get('cleaned_path')
            processing_status = doc.get('processing_status', {})
            text_cleaned = processing_status.get('text_cleaned', False)
            
            # Check if text file exists
            text_path = doc.get('text_path')
            if not text_path:
                continue
            
            text_file = BASE_DIR / text_path
            if not text_file.exists():
                logger.warning(f"⚠️  Text file not found: {text_file}")
                continue
            
            # Determine if cleaning is needed
            needs_cleaning = False
            
            if not cleaned_path or cleaned_path == 'null':
                needs_cleaning = True
            elif not text_cleaned:
                needs_cleaning = True
            elif cleaned_path:
                # Check if cleaned file actually exists
                cleaned_file = BASE_DIR / cleaned_path
                if not cleaned_file.exists():
                    needs_cleaning = True
            
            if needs_cleaning:
                files_to_clean.append({
                    'text_file': text_file,
                    'doc_metadata': doc
                })
            else:
                self.stats['skipped_already_cleaned'] += 1
        
        self.stats['total_files'] = len(files_to_clean)
        
        print(f"📊 Total Balochistan Code documents: {sum(1 for d in metadata['documents'] if d.get('source_website') == 'balochistan-code')}")
        print(f"✅ Already cleaned: {self.stats['skipped_already_cleaned']}")
        print(f"🔄 Need cleaning: {self.stats['total_files']}")
        print("=" * 80)
        
        if self.stats['total_files'] == 0:
            print("✅ All files already cleaned!")
            return
        
        # Process each file
        cleaned_documents = []
        
        for idx, file_info in enumerate(files_to_clean, 1):
            text_file = file_info['text_file']
            doc_metadata = file_info['doc_metadata']
            
            print(f"\n[{idx}/{self.stats['total_files']}] Cleaning: {text_file.name}")
            
            # Determine output path
            relative_path = text_file.relative_to(TEXT_DIR)
            output_path = CLEANED_DIR / relative_path
            
            # Clean the file
            result = self.clean_file(text_file, output_path)
            
            if result['success']:
                self.stats['successfully_cleaned'] += 1
                self.stats['total_size_before'] += result['original_size']
                self.stats['total_size_after'] += result['cleaned_size']
                
                print(f"   ✅ Success | Size: {result['original_size']:,} → {result['cleaned_size']:,} bytes | Reduction: {result['reduction_percent']:.1f}%")
                
                # Store info for metadata update
                cleaned_documents.append({
                    'source_url': doc_metadata.get('source_url'),
                    'cleaned_path': str(output_path.relative_to(BASE_DIR)),
                    'cleaned_at': datetime.now().isoformat(),
                    'original_size': result['original_size'],
                    'cleaned_size': result['cleaned_size'],
                    'reduction_percent': result['reduction_percent']
                })
            else:
                self.stats['failed'] += 1
                print(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
        
        # Update metadata
        self._update_metadata(cleaned_documents)
        
        # Print validation errors if any
        if self.validation_errors:
            print("\n" + "=" * 80)
            print("⚠️  VALIDATION WARNINGS")
            print("=" * 80)
            for error in self.validation_errors[:10]:  # Show first 10
                print(f"File: {error['file']}")
                print(f"Reason: {error['reason']}")
                print(f"Details: {error}")
                print("-" * 80)
        
        # Print summary
        self._print_summary()
    
    def _update_metadata(self, cleaned_documents):
        """Update metadata with cleaning information"""
        if not cleaned_documents:
            logger.info("No documents to update in metadata")
            return
        
        try:
            # Load existing metadata
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Update documents
            updated_count = 0
            for cleaning_info in cleaned_documents:
                source_url = cleaning_info.get('source_url')
                
                for doc in metadata['documents']:
                    if doc.get('source_url') == source_url:
                        # Update cleaned_path
                        doc['cleaned_path'] = cleaning_info['cleaned_path']
                        
                        # Update status
                        doc['status'] = 'text_cleaned'
                        
                        # Update processing_status
                        if 'processing_status' not in doc:
                            doc['processing_status'] = {}
                        doc['processing_status']['text_cleaned'] = True
                        
                        # Add cleaning metadata
                        doc['cleaning_info'] = {
                            'cleaned_at': cleaning_info['cleaned_at'],
                            'original_size': cleaning_info['original_size'],
                            'cleaned_size': cleaning_info['cleaned_size'],
                            'reduction_percent': round(cleaning_info['reduction_percent'], 2)
                        }
                        
                        updated_count += 1
                        break
            
            # Update metadata timestamp and total_cleaned count
            metadata['last_updated'] = datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')
            
            # Count total cleaned documents (all sources)
            total_cleaned = sum(1 for doc in metadata['documents'] 
                              if doc.get('processing_status', {}).get('text_cleaned', False))
            metadata['total_cleaned'] = total_cleaned
            
            # Save updated metadata
            with open(METADATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ Updated {updated_count} document entries in metadata")
            print(f"📊 Total cleaned documents (all sources): {total_cleaned}")
            
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
    
    def _print_summary(self):
        """Print cleaning summary"""
        print("\n" + "=" * 80)
        print("CLEANING SUMMARY")
        print("=" * 80)
        print(f"Total files to clean: {self.stats['total_files']}")
        print(f"Successfully cleaned: {self.stats['successfully_cleaned']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Already cleaned (skipped): {self.stats['skipped_already_cleaned']}")
        
        if self.stats['total_size_before'] > 0:
            total_reduction = ((self.stats['total_size_before'] - self.stats['total_size_after']) 
                             / self.stats['total_size_before'] * 100)
            print(f"\n📦 Storage Impact:")
            print(f"   Before: {self.stats['total_size_before']:,} bytes ({self.stats['total_size_before'] / 1024 / 1024:.2f} MB)")
            print(f"   After:  {self.stats['total_size_after']:,} bytes ({self.stats['total_size_after'] / 1024 / 1024:.2f} MB)")
            print(f"   Reduction: {total_reduction:.1f}%")
        
        if self.validation_errors:
            print(f"\n⚠️  Validation warnings: {len(self.validation_errors)}")
        
        print("=" * 80)


def main():
    """Main execution function"""
    try:
        cleaner = BalochistanCodeCleaner()
        cleaner.process_all_files()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
