"""
Ultra-Minimal Text Cleaning Script for Supreme Court Judgments
Preserve-first approach - only removes excessive whitespace
- Normalizes ONLY excessive blank lines (4+ newlines → 2)
- Preserves ALL legal content, case information, and judicial reasoning
- Most conservative cleaner - designed for already professional documents
- Updates metadata with cleaning information
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
TEXT_DIR = BASE_DIR / "data" / "text" / "supremecourt-judgments"
CLEANED_DIR = BASE_DIR / "data" / "cleaned" / "supremecourt-judgments"
METADATA_FILE = BASE_DIR / "data" / "metadata" / "documents_metadata.json"


class SupremeCourtCleaner:
    """Ultra-minimal cleaner for Supreme Court judgment documents"""
    
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
        Apply ultra-minimal cleaning while preserving ALL legal content
        Preserve-first approach for already excellent Supreme Court judgments
        """
        original_text = text
        
        # ONLY Step: Normalize excessive whitespace (4+ newlines → 2)
        text = self._normalize_whitespace(text)
        
        # Final cleanup (trailing spaces only)
        text = text.strip()
        
        # Validation: Ensure we didn't lose ANY content
        if not self._validate_cleaning(original_text, text, filename):
            logger.warning(f"⚠️  Validation failed for {filename}, using original text")
            return original_text
        
        return text
    
    def _normalize_whitespace(self, text):
        """Normalize excessive whitespace (ONLY 4+ blank lines)"""
        # Replace 4+ newlines with 2 newlines (preserve paragraph breaks)
        text = re.sub(r'\n{4,}', '\n\n', text)
        
        # Remove trailing spaces from lines
        text = re.sub(r' +\n', '\n', text)
        
        return text
    
    def _validate_cleaning(self, original, cleaned, filename):
        """Validate that no important content was lost"""
        
        # Check 1: Content reduction shouldn't exceed 5% (strictest)
        original_len = len(original)
        cleaned_len = len(cleaned)
        reduction = ((original_len - cleaned_len) / original_len * 100) if original_len > 0 else 0
        
        if reduction > 5:
            self.validation_errors.append({
                'file': filename,
                'reason': f'Excessive reduction: {reduction:.1f}%',
                'original_length': original_len,
                'cleaned_length': cleaned_len
            })
            return False
        
        # Check 2: Ensure case title preserved
        case_patterns = [
            r'IN THE SUPREME COURT OF PAKISTAN',
            r'SUPREME COURT OF PAKISTAN',
            r'Civil Petition',
            r'Criminal Appeal',
            r'Constitutional Petition'
        ]
        
        original_has_case = any(re.search(pattern, original, re.IGNORECASE) for pattern in case_patterns)
        cleaned_has_case = any(re.search(pattern, cleaned, re.IGNORECASE) for pattern in case_patterns)
        
        if original_has_case and not cleaned_has_case:
            self.validation_errors.append({
                'file': filename,
                'reason': 'Case title information lost'
            })
            return False
        
        # Check 3: Ensure judgment text preserved (look for "JUDGMENT" or "ORDER" heading)
        judgment_keywords = ['JUDGMENT', 'ORDER', 'OPINION']
        original_has_judgment = any(keyword in original.upper() for keyword in judgment_keywords)
        cleaned_has_judgment = any(keyword in cleaned.upper() for keyword in judgment_keywords)
        
        if original_has_judgment and not cleaned_has_judgment:
            self.validation_errors.append({
                'file': filename,
                'reason': 'Judgment/Order section lost'
            })
            return False
        
        return True
    
    def clean_file(self, input_path, output_path):
        """Clean a single file and save to cleaned folder"""
        try:
            # Read original file
            with open(input_path, 'r', encoding='utf-8') as f:
                original_text = f.read()
            
            original_size = len(original_text)
            
            # Clean the text
            cleaned_text = self.clean_text(original_text, input_path.name)
            cleaned_size = len(cleaned_text)
            
            # Calculate reduction
            reduction_percent = ((original_size - cleaned_size) / original_size * 100) if original_size > 0 else 0
            
            # Create output directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write cleaned text
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_text)
            
            self.stats['total_size_before'] += original_size
            self.stats['total_size_after'] += cleaned_size
            
            return {
                'success': True,
                'original_size': original_size,
                'cleaned_size': cleaned_size,
                'reduction_bytes': original_size - cleaned_size,
                'reduction_pct': round(reduction_percent, 2)
            }
            
        except Exception as e:
            logger.error(f"Error cleaning {input_path.name}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def process_all_files(self):
        """Process all Supreme Court judgment files that need cleaning"""
        print("=" * 80)
        print("SMART TEXT CLEANING - SUPREME COURT JUDGMENTS (ULTRA-MINIMAL)")
        print("=" * 80)
        
        # Create output directory
        CLEANED_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load metadata
        if not METADATA_FILE.exists():
            logger.error(f"❌ Metadata file not found: {METADATA_FILE}")
            return
        
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except Exception as e:
            logger.error(f"❌ Error loading metadata: {str(e)}")
            return
        
        # Get Supreme Court documents that need cleaning
        files_to_clean = []
        
        for doc in metadata.get('documents', []):
            if doc.get('source_website') != 'supremecourt':
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
        
        print(f"📊 Total Supreme Court documents: {sum(1 for d in metadata['documents'] if d.get('source_website') == 'supremecourt')}")
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
            
            # Show progress every 100 files or for first/last file
            show_progress = (idx % 100 == 0) or (idx == 1) or (idx == self.stats['total_files'])
            
            if show_progress:
                print(f"\n[{idx}/{self.stats['total_files']}] Cleaning: {text_file.name[:60]}...")
            
            # Determine output path
            relative_path = text_file.relative_to(TEXT_DIR)
            output_path = CLEANED_DIR / relative_path
            
            # Clean file
            result = self.clean_file(text_file, output_path)
            
            if result['success']:
                self.stats['successfully_cleaned'] += 1
                if show_progress:
                    print(f"   ✅ Success | Size: {result['original_size']:,} → {result['cleaned_size']:,} bytes | Reduction: {result['reduction_pct']}%")
                
                # Store info for metadata update
                cleaned_documents.append({
                    'source_url': doc_metadata.get('source_url'),
                    'cleaned_path': str(output_path.relative_to(BASE_DIR)),
                    'cleaned_at': datetime.now().isoformat(),
                    'original_size': result['original_size'],
                    'cleaned_size': result['cleaned_size'],
                    'reduction_percent': result['reduction_pct']
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
            for error in self.validation_errors[:10]:
                print(f"File: {error['file'][:60]}")
                print(f"Reason: {error['reason']}")
                print(f"Details: {error.get('details', error)}")
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
            total_reduction = (
                (self.stats['total_size_before'] - self.stats['total_size_after']) 
                / self.stats['total_size_before']
            ) * 100
            
            print(f"\n📦 Storage Impact:")
            print(f"   Before: {self.stats['total_size_before']:,} bytes ({self.stats['total_size_before']/1024/1024:.2f} MB)")
            print(f"   After:  {self.stats['total_size_after']:,} bytes ({self.stats['total_size_after']/1024/1024:.2f} MB)")
            print(f"   Reduction: {total_reduction:.1f}%")
        
        if self.validation_errors:
            print(f"\n⚠️  Validation warnings: {len(self.validation_errors)}")
        
        print("=" * 80)


def main():
    """Main execution function"""
    try:
        cleaner = SupremeCourtCleaner()
        cleaner.process_all_files()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Cleaning interrupted by user")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
