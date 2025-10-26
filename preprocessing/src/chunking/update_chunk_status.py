"""
Script Name: update_chunk_status.py
Purpose: Update documents_metadata.json based on valid chunk files in data/chunks.
Author: AI-Based Smart Lawyer (Phase 3 → Phase 4 transition)
"""

import os
import json
from datetime import datetime

# === CONFIG ===
METADATA_PATH = os.path.join("..", "data", "metadata", "documents_metadata.json")
CHUNKS_DIR = os.path.join("..", "data", "chunks")

# === FUNCTIONS ===

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_valid_chunk_file(path):
    """Check if chunk file exists and has non-empty 'chunks' list."""
    if not os.path.exists(path) or os.path.getsize(path) < 5:
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return bool(data and "chunks" in data and len(data["chunks"]) > 0)
    except Exception:
        return False

def find_chunk_file(cleaned_path):
    """Guess chunk file path based on cleaned file name."""
    # Extract source from cleaned_path, e.g., "data\cleaned\pakistan-code\file.txt" -> "pakistan-code"
    parts = cleaned_path.replace("\\", "/").split("/")
    if len(parts) >= 3 and parts[0] == "data" and parts[1] == "cleaned":
        source = parts[2]
        base_name = os.path.splitext(os.path.basename(cleaned_path))[0]
        possible_path = os.path.join(CHUNKS_DIR, source, f"{base_name}.json")
        return possible_path if os.path.exists(possible_path) else None
    return None


def main():
    print("🔍 Checking chunk files and updating metadata...")

    # Load metadata
    metadata = load_json(METADATA_PATH)
    updated_count = 0
    total_valid = 0

    for doc in metadata.get("documents", []):
        cleaned_path = doc.get("cleaned_path")
        if not cleaned_path:
            continue

        chunk_file = find_chunk_file(cleaned_path)
        if chunk_file and is_valid_chunk_file(chunk_file):
            # Update document metadata
            doc["chunked_path"] = chunk_file.replace("\\", "/")
            doc["status"] = "chunked"
            doc["processing_status"]["chunked"] = True
            updated_count += 1
            total_valid += 1
        else:
            # Ensure it's marked unchunked
            doc["processing_status"]["chunked"] = False

    # Update summary fields
    # Insert total_chunked after total_cleaned
    new_metadata = {}
    for key in metadata:
        new_metadata[key] = metadata[key]
        if key == "total_cleaned":
            new_metadata["total_chunked"] = total_valid
    metadata = new_metadata
    metadata["last_updated"] = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")

    # Save updated metadata
    save_json(METADATA_PATH, metadata)

    print(f"✅ Updated {updated_count} documents.")
    print(f"📊 Total valid chunked files: {total_valid}")
    print(f"🕒 Metadata last updated at: {metadata['last_updated']}")


if __name__ == "__main__":
    main()
