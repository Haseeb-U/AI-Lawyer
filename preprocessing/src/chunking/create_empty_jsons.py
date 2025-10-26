import os
import json
from pathlib import Path

def create_empty_jsons():
    cleaned_dir = Path('../data/cleaned')
    chunks_dir = Path('../data/chunks')

    # Ensure chunks directory exists
    chunks_dir.mkdir(parents=True, exist_ok=True)

    # Traverse all files in cleaned directory
    for root, dirs, files in os.walk(cleaned_dir):
        for file in files:
            # Get the relative path from cleaned_dir
            rel_path = Path(root).relative_to(cleaned_dir)
            # Create the corresponding path in chunks_dir
            chunk_subdir = chunks_dir / rel_path
            chunk_subdir.mkdir(parents=True, exist_ok=True)

            # Get the file name without extension
            file_stem = Path(file).stem
            # Create the json file path
            json_file = chunk_subdir / f"{file_stem}.json"

            if json_file.exists():
                print(f"Skipped existing JSON: {json_file}")
            else:
                # Create empty json file
                with open(json_file, 'w') as f:
                    json.dump({}, f)

                print(f"Created empty JSON: {json_file}")

if __name__ == "__main__":
    create_empty_jsons()
