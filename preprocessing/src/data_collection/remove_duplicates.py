import json
import os

def remove_duplicates_by_raw_path(metadata_path):
    """
    Removes duplicate entries from documents_metadata.json based on raw_path.
    Keeps only the first occurrence of each raw_path.
    Updates the total_documents count.
    """
    # Load the metadata
    with open(metadata_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    documents = data.get('documents', [])
    seen_raw_paths = set()
    unique_documents = []

    for doc in documents:
        raw_path = doc.get('raw_path')
        if raw_path not in seen_raw_paths:
            seen_raw_paths.add(raw_path)
            unique_documents.append(doc)

    # Update the documents list and total count
    data['documents'] = unique_documents
    data['total_documents'] = len(unique_documents)

    # Write back to file
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Removed {len(documents) - len(unique_documents)} duplicate entries.")
    print(f"Total documents now: {len(unique_documents)}")

if __name__ == "__main__":
    # Path to the metadata file
    metadata_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'metadata', 'documents_metadata.json')
    remove_duplicates_by_raw_path(metadata_path)