import os
import json
from pathlib import Path
from datetime import datetime
from sentence_transformers import SentenceTransformer
import numpy as np

# ==========================
# CONFIG PATHS (robust to CWD)
# ==========================
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parents[2]  # points to project root (AI-Lawyer)
DATA_DIR = BASE_DIR / "data"
CHUNKS_DIR = DATA_DIR / "chunks"
EMBEDDINGS_DIR = DATA_DIR / "embeddings"
METADATA_FILE = DATA_DIR / "metadata" / "documents_metadata.json"
EMBEDDINGS_FILE = EMBEDDINGS_DIR / "embeddings.jsonl"

# ==========================
# HELPER FUNCTIONS
# ==========================
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def append_jsonl(path, data):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

def get_last_id(path):
    """Read last ID from jsonl so numbering continues across runs"""
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if not lines:
            return 0
        try:
            last_record = json.loads(lines[-1])
            return last_record["metadata"]["id"]
        except Exception:
            return 0

def embeddings_exist_for(doc_title, embeddings_file):
    """Check if any embedding record exists for a given document title."""
    if not embeddings_file.exists():
        return False
    with open(embeddings_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                if rec["metadata"].get("title") == doc_title:
                    return True
            except Exception:
                continue
    return False

# ==========================
# MAIN LOGIC
# ==========================
def main():
    print("🔍 Loading metadata...")
    metadata = load_json(METADATA_FILE)
    model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")

    EMBEDDINGS_DIR.mkdir(parents=True, exist_ok=True)
    id_counter = get_last_id(EMBEDDINGS_FILE) + 1
    total_added = 0

    for doc in metadata["documents"]:
        proc_status = doc.get("processing_status", {})
        already_embedded = proc_status.get("embedded", False)
        has_embeddings = embeddings_exist_for(doc.get("title"), EMBEDDINGS_FILE)

        # Skip logic: only skip if already embedded AND embeddings actually exist
        if not proc_status.get("chunked") or (already_embedded and has_embeddings):
            continue

        # Resolve chunk path
        chunked_path_str = str(doc["chunked_path"])
        while chunked_path_str.startswith("../") or chunked_path_str.startswith("./"):
            chunked_path_str = chunked_path_str.partition("/")[2]
        chunk_path = (BASE_DIR / chunked_path_str).resolve()
        if not chunk_path.exists():
            print(f"⚠️ Skipping missing chunk file: {chunk_path}")
            continue

        try:
            chunk_data = load_json(chunk_path)
        except Exception as e:
            print(f"❌ Error reading {chunk_path}: {e}")
            continue

        document_type = chunk_data.get("document_type", "unknown")
        chunks = chunk_data.get("chunks", [])

        print(f"🧠 Creating embeddings for: {chunk_path.name} ({len(chunks)} chunks)")

        for chunk in chunks:
            # === Construct semantically rich embedding text ===
            title = doc.get("title", "")
            chunk_title = chunk.get("chunk_title", "")
            chunk_type = chunk.get("chunk_type", "")
            year = str(doc.get("year", "")) if doc.get("year") else ""
            document_type = doc.get("document_type", document_type)

            embedding_text_parts = [
                f"{document_type}" if document_type else "",
                f"{title}" if title else "",
                f"{chunk_type}" if chunk_type else "",
                f"{chunk_title}" if chunk_title else "",
                f"Year {year}" if year else "",
                chunk["text"],
            ]
            embedding_text = " | ".join(filter(None, embedding_text_parts))

            # === Create embedding ===
            embedding = model.encode(embedding_text, convert_to_numpy=True).tolist()

            # === Record to save ===
            record = {
                "chunk": chunk["text"],
                "embedding": embedding,
                "upload": False,
                "metadata": {
                    "id": id_counter,
                    "title": title,
                    "source_page": doc.get("source_page"),
                    "source_url": doc.get("source_url"),
                    "source_website": doc.get("source_website"),
                    "cleaned_path": doc.get("cleaned_path"),
                    "download_date": doc.get("download_date"),
                    "document_type": document_type,
                    "year": doc.get("year"),
                    "court": doc.get("court"),
                    "chunk_index": chunk.get("chunk_index"),
                    "chunk_type": chunk_type,
                    "chunk_title": chunk_title,
                },
            }

            append_jsonl(EMBEDDINGS_FILE, record)
            id_counter += 1
            total_added += 1

        # ✅ Mark as embedded and save metadata immediately
        doc["processing_status"]["embedded"] = True
        doc["status"] = "embedded"
        save_json(METADATA_FILE, metadata)
        print(f"✅ Updated metadata after {chunk_path.name}")

    print(f"\n🎯 All done! Added {total_added} embeddings.")
    print(f"📁 Saved to: {EMBEDDINGS_FILE}")

# ==========================
# ENTRY POINT
# ==========================
if __name__ == "__main__":
    main()
