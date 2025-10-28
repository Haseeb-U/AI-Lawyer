"""
upload_embeddings_to_qdrant.py
Uploads embeddings from data/embeddings/embeddings.jsonl
to Qdrant (vector DB) and MongoDB (metadata store).

Configured to:
 - Send only {id, chunk, vector, title, year, court, document_type, download_date} to Qdrant.
 - Send full metadata (except embeddings) to MongoDB.
 - Automatically create useful indexes in MongoDB.
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from pymongo import MongoClient, ASCENDING

# ==========================
# CONFIGURATION
# ==========================
load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parents[2]
DATA_DIR = BASE_DIR / "data"
EMBEDDINGS_FILE = DATA_DIR / "embeddings" / "embeddings.jsonl"

# Environment variables (.env)
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", None)
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "Embeddings")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "smartlawyer")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "Embeddings")

VECTOR_SIZE = 768  # For sentence-transformers MiniLM/MPNet

# ==========================
# INIT CLIENTS
# ==========================
print("🔗 Connecting to Qdrant and MongoDB...")

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
mongo = MongoClient(MONGO_URI)
mdb = mongo[MONGO_DB]
metadata_col = mdb[MONGO_COLLECTION]

# ==========================
# ENSURE QDRANT COLLECTION
# ==========================
try:
    qdrant.get_collection(QDRANT_COLLECTION)
    print(f"✅ Qdrant collection '{QDRANT_COLLECTION}' already exists.")
except Exception:
    print(f"⚙️ Creating new Qdrant collection: {QDRANT_COLLECTION}")
    qdrant.recreate_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=qmodels.VectorParams(size=VECTOR_SIZE, distance=qmodels.Distance.COSINE),
    )

# ==========================
# HELPER FUNCTIONS
# ==========================
def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                continue

def rewrite_jsonl_with_flag(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ==========================
# MAIN UPLOAD FUNCTION
# ==========================
def upload_embeddings():
    print("📦 Starting embedding upload process...")
    records = list(read_jsonl(EMBEDDINGS_FILE))
    total = len(records)
    uploaded = 0

    for rec in records:
        if rec.get("upload", False):
            continue

        vec = rec.get("embedding")
        meta = rec.get("metadata", {})
        chunk = rec.get("chunk", "")

        if not vec or not meta:
            continue

        # ---- 1️⃣ Upload to Qdrant ----
        try:
            payload = {
                "id": meta.get("id"),
                "chunk": chunk,
                "title": meta.get("title"),
                "year": meta.get("year"),
                "court": meta.get("court"),
                "document_type": meta.get("document_type"),
                "download_date": meta.get("download_date"),
            }

            qdrant.upsert(
                collection_name=QDRANT_COLLECTION,
                points=[
                    qmodels.PointStruct(
                        id=meta.get("id"),
                        vector=vec,
                        payload=payload,
                    )
                ],
            )
        except Exception as e:
            print(f"❌ Qdrant upload failed for ID {meta.get('id')}: {e}")
            continue

        # ---- 2️⃣ Upload metadata to MongoDB ----
        try:
            clean_meta = {k: v for k, v in meta.items() if k != "embedding"}
            metadata_col.update_one({"id": meta.get("id")}, {"$set": clean_meta}, upsert=True)
        except Exception as e:
            print(f"⚠️ MongoDB upload failed for ID {meta.get('id')}: {e}")

        # ---- 3️⃣ Update local flag ----
        rec["upload"] = True
        uploaded += 1

        if uploaded % 10 == 0:
            print(f"🧩 Uploaded {uploaded}/{total} embeddings...")

    # ---- Rewrite file with updated flags ----
    rewrite_jsonl_with_flag(EMBEDDINGS_FILE, records)
    print(f"\n🎯 Upload completed! Total uploaded: {uploaded}/{total}")
    print(f"📁 Updated local file: {EMBEDDINGS_FILE}")

# ==========================
# CREATE INDEXES IN MONGODB
# ==========================
def ensure_indexes():
    print("⚙️ Ensuring MongoDB indexes...")
    try:
        metadata_col.create_index([("court", ASCENDING)])
        metadata_col.create_index([("year", ASCENDING)])
        metadata_col.create_index([("document_type", ASCENDING)])
        metadata_col.create_index([("title", ASCENDING)])
        print("✅ MongoDB indexes created or verified.")
    except Exception as e:
        print(f"⚠️ Index creation failed: {e}")

# ==========================
# ENTRY POINT
# ==========================
if __name__ == "__main__":
    ensure_indexes()
    upload_embeddings()
