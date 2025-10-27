"""
test_qdrant_query.py
---------------------------------------
Test semantic search on Qdrant using a sample query string.
Generates query embedding, searches top results, and prints metadata.
"""

import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# ==========================
# CONFIGURATION
# ==========================
load_dotenv()

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "smartlawyer_embeddings")

# The same embedding model you used for document embeddings
MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"  
# or use "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2" for Urdu + English

# ==========================
# TEST QUERY
# ==========================
query = "Which ordinance amended the Excise Regulation, 1915, in Balochistan?"  # 👈 change this to any query

# ==========================
# INITIALIZE CLIENTS
# ==========================
print("🔗 Connecting to Qdrant...")
qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
model = SentenceTransformer(MODEL_NAME)

# ==========================
# CREATE QUERY EMBEDDING
# ==========================
print(f"🧠 Generating embedding for query: {query}")
query_embedding = model.encode(query, convert_to_numpy=True).tolist()

# ==========================
# SEARCH QDRANT
# ==========================
print("🔍 Searching Qdrant for similar chunks...")
results = qdrant.search(
    collection_name=QDRANT_COLLECTION,
    query_vector=query_embedding,
    limit=5  # top 5 most similar chunks
)

# ==========================
# DISPLAY RESULTS
# ==========================
print("\n🎯 Top Matching Results:\n")
for i, res in enumerate(results, 1):
    payload = res.payload
    print(f"Result {i}:")
    print(f"  📄 Title: {payload.get('title')}")
    print(f"  ⚖️  Court: {payload.get('court')}")
    print(f"  📅 Year: {payload.get('year')}")
    print(f"  🗂️  Document Type: {payload.get('document_type')}")
    print(f"  💬 Chunk: {payload.get('chunk')[:300]}...")
    print(f"  🔢 Score: {res.score:.4f}\n")

print("✅ Query test completed.")
