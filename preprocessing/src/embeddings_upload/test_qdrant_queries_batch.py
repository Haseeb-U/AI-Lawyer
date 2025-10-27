"""
test_qdrant_queries_batch.py
---------------------------------------
Runs multiple semantic search queries against Qdrant to test
retrieval of legal text embeddings.
Each query is embedded using the same model and compared to vectors
stored in Qdrant. Prints top 3 most similar results for each.
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

MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
# Use multilingual model if you have Urdu or mixed-language data:
# MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# ==========================
# TEST QUERIES
# ==========================
queries = [
    "Which ordinance amended the Excise Regulation, 1915, in Balochistan?",
    "Give details of the 1984 Balochistan Excise Ordinance and its legal basis.",
    "Under what powers did the Governor of Balochistan issue the 1984 excise amendment?",
    "Explain the purpose and context of Baln Ordinance X of 1984.",
    "Show laws where immediate action was taken to amend the Excise Regulation in Balochistan."
]

# ==========================
# INITIALIZE CLIENTS
# ==========================
print("🔗 Connecting to Qdrant...")
qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
model = SentenceTransformer(MODEL_NAME)

# ==========================
# SEARCH FUNCTION
# ==========================
def search_query(query_text, limit=3):
    print(f"\n🧠 Generating embedding for query: {query_text}")
    query_vec = model.encode(query_text, convert_to_numpy=True).tolist()

    results = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=query_vec,
        limit=limit
    )

    print(f"\n🎯 Top {limit} results for: '{query_text}'\n")
    for i, res in enumerate(results, 1):
        payload = res.payload
        print(f"Result {i}:")
        print(f"  📄 Title: {payload.get('title')}")
        print(f"  📅 Year: {payload.get('year')}")
        print(f"  ⚖️ Court: {payload.get('court')}")
        print(f"  🗂️ Type: {payload.get('document_type')}")
        print(f"  💬 Chunk: {payload.get('chunk', '')[:300]}...")
        print(f"  🔢 Score: {res.score:.4f}\n")
    print("-" * 80)

# ==========================
# RUN ALL QUERIES
# ==========================
if __name__ == "__main__":
    print(f"🚀 Running {len(queries)} test queries...\n")
    for q in queries:
        search_query(q)
    print("\n✅ All test queries completed successfully!")
