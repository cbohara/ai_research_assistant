import os
import qdrant_client
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer
import pyiceberg
from pyiceberg.table import Table
from pyiceberg.catalog import load_catalog

# Config
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "papers")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "spark_catalog")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "default.papers_refined")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 128))

def get_iceberg_table():
    # This assumes you have a working Iceberg catalog config (e.g., via environment or ~/.iceberg)
    catalog = load_catalog(ICEBERG_CATALOG)
    return catalog.load_table(ICEBERG_TABLE)

def get_records(table):
    # This is a simple scan; you may want to filter or batch for large tables
    for row in table.scan().to_arrow().to_pylist():
        yield row

def main():
    # Load model and Qdrant client
    model = SentenceTransformer(EMBEDDING_MODEL)
    client = qdrant_client.QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    # Create collection if not exists
    client.recreate_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=qmodels.VectorParams(size=model.get_sentence_embedding_dimension(), distance="Cosine"),
    )

    table = get_iceberg_table()
    batch = []
    for i, record in enumerate(get_records(table)):
        text = " ".join([record.get("title", ""), record.get("abstract", ""), record.get("full_text", "")])
        vector = model.encode(text)
        payload = {
            "id": record.get("id"),
            "title": record.get("title"),
            "authors": record.get("authors"),
            "publication_date": record.get("publication_date"),
            "doi": record.get("doi"),
            "source": record.get("source"),
            # Add more fields as needed
        }
        batch.append(qmodels.PointStruct(id=record.get("id"), vector=vector.tolist(), payload=payload))
        if len(batch) >= BATCH_SIZE:
            client.upsert(collection_name=QDRANT_COLLECTION, points=batch)
            batch = []
    if batch:
        client.upsert(collection_name=QDRANT_COLLECTION, points=batch)
    print("Finished loading vectors into Qdrant.")

if __name__ == "__main__":
    main()