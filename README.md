# Pipeline Overview

This data pipeline is orchestrated via **Flyte** and consists of three main stages:

## 1. Ingestion & Storage (PySpark)
- Raw data is ingested from multiple academic paper sources  
- Each source writes to its own **partitioned Iceberg raw table**  
- Data is normalized, deduplicated, and merged into a **master Iceberg table** partitioned by publication date

## 2. Embedding & Vector Search Indexing (Python)
- The master Iceberg table latest updates are read by a Python job 
- Text fields are embedded using a sentence transformer model  
- Embedded vectors are uploaded to a **Qdrant vector search engine**

## 3. Real-Time Query Service
- A real-time application queries Qdrant to provide fast, relevant results to users via an AI assistant interface