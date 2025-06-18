#!/usr/bin/env python3
"""
Sample data generator for testing the academic papers pipeline.
This script creates sample CSV files that match the expected schema.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import uuid
from config import S3_BUCKET

def generate_sample_papers(num_papers=100, source_name="arxiv"):
    """Generate sample academic paper data"""
    
    # Sample data for realistic academic papers
    journals = [
        "Nature", "Science", "Cell", "PNAS", "PLOS ONE", "arXiv", 
        "IEEE Transactions on Pattern Analysis and Machine Intelligence",
        "Journal of Machine Learning Research", "Neural Information Processing Systems"
    ]
    
    keywords_list = [
        "machine learning", "artificial intelligence", "deep learning", "neural networks",
        "computer vision", "natural language processing", "reinforcement learning",
        "data science", "big data", "cloud computing", "blockchain", "cybersecurity"
    ]
    
    papers = []
    
    for i in range(num_papers):
        # Generate random publication date within last 30 days
        days_ago = np.random.randint(0, 30)
        pub_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        # Generate realistic title
        title_words = np.random.choice([
            "Novel", "Advanced", "Improved", "Efficient", "Scalable", "Robust",
            "Deep", "Neural", "Machine", "Learning", "Analysis", "Framework",
            "System", "Model", "Algorithm", "Method", "Approach", "Technique"
        ], size=np.random.randint(4, 8))
        title = " ".join(title_words) + " for " + np.random.choice(keywords_list).title()
        
        # Generate authors
        num_authors = np.random.randint(1, 5)
        authors = []
        for j in range(num_authors):
            first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa"]
            last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
            author = f"{np.random.choice(first_names)} {np.random.choice(last_names)}"
            authors.append(author)
        authors_str = "; ".join(authors)
        
        # Generate abstract
        abstract_sentences = [
            f"This paper presents a novel approach to {np.random.choice(keywords_list)}.",
            f"We propose an efficient algorithm that improves upon existing methods.",
            f"Experimental results demonstrate significant performance improvements.",
            f"Our framework achieves state-of-the-art results on benchmark datasets.",
            f"The proposed method shows promising results in real-world applications."
        ]
        abstract = " ".join(np.random.choice(abstract_sentences, size=np.random.randint(3, 6)))
        
        # Generate full text (simplified)
        full_text = abstract + " " + " ".join([
            "The remainder of this paper is organized as follows. Section 2 provides background information.",
            "Section 3 describes our proposed methodology. Section 4 presents experimental results.",
            "Section 5 concludes with future work directions."
        ])
        
        # Generate DOI (optional - some papers might not have one)
        doi = None
        if np.random.random() > 0.2:  # 80% of papers have DOI
            doi = f"10.1000/{uuid.uuid4().hex[:8]}"
        
        # Generate keywords
        num_keywords = np.random.randint(2, 6)
        keywords = "; ".join(np.random.choice(keywords_list, size=num_keywords, replace=False))
        
        # Generate PDF URL
        pdf_url = f"https://example.com/papers/{uuid.uuid4().hex[:8]}.pdf"
        
        paper = {
            "doi": doi,
            "title": title,
            "authors": authors_str,
            "abstract": abstract,
            "full_text": full_text,
            "journal": np.random.choice(journals),
            "publication_date": pub_date,
            "keywords": keywords,
            "pdf_url": pdf_url,
            "source": source_name,
            "ingestion_timestamp": datetime.now().strftime("%Y-%m-%d")
        }
        
        papers.append(paper)
    
    return pd.DataFrame(papers)

def create_sample_data_files():
    """Create sample CSV files for different sources"""
    
    # Create data directory if it doesn't exist
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate data for different sources
    sources = {
        "arxiv": 150,
        "pubmed": 120,
        "ieee": 80,
        "acm": 100
    }
    
    for source_name, num_papers in sources.items():
        df = generate_sample_papers(num_papers, source_name)
        
        # Save to CSV
        filename = f"{data_dir}/{source_name}_sample.csv"
        df.to_csv(filename, index=False)
        print(f"Generated {filename} with {len(df)} papers")
        
        # Display sample
        print(f"\nSample from {source_name}:")
        print(f"Title: {df.iloc[0]['title']}")
        print(f"Authors: {df.iloc[0]['authors']}")
        print(f"Date: {df.iloc[0]['publication_date']}")
        print("-" * 50)

def create_s3_upload_script():
    """Create a script to upload sample data to S3"""
    
    script_content = f"""#!/bin/bash
# Upload sample data to S3
# Make sure you have AWS CLI configured

BUCKET="{S3_BUCKET}"

# Create raw data directories in S3
aws s3 mb s3://$BUCKET --region us-east-1 2>/dev/null || true

# Upload sample data
aws s3 cp data/arxiv_sample.csv s3://$BUCKET/raw/arxiv/
aws s3 cp data/pubmed_sample.csv s3://$BUCKET/raw/pubmed/
aws s3 cp data/ieee_sample.csv s3://$BUCKET/raw/ieee/
aws s3 cp data/acm_sample.csv s3://$BUCKET/raw/acm/

echo "Sample data uploaded to s3://$BUCKET/raw/"
"""
    
    with open("upload_to_s3.sh", "w") as f:
        f.write(script_content)
    
    os.chmod("upload_to_s3.sh", 0o755)
    print("Created upload_to_s3.sh script")

if __name__ == "__main__":
    print("Generating sample academic paper data...")
    create_sample_data_files()
    create_s3_upload_script()
    print("\nSample data generation complete!")
    print("Run './upload_to_s3.sh' to upload the data to S3") 