#!/usr/bin/env python3
"""
Separated services approach for academic papers ingestion.
This separates PDF downloads (Python service) from data processing (PySpark job).
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Any
import boto3
import os
from urllib.parse import urlparse
from config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class PDFDownloadService:
    """Service for downloading PDFs and storing metadata in S3"""
    
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        
    def generate_paper_id(self, paper: Dict[str, Any]) -> str:
        """Generate paper ID from title, authors, and date"""
        import hashlib
        
        title = paper.get("title", "").lower().strip()
        authors = paper.get("authors", "").lower().strip()
        date = paper.get("publication_date", "")
        
        # Create hash
        content = f"{title}||{authors}||{date}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def download_pdf_to_s3(self, pdf_url: str, paper_id: str, source: str) -> str:
        """Download PDF from URL and store in S3"""
        if not pdf_url:
            return None
            
        try:
            logger.info(f"Downloading PDF: {pdf_url}")
            
            # Download PDF content
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Generate S3 key
            date_prefix = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"raw_pdfs/{source}/{date_prefix}/{paper_id}.pdf"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=response.content,
                ContentType='application/pdf'
            )
            
            s3_path = f"s3://{self.s3_bucket}/{s3_key}"
            logger.info(f"PDF stored at: {s3_path}")
            
            return s3_path
            
        except Exception as e:
            logger.error(f"Error downloading PDF {pdf_url}: {str(e)}")
            return None
    
    def fetch_arxiv_papers(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """Fetch recent papers from arXiv API and download PDFs"""
        logger.info("Fetching papers from arXiv API...")
        
        # In production, you'd make actual API calls here
        sample_data = [
            {
                "title": "Advanced Machine Learning Techniques for Natural Language Processing",
                "authors": "John Doe; Jane Smith; Bob Wilson",
                "abstract": "This paper presents novel approaches to NLP using deep learning...",
                "publication_date": "2024-01-15",
                "journal": "arXiv",
                "doi": None,
                "keywords": "machine learning; natural language processing; deep learning",
                "pdf_url": "https://arxiv.org/pdf/2401.12345.pdf",
                "source": "arxiv"
            },
            {
                "title": "Efficient Computer Vision Models for Real-time Applications",
                "authors": "Alice Johnson; Charlie Brown",
                "abstract": "We propose efficient architectures for real-time computer vision...",
                "publication_date": "2024-01-14",
                "journal": "arXiv",
                "doi": None,
                "keywords": "computer vision; real-time; efficiency",
                "pdf_url": "https://arxiv.org/pdf/2401.12346.pdf",
                "source": "arxiv"
            }
        ]
        
        processed_papers = []
        
        for paper in sample_data:
            # Generate paper ID first
            paper_id = self.generate_paper_id(paper)
            
            # Download PDF to S3
            pdf_s3_path = self.download_pdf_to_s3(
                paper["pdf_url"], 
                paper_id, 
                paper["source"]
            )
            
            # Add S3 path and ID to paper data
            paper["pdf_s3_path"] = pdf_s3_path
            paper["id"] = paper_id
            processed_papers.append(paper)
            
            # Rate limiting
            time.sleep(1)
        
        logger.info(f"Processed {len(processed_papers)} papers from arXiv")
        return processed_papers
    
    def fetch_pubmed_papers(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """Fetch recent papers from PubMed API and download PDFs"""
        logger.info("Fetching papers from PubMed API...")
        
        # In production, you'd make actual API calls here
        sample_data = [
            {
                "title": "Machine Learning Applications in Healthcare: A Comprehensive Review",
                "authors": "Dr. Sarah Wilson; Dr. Michael Chen",
                "abstract": "This review examines the current state of ML in healthcare...",
                "publication_date": "2024-01-10",
                "journal": "Nature Medicine",
                "doi": "10.1038/s41591-024-01234-5",
                "keywords": "machine learning; healthcare; medical applications",
                "pdf_url": "https://example.com/paper.pdf",
                "source": "pubmed"
            }
        ]
        
        processed_papers = []
        
        for paper in sample_data:
            # Generate paper ID first
            paper_id = self.generate_paper_id(paper)
            
            # Download PDF to S3
            pdf_s3_path = self.download_pdf_to_s3(
                paper["pdf_url"], 
                paper_id, 
                paper["source"]
            )
            
            # Add S3 path and ID to paper data
            paper["pdf_s3_path"] = pdf_s3_path
            paper["id"] = paper_id
            processed_papers.append(paper)
            
            # Rate limiting
            time.sleep(1)
        
        logger.info(f"Processed {len(processed_papers)} papers from PubMed")
        return processed_papers
    
    def store_metadata_to_s3(self, papers: List[Dict[str, Any]], source: str, format: str = "parquet"):
        """Store paper metadata to S3 for PySpark processing"""
        if not papers:
            logger.warning(f"No papers to store for {source}")
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(papers)
            
            # Add ingestion timestamp
            df['ingestion_timestamp'] = datetime.now().isoformat()
            
            # Store based on format
            if format == "parquet":
                # Store as Parquet
                buffer = df.to_parquet(index=False)
                key = f"raw_metadata/{source}/{datetime.now().strftime('%Y/%m/%d')}/{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                
            elif format == "json":
                # Store as JSON
                buffer = df.to_json(orient='records', lines=True)
                key = f"raw_metadata/{source}/{datetime.now().strftime('%Y/%m/%d')}/{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=buffer
            )
            
            logger.info(f"Stored {len(papers)} papers metadata to s3://{self.s3_bucket}/{key}")
            
        except Exception as e:
            logger.error(f"Error storing metadata to S3: {str(e)}")
    
    def run_pdf_ingestion(self):
        """Run the complete PDF ingestion process"""
        logger.info("Starting PDF ingestion service...")
        
        # Fetch from different sources
        sources = [
            ("arxiv", self.fetch_arxiv_papers, "parquet"),
            ("pubmed", self.fetch_pubmed_papers, "json"),
        ]
        
        for source_name, fetch_func, format_type in sources:
            try:
                papers = fetch_func()
                self.store_metadata_to_s3(papers, source_name, format_type)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error processing {source_name}: {str(e)}")
        
        logger.info("PDF ingestion service completed")

def main():
    """Main function for PDF ingestion service"""
    s3_bucket = S3_BUCKET
    
    service = PDFDownloadService(s3_bucket)
    service.run_pdf_ingestion()

if __name__ == "__main__":
    main() 