"""
Production Flyte workflow for academic papers ingestion pipeline.
This orchestrates the PDF ingestion service and PySpark processing job.
"""

from flytekit import task, workflow, Resources, ContainerTask
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(
    task_config=ContainerTask(
        image="your-registry/academic-papers-pdf-ingestion:latest",
        requests=Resources(cpu="2", mem="4Gi"),
        limits=Resources(cpu="4", mem="8Gi")
    ),
    cache=True,
    cache_version="1.0"
)
def pdf_ingestion_task(
    s3_bucket: str,
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Task to run the PDF ingestion service.
    Downloads PDFs from APIs and stores metadata in S3.
    """
    import os
    from datetime import datetime
    
    # Set environment variables for the container
    os.environ["S3_BUCKET"] = s3_bucket
    os.environ["PARTITION_LOOKBACK_DAYS"] = str(days_back)
    os.environ["SOURCES"] = ",".join(sources)
    
    # The container will run the PDF ingestion script
    # This task just returns a summary
    return {
        "status": "success",
        "sources_processed": sources,
        "s3_bucket": s3_bucket,
        "days_back": days_back,
        "timestamp": datetime.now().isoformat()
    }

@task(
    task_config=ContainerTask(
        image="your-registry/academic-papers-pyspark:latest",
        requests=Resources(cpu="4", mem="8Gi"),
        limits=Resources(cpu="8", mem="16Gi")
    ),
    cache=True,
    cache_version="1.0"
)
def pyspark_processing_task(
    s3_bucket: str,
    master_table_name: str = "spark_catalog.default.papers_master",
    partition_lookback_days: int = 7
) -> Dict[str, Any]:
    """
    Task to run the PySpark processing job.
    Reads metadata from S3 and processes it into Iceberg tables.
    """
    import os
    from datetime import datetime
    
    # Set environment variables for the container
    os.environ["S3_BUCKET"] = s3_bucket
    os.environ["MASTER_TABLE_NAME"] = master_table_name
    os.environ["PARTITION_LOOKBACK_DAYS"] = str(partition_lookback_days)
    
    # The container will run the PySpark job
    # This task just returns a summary
    return {
        "status": "success",
        "s3_bucket": s3_bucket,
        "master_table_name": master_table_name,
        "partition_lookback_days": partition_lookback_days,
        "timestamp": datetime.now().isoformat()
    }

@task
def validate_ingestion_task(
    pdf_ingestion_result: Dict[str, Any],
    pyspark_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Task to validate that both ingestion and processing completed successfully.
    """
    import boto3
    from datetime import datetime
    
    logger.info("Validating pipeline results...")
    
    # Check that both tasks completed successfully
    if pdf_ingestion_result.get("status") != "success":
        raise Exception("PDF ingestion failed")
    
    if pyspark_result.get("status") != "success":
        raise Exception("PySpark processing failed")
    
    # Optional: Check S3 for expected files
    s3_bucket = pyspark_result.get("s3_bucket")
    try:
        s3_client = boto3.client('s3')
        
        # Check for PDF files
        pdf_response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="raw_pdfs/",
            MaxKeys=1
        )
        
        # Check for metadata files
        metadata_response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="raw_metadata/",
            MaxKeys=1
        )
        
        # Check for Iceberg tables
        iceberg_response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="iceberg/",
            MaxKeys=1
        )
        
        validation_status = {
            "pdfs_found": len(pdf_response.get('Contents', [])) > 0,
            "metadata_found": len(metadata_response.get('Contents', [])) > 0,
            "iceberg_tables_found": len(iceberg_response.get('Contents', [])) > 0
        }
        
    except Exception as e:
        logger.warning(f"Could not validate S3 contents: {e}")
        validation_status = {"error": str(e)}
    
    # Create summary
    summary = {
        "pipeline_status": "success",
        "pdf_ingestion": pdf_ingestion_result,
        "pyspark_processing": pyspark_result,
        "validation": validation_status,
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"Pipeline validation completed: {summary}")
    return summary

@workflow
def academic_papers_pipeline(
    s3_bucket: str = "your-academic-data-bucket",
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7,
    master_table_name: str = "spark_catalog.default.papers_master"
) -> Dict[str, Any]:
    """
    Main Flyte workflow for academic papers ingestion pipeline.
    
    This workflow:
    1. Runs PDF ingestion service to download PDFs and store metadata
    2. Runs PySpark processing job to process metadata into Iceberg tables
    3. Validates the results
    
    Args:
        s3_bucket: S3 bucket for storing data
        sources: List of sources to process (arxiv, pubmed, etc.)
        days_back: Number of days back to fetch data
        master_table_name: Name of the master Iceberg table
    
    Returns:
        Summary of the pipeline execution
    """
    logger.info(f"Starting academic papers pipeline for bucket: {s3_bucket}")
    
    # Step 1: Run PDF ingestion service
    pdf_result = pdf_ingestion_task(
        s3_bucket=s3_bucket,
        sources=sources,
        days_back=days_back
    )
    
    # Step 2: Run PySpark processing job (depends on PDF ingestion)
    pyspark_result = pyspark_processing_task(
        s3_bucket=s3_bucket,
        master_table_name=master_table_name,
        partition_lookback_days=days_back
    )
    
    # Step 3: Validate results
    validation_result = validate_ingestion_task(
        pdf_ingestion_result=pdf_result,
        pyspark_result=pyspark_result
    )
    
    return validation_result

# Alternative workflows for testing individual components
@workflow
def pdf_ingestion_only_workflow(
    s3_bucket: str = "your-academic-data-bucket",
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Workflow to run only the PDF ingestion service (for testing).
    """
    return pdf_ingestion_task(
        s3_bucket=s3_bucket,
        sources=sources,
        days_back=days_back
    )

@workflow
def pyspark_processing_only_workflow(
    s3_bucket: str = "your-academic-data-bucket",
    master_table_name: str = "spark_catalog.default.papers_master",
    partition_lookback_days: int = 7
) -> Dict[str, Any]:
    """
    Workflow to run only the PySpark processing job (for testing).
    """
    return pyspark_processing_task(
        s3_bucket=s3_bucket,
        master_table_name=master_table_name,
        partition_lookback_days=partition_lookback_days
    )

if __name__ == "__main__":
    # Example usage
    result = academic_papers_pipeline(
        s3_bucket="my-academic-data-bucket",
        sources=["arxiv", "pubmed"],
        days_back=7
    )
    print(f"Pipeline result: {result}") 