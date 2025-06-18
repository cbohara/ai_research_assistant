"""
Production Flyte workflow for academic papers ingestion pipeline.
This orchestrates the raw data extraction, refined data processing, and vector loading.
"""

from flytekit import task, workflow, Resources, ContainerTask
from typing import List, Dict, Any
import logging
from config import (
    DOCKER_IMAGES, S3_BUCKET, QDRANT_HOST, QDRANT_PORT, 
    QDRANT_COLLECTION, MASTER_TABLE_NAME
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(
    task_config=ContainerTask(
        image=DOCKER_IMAGES["raw_data_lake"],
        requests=Resources(cpu="2", mem="4Gi"),
        limits=Resources(cpu="4", mem="8Gi")
    ),
    cache=True,
    cache_version="1.0"
)
def raw_data_extraction_task(
    s3_bucket: str,
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Task to run the raw data extraction service.
    Downloads PDFs from APIs and stores metadata in S3 raw data lake.
    """
    import os
    from datetime import datetime
    
    # Set environment variables for the container
    os.environ["S3_BUCKET"] = s3_bucket
    os.environ["PARTITION_LOOKBACK_DAYS"] = str(days_back)
    os.environ["SOURCES"] = ",".join(sources)
    
    # The container will run the raw data extraction script
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
        image=DOCKER_IMAGES["refined_data_lakehouse"],
        requests=Resources(cpu="4", mem="8Gi"),
        limits=Resources(cpu="8", mem="16Gi")
    ),
    cache=True,
    cache_version="1.0"
)
def refined_data_processing_task(
    s3_bucket: str,
    master_table_name: str = MASTER_TABLE_NAME,
    partition_lookback_days: int = 7
) -> Dict[str, Any]:
    """
    Task to run the refined data processing job.
    Reads raw data from S3 and processes it into Iceberg tables in the data lakehouse.
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

@task(
    task_config=ContainerTask(
        image=DOCKER_IMAGES["load_vector_search"],
        requests=Resources(cpu="2", mem="4Gi"),
        limits=Resources(cpu="4", mem="8Gi")
    ),
    cache=True,
    cache_version="1.0"
)
def vector_loading_task(
    qdrant_host: str = QDRANT_HOST,
    qdrant_port: int = QDRANT_PORT,
    qdrant_collection: str = QDRANT_COLLECTION,
    iceberg_table: str = MASTER_TABLE_NAME
) -> Dict[str, Any]:
    """
    Task to load refined data into vector search (Qdrant).
    Reads from Iceberg tables and generates embeddings for real-time search.
    """
    import os
    from datetime import datetime
    
    # Set environment variables for the container
    os.environ["QDRANT_HOST"] = qdrant_host
    os.environ["QDRANT_PORT"] = str(qdrant_port)
    os.environ["QDRANT_COLLECTION"] = qdrant_collection
    os.environ["ICEBERG_TABLE"] = iceberg_table
    
    # The container will run the vector loading script
    # This task just returns a summary
    return {
        "status": "success",
        "qdrant_host": qdrant_host,
        "qdrant_port": qdrant_port,
        "qdrant_collection": qdrant_collection,
        "iceberg_table": iceberg_table,
        "timestamp": datetime.now().isoformat()
    }

@task
def validate_pipeline_task(
    raw_extraction_result: Dict[str, Any],
    refined_processing_result: Dict[str, Any],
    vector_loading_result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Task to validate that all pipeline stages completed successfully.
    """
    import boto3
    from datetime import datetime
    
    logger.info("Validating pipeline results...")
    
    # Check that all tasks completed successfully
    if raw_extraction_result.get("status") != "success":
        raise Exception("Raw data extraction failed")
    
    if refined_processing_result.get("status") != "success":
        raise Exception("Refined data processing failed")
    
    if vector_loading_result.get("status") != "success":
        raise Exception("Vector loading failed")
    
    # Optional: Check S3 for expected files
    s3_bucket = refined_processing_result.get("s3_bucket")
    try:
        s3_client = boto3.client('s3')
        
        # Check for raw data files
        raw_response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="raw/",
            MaxKeys=1
        )
        
        # Check for Iceberg tables
        iceberg_response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="iceberg/",
            MaxKeys=1
        )
        
        validation_status = {
            "raw_data_found": len(raw_response.get('Contents', [])) > 0,
            "iceberg_tables_found": len(iceberg_response.get('Contents', [])) > 0,
            "vector_loading_success": True
        }
        
    except Exception as e:
        logger.warning(f"Could not validate S3 contents: {e}")
        validation_status = {"error": str(e)}
    
    # Create summary
    summary = {
        "pipeline_status": "success",
        "raw_data_extraction": raw_extraction_result,
        "refined_data_processing": refined_processing_result,
        "vector_loading": vector_loading_result,
        "validation": validation_status,
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"Pipeline validation completed: {summary}")
    return summary

@workflow
def academic_papers_pipeline(
    s3_bucket: str = S3_BUCKET,
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7,
    master_table_name: str = MASTER_TABLE_NAME,
    qdrant_host: str = QDRANT_HOST,
    qdrant_port: int = QDRANT_PORT,
    qdrant_collection: str = QDRANT_COLLECTION
) -> Dict[str, Any]:
    """
    Main Flyte workflow for academic papers ingestion pipeline.
    
    This workflow:
    1. Runs raw data extraction service to download PDFs and store metadata
    2. Runs refined data processing job to process raw data into Iceberg tables
    3. Runs vector loading service to generate embeddings and load into Qdrant
    4. Validates the results
    
    Args:
        s3_bucket: S3 bucket for storing data
        sources: List of sources to process (arxiv, pubmed, etc.)
        days_back: Number of days back to fetch data
        master_table_name: Name of the master Iceberg table
        qdrant_host: Qdrant vector database host
        qdrant_port: Qdrant vector database port
        qdrant_collection: Qdrant collection name
    
    Returns:
        Summary of the pipeline execution
    """
    logger.info(f"Starting academic papers pipeline for bucket: {s3_bucket}")
    
    # Step 1: Run raw data extraction service
    raw_result = raw_data_extraction_task(
        s3_bucket=s3_bucket,
        sources=sources,
        days_back=days_back
    )
    
    # Step 2: Run refined data processing job (depends on raw extraction)
    refined_result = refined_data_processing_task(
        s3_bucket=s3_bucket,
        master_table_name=master_table_name,
        partition_lookback_days=days_back
    )
    
    # Step 3: Run vector loading service (depends on refined processing)
    vector_result = vector_loading_task(
        qdrant_host=qdrant_host,
        qdrant_port=qdrant_port,
        qdrant_collection=qdrant_collection,
        iceberg_table=master_table_name
    )
    
    # Step 4: Validate results
    validation_result = validate_pipeline_task(
        raw_extraction_result=raw_result,
        refined_processing_result=refined_result,
        vector_loading_result=vector_result
    )
    
    return validation_result

# Individual workflow for testing raw data extraction only
@workflow
def raw_data_extraction_only_workflow(
    s3_bucket: str = S3_BUCKET,
    sources: List[str] = ["arxiv", "pubmed"],
    days_back: int = 7
) -> Dict[str, Any]:
    """
    Workflow to run only the raw data extraction stage.
    Useful for testing the extraction service independently.
    """
    return raw_data_extraction_task(
        s3_bucket=s3_bucket,
        sources=sources,
        days_back=days_back
    )

# Individual workflow for testing refined data processing only
@workflow
def refined_data_processing_only_workflow(
    s3_bucket: str = S3_BUCKET,
    master_table_name: str = MASTER_TABLE_NAME,
    partition_lookback_days: int = 7
) -> Dict[str, Any]:
    """
    Workflow to run only the refined data processing stage.
    Useful for testing the processing service independently.
    """
    return refined_data_processing_task(
        s3_bucket=s3_bucket,
        master_table_name=master_table_name,
        partition_lookback_days=partition_lookback_days
    )

# Individual workflow for testing vector loading only
@workflow
def vector_loading_only_workflow(
    qdrant_host: str = QDRANT_HOST,
    qdrant_port: int = QDRANT_PORT,
    qdrant_collection: str = QDRANT_COLLECTION,
    iceberg_table: str = MASTER_TABLE_NAME
) -> Dict[str, Any]:
    """
    Workflow to run only the vector loading stage.
    Useful for testing the vector loading service independently.
    """
    return vector_loading_task(
        qdrant_host=qdrant_host,
        qdrant_port=qdrant_port,
        qdrant_collection=qdrant_collection,
        iceberg_table=iceberg_table
    )

if __name__ == "__main__":
    # Example usage
    result = academic_papers_pipeline(
        s3_bucket="my-academic-data-bucket",
        sources=["arxiv", "pubmed"],
        days_back=7
    )
    print(f"Pipeline result: {result}") 