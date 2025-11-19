import os
import logging
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

GCP_PROJECT = os.getenv("GCP_PROJECT") 
# GCP_PROJECT = "zippy-brand-176907"
BQ_DATASET = os.getenv("BQ_DATASET", "mongo_dbt_data")
# BQ_DATASET = "mongo_dbt_data"
BQ_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.job_queue"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- BigQuery Client Initialization ---
try:
    client = bigquery.Client()
    if not GCP_PROJECT:
        GCP_PROJECT = client.project
        BQ_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.job_queue"
        logging.info(f"GCP Project auto-detected as: {GCP_PROJECT}")
except Exception as e:
    client = None
    logging.error(f"Could not initialize BigQuery client. Ensure authentication is configured correctly. Error: {e}")

def create_job_status_table_if_not_exists():
    """Checks if the status table exists and creates it if it doesn't."""
    if not client:
        logging.error("BigQuery client is not available. Cannot create table.")
        return
    try:
        client.get_table(BQ_TABLE)
        logging.info(f"Table {BQ_TABLE} already exists.")
    except NotFound:
        logging.info(f"Table {BQ_TABLE} not found. Creating table...")
        schema = [
            bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("details", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(BQ_TABLE, schema=schema)
        client.create_table(table)
        logging.info(f"Successfully created table {BQ_TABLE}.")
    except Exception as e:
        logging.error(f"An error occurred while creating the BigQuery table: {e}")

def insert_new_job(job_id: str):
    """Inserts the initial job record with 'started' status using a DML query."""
    if not client:
        logging.error("BigQuery client not available. Cannot insert job.")
        return

    timestamp_str = datetime.now(timezone.utc).isoformat()
    
    # Use a standard SQL INSERT statement
    query = f"""
        INSERT INTO `{BQ_TABLE}` (job_id, status, details, created_at, updated_at)
        VALUES (@job_id, @status, @details, @timestamp, @timestamp)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            bigquery.ScalarQueryParameter("status", "STRING", "started"),
            bigquery.ScalarQueryParameter(
                "details", "STRING", "Job has been accepted and is waiting to run."
            ),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp_str),
        ]
    )
    
    try:
        # Execute the query
        client.query(query, job_config=job_config).result()
        logging.info(f"Successfully inserted new job with ID: {job_id}")
    except Exception as e:
        logging.error(f"Failed to insert job {job_id} using DML: {e}")
        raise

def update_job_status(job_id: str, status: str, details: str = ""):
    """Updates the status and details of an existing job."""
    if not client: return
    
    timestamp_str = datetime.now(timezone.utc).isoformat()
    
    query = f"""
        UPDATE `{BQ_TABLE}`
        SET status = @status, details = @details, updated_at = @timestamp
        WHERE job_id = @job_id
    """
    
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("details", "STRING", details),
        bigquery.ScalarQueryParameter("timestamp", "STRING", timestamp_str),
        bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
    ])
    
    try:
        client.query(query, job_config=job_config).result()
        logging.info(f"Successfully updated job {job_id} to status: {status}")
    except Exception as e:
        logging.error(f"Failed to update job {job_id} in BigQuery: {e}")

def get_job_status(job_id: str):
    """Retrieves the status of a specific job."""
    if not client: return None
    query = f"SELECT * FROM `{BQ_TABLE}` WHERE job_id = @job_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )
    try:
        rows = list(client.query(query, job_config=job_config).result())
        return dict(rows[0]) if rows else None
    except Exception as e:
        logging.error(f"Failed to retrieve job status for {job_id}: {e}")
        return None