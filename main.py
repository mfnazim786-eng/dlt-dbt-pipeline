import logging
import uuid
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pipeline import run_full_pipeline
from bigquery_handler import (
    insert_new_job,
    get_job_status,
    create_job_status_table_if_not_exists
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

app = FastAPI(
    title="Data Pipeline Trigger API",
    description="An API to trigger and monitor the MongoDB to BigQuery data pipeline.",
    version="1.1.0",
)

@app.on_event("startup")
def startup_event():
    """On application startup, ensure the BigQuery table exists."""
    logging.info("Application starting up. Checking for job_queue table in BigQuery...")
    create_job_status_table_if_not_exists()

@app.get("/", tags=["Health Check"])
def read_root():
    """A simple health check endpoint."""
    return {"status": "ok", "message": "API is running"}

@app.post("/trigger-pipeline", status_code=202, tags=["Pipeline"])
def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    Triggers the data pipeline to run as a background task.
    Returns a unique job ID that can be used to check the status.
    """
    job_id = str(uuid.uuid4())
    logging.info(f"Received request to trigger pipeline. Assigning job ID: {job_id}")
    
    try:
        # 1. Create the initial job record in BigQuery
        insert_new_job(job_id)
        
        # 2. Add the pipeline function to run in the background, passing the job_id
        background_tasks.add_task(run_full_pipeline, job_id)
        
        # 3. Immediately return the job ID to the client
        return {
            "status": "accepted",
            "message": "Pipeline execution has been triggered in the background.",
            "job_id": job_id
        }
    except Exception as e:
        logging.exception("Failed to schedule pipeline task.")
        raise HTTPException(status_code=500, detail=f"Failed to schedule pipeline task: {e}")
    
# @app.post("/trigger-pipeline", status_code=200, tags=["Pipeline"])
# def trigger_pipeline(): # Remove BackgroundTasks from here
#     """
#     Triggers and runs the full data pipeline synchronously.

#     This endpoint will wait for the entire pipeline to complete
#     before returning a response.
#     """
#     try:
#         logging.info("Received request to trigger the data pipeline. Running synchronously.")
#         # print("Received request to trigger the data pipeline. Running synchronously.")
        
#         # Call the function directly instead of adding it to background tasks.
#         run_full_pipeline() 
        
#         # This response is now only sent AFTER the pipeline finishes.
#         logging.info("Pipeline execution has finished successfully.")
#         return {
#             "status": "completed",
#             "message": "Pipeline execution has finished successfully."
#         }
#     except Exception as e:
#         # print(f"Error occurred during pipeline execution: {e}")
#         # logging.exception will automatically include the stack trace, which is very useful
#         logging.exception("An unhandled error occurred during pipeline execution.")
#         raise HTTPException(
#             status_code=500,
#             detail=f"Pipeline failed during execution: {e}"
#         )

@app.get("/job-status/{job_id}", tags=["Pipeline"])
def check_job_status(job_id: str):
    """
    Checks the status of a pipeline job using its job ID.
    """
    logging.info(f"Received request to check status for job ID: {job_id}")
    status_info = get_job_status(job_id)
    
    if status_info:
        return status_info
    else:
        raise HTTPException(status_code=404, detail=f"Job with ID '{job_id}' not found.")