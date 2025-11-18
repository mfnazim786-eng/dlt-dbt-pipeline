from fastapi import FastAPI, BackgroundTasks, HTTPException
from pipeline import run_full_pipeline

# Initialize the FastAPI application
app = FastAPI(
    title="Data Pipeline Trigger API",
    description="An API to trigger the MongoDB to BigQuery data pipeline.",
    version="1.0.0",
)

@app.get("/", tags=["Health Check"])
def read_root():
    """
    A simple health check endpoint to confirm the API is running.
    """
    return {"status": "ok", "message": "API is running"}

# @app.post("/trigger-pipeline", status_code=202, tags=["Pipeline"])
# def trigger_pipeline(background_tasks: BackgroundTasks):
#     """
#     Triggers the full data pipeline to run as a background task.

#     This endpoint immediately returns a response and the pipeline
#     continues to execute in the background. This is ideal for
#     long-running processes.
#     """
#     try:
#         print("Received request to trigger the data pipeline.")
#         # Add the main pipeline function to be executed after the response is sent
#         background_tasks.add_task(run_full_pipeline)
#         return {
#             "status": "accepted",
#             "message": "Pipeline execution has been successfully triggered in the background."
#         }
#     except Exception as e:
#         # This is a fallback, but errors within the background task won't be caught here.
#         # They will appear in the Cloud Run logs.
#         print(f"Error occurred while trying to trigger the pipeline: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to trigger pipeline task: {e}"
#         )

@app.post("/trigger-pipeline", status_code=200, tags=["Pipeline"])
def trigger_pipeline(): # Remove BackgroundTasks from here
    """
    Triggers and runs the full data pipeline synchronously.

    This endpoint will wait for the entire pipeline to complete
    before returning a response.
    """
    try:
        print("Received request to trigger the data pipeline. Running synchronously.")
        
        # Call the function directly instead of adding it to background tasks.
        run_full_pipeline() 
        
        # This response is now only sent AFTER the pipeline finishes.
        return {
            "status": "completed",
            "message": "Pipeline execution has finished successfully."
        }
    except Exception as e:
        print(f"Error occurred during pipeline execution: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline failed during execution: {e}"
        )