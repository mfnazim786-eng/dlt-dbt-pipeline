import dlt
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline
try:
    from .mongodb import mongodb, mongodb_collection 
except ImportError:
    from mongodb import mongodb, mongodb_collection
import dlt.destinations
# import json
import duckdb
from dlt import dbt
import datetime
from google.cloud import storage
import os
import logging
import tempfile
from bigquery_handler import update_job_status
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# GCS_BUCKET_NAME = "duckdb_bucket"
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") 
# DUCKDB_FILE_NAME = "local_mongo.duckdb"
DUCKDB_FILE_NAME = os.getenv("DUCKDB_FILE_NAME") 

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        if blob.exists():
            # print(f"Downloading {source_blob_name} from bucket {bucket_name}...")
            logging.info(f"Downloading {source_blob_name} from bucket {bucket_name}...")
            blob.download_to_filename(destination_file_name)
            # print("Download complete.")
            logging.info("Download complete.")   
        else:
            # print(f"{source_blob_name} does not exist in the bucket. A new database will be created.")
            logging.warning(f"{source_blob_name} does not exist in the bucket. A new database will be created.")
    except Exception as e:
        # print(f"Failed to download from GCS: {e}")
        logging.error(f"Failed to download from GCS: {e}")
        raise
        
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        # print(f"Uploading {source_file_name} to bucket {bucket_name}...")
        logging.info(f"Uploading {source_file_name} to bucket {bucket_name}...")
        blob.upload_from_filename(source_file_name)
        # print("Upload complete.")
        logging.info("Upload complete.")
    except Exception as e:
        # print(f"Failed to upload to GCS: {e}")
        logging.error(f"Failed to upload to GCS: {e}")
        raise


# --- STAGE 1: LOAD DATA FROM MONGODB TO DUCKDB ---
def load_mongo_to_duckdb(pipeline: Pipeline) -> LoadInfo:
    """
    Loads collections from the MongoDB database specified in config.toml into the DuckDB destination.
    """
    logging.info("\n--- STAGE 1: Loading data from MongoDB to DuckDB ---")

    source = mongodb()

    incremental_config = {
        "franchiseCBSale": {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "salesCB":         {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "targetsCB":       {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "targets":         {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "discounts":       {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "itemSale":        {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "sales":           {"cursor": "createdBy.date", "initial_start_days_ago": 10, "end_days_ago": 0},
        "checkins":        {"cursor": "lastCheckIn",    "initial_start_days_ago": 10, "end_days_ago": 0}
    }

    for resource_name, config in incremental_config.items():
        initial_start_days_ago = config["initial_start_days_ago"]
        cursor_column = config["cursor"]
        start_from_date = datetime.date.today() - datetime.timedelta(days=initial_start_days_ago)
        initial_start_date = datetime.datetime(
            start_from_date.year, start_from_date.month, start_from_date.day, tzinfo=datetime.timezone.utc
        )

        end_days_ago = config["end_days_ago"]
        end_from_date = datetime.date.today() - datetime.timedelta(days=end_days_ago)
        end_date = datetime.datetime.combine(end_from_date, datetime.time.max, tzinfo=datetime.timezone.utc)

        # print(f"Applying hints to '{resource_name}': loading from {initial_start_date.date()} up to {end_date.date()}")
        logging.info(f"Applying hints to '{resource_name}': loading from {initial_start_date.date()} up to {end_date.date()}")

        resource = source.resources[resource_name]

        resource.apply_hints(
            primary_key="_id",
            incremental=dlt.sources.incremental(
                cursor_column,
                initial_value=initial_start_date,
                end_value=end_date 
            )
        )

    replace_collections = ["houses","brands","franchiseCBs","franchiseCBPrices","items","brandCBs","access_roles","customers","categoryCBs","cities",
                           "currencies","malls","markets","productCBs","shops","subCategoryCBs","users"]
    
    column_hints = {
        "cities": {
            "logo": {"data_type": "text"}
        },
        "currencies": {
            "created_by__user": {"data_type": "text"},
            "edited_by__user": {"data_type": "text"}
        },
        "malls": {
            "logo": {"data_type": "text"}
        },
        "productCBs": {
            "logo": {"data_type": "text"}
        },
        "users": {
            "designation": {"data_type": "text"},
            "training_manager": {"data_type": "text"},
            "distributor": {"data_type": "text"},
            "level": {"data_type": "text"}
        }
    }

    for resource_name in replace_collections:
        resource = source.resources[resource_name]
        hints_to_apply = {"write_disposition": "replace"}
        if resource_name in column_hints:
            hints_to_apply["columns"] = column_hints[resource_name]
        resource.apply_hints(**hints_to_apply)

    info = pipeline.run(source)
    logging.info("--- STAGE 1: Finished ---")
    return info


# --- STAGE 2: TRANSFORM WITH DBT ---
def transform_data_in_duckdb(dbt_pipeline: Pipeline) -> None:
    """
    Runs the dbt project to transform the raw data in DuckDB.
    """
    logging.info("\n--- STAGE 2: Transforming data in DuckDB with dbt ---")

    # Use the 'package' helper function to correctly create the dbt runner.
    # This function takes the pipeline and the path to the dbt project.
    dbt_runner = dbt.package(
        pipeline=dbt_pipeline,
        package_location="mongo_transforms"
    )

    # The run_all() method executes 'dbt run'
    models = dbt_runner.run_all()
    
    # for m in models:
        # logging.info(f"dbt model {m.model_name} ran successfully with status {m.status} and message {m.message}")
    
    logging.info("--- STAGE 2: Finished ---")


# --- STAGE 3: LOAD ALL DUCKDB TABLES TO BIGQUERY ---
def load_all_duckdb_to_bigquery(duckdb_pipeline: Pipeline, db_path: str):
    """
    Loads BOTH the raw tables and the transformed dbt models from DuckDB to BigQuery.
    """
    logging.info("\n--- STAGE 3: Loading all data from DuckDB to BigQuery ---")

    tables_to_replace = [
        "houses_duckdb","brands_duckdb","shades_unpivoting_duckdb","shades_duckdb","fcb_sales_shades_duckdb","franchisecbprices_duckdb","access_roles_duckdb",
        "brandcbs_duckdb","customers_duckdb","items_duckdb","categorycbs_duckdb","cities_duckdb","currencies_duckdb","malls_duckdb","markets_duckdb","shops_duckdb",
        "productcbs_duckdb","subcategorycbs_duckdb","users_duckdb","franchisecbs_duckdb"
    ]
    # These tables will be merged. New data will be inserted, existing data will be updated.
    tables_to_merge = {
        "itemsales_duckdb": "_id",
        "fcb_sales_duckdb": "_id",
        "salescb_duckdb": "_id",
        "sales_duckdb": "_id",
        "discounts_duckdb": "_id",
        "checkins_duckdb":"_id",
        "targets_duckdb":"_id",
        "targetscb_duckdb":"_id"
    }
    
    tables_to_load_to_bigquery = tables_to_replace + list(tables_to_merge.keys())
    schema_name = duckdb_pipeline.dataset_name

    try:
        with duckdb.connect(db_path, read_only=True) as con:
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
            all_existing_tables = [table[0] for table in con.execute(query).fetchall() if not table[0].startswith('_dlt_')]
            final_tables_to_load = [table for table in all_existing_tables if table in tables_to_load_to_bigquery]
            
            if not final_tables_to_load:
                 logging.warning(f"None of the specified tables found in the DuckDB schema '{schema_name}'. Skipping Stage 3.")
                 return

            logging.info(f"Found {len(final_tables_to_load)} tables to load to BigQuery.")

    except duckdb.IOException as e:
        logging.error(f"Error connecting to DuckDB file: {e}. Stage 3 will not run.")
        return

    # The source will now load all discovered tables from the single, correct schema
    @dlt.source
    def duckdb_full_source(db_path, single_schema, tables_to_load):

        with duckdb.connect(db_path, read_only=True) as con:
            for table in tables_to_load:
                # print(f"Creating a dlt resource for table: {single_schema}.{table}")
                arrow_table = con.execute(f"SELECT * FROM {single_schema}.{table}").fetch_arrow_table()
                
                if table in tables_to_merge:
                    primary_key = tables_to_merge[table]
                    # print(f"--> Applying 'merge' disposition to '{table}' with primary key '{primary_key}'")
                    yield dlt.resource(
                        arrow_table, 
                        name=table, 
                        write_disposition='merge', 
                        primary_key=primary_key
                    )
                elif table in tables_to_replace:
                    # print(f"--> Applying 'replace' disposition to '{table}'")
                    yield dlt.resource(
                        arrow_table, 
                        name=table, 
                        write_disposition='replace'
                    )

    bigquery_pipeline = dlt.pipeline(
        pipeline_name="duckdb_full_to_bigquery",
        pipelines_dir=".",
        destination=dlt.destinations.bigquery(),
        dataset_name="mongo_dbt_data"
    )

    if final_tables_to_load:
        info = bigquery_pipeline.run(duckdb_full_source(db_path, schema_name, final_tables_to_load)
            )
        # print("--- STAGE 3: Finished ---")
        logging.info("--- STAGE 3: Finished ---")
        logging.info(info)
    else:
        logging.info("--- STAGE 3: No tables found in DuckDB to load. Skipping. ---")


def run_full_pipeline(job_id: str):
    try:
        update_job_status(job_id, "running", "Pipeline has started. Preparing local database.")

        # On Cloud Run, this is an in-memory filesystem.
        # Use tempfile to create an OS-agnostic temporary path
        temp_dir = tempfile.gettempdir()
        local_db_path = os.path.join(temp_dir, DUCKDB_FILE_NAME)
        logging.info(f"Using temporary database path: {local_db_path}")

        # Create the full DSN (connection string) for the dlt destination
        duckdb_dsn = f"duckdb:///{local_db_path}"

        # Download the DuckDB file from GCS before the pipeline runs
        download_from_gcs(GCS_BUCKET_NAME, DUCKDB_FILE_NAME, local_db_path)

        # dlt.config["runtime.log_level"] = "ERROR"
        dlt.config["schema.naming_convention"] = "direct"

        mongo_pipeline = dlt.pipeline(
            pipeline_name="local_mongo",
            pipelines_dir=".",
            destination=dlt.destinations.duckdb(credentials=duckdb_dsn),
            dataset_name="mongo_select",
        )

        update_job_status(job_id, "running", "Stage 1: Loading data from MongoDB to DuckDB.")
        load_info = load_mongo_to_duckdb(mongo_pipeline)

        if not load_info.has_failed_jobs:
            update_job_status(job_id, "running", "Stage 2: Transforming data with dbt.")
            transform_data_in_duckdb(mongo_pipeline)

            update_job_status(job_id, "running", "Stage 3: Loading all data from DuckDB to BigQuery.")
            load_all_duckdb_to_bigquery(mongo_pipeline, local_db_path)
        else:
            # logging.error("Stage 1 (MongoDB to DuckDB) failed. Downstream stages will not run.")
            raise RuntimeError("Stage 1 (MongoDB to DuckDB) failed. Halting pipeline.")

        update_job_status(job_id, "running", "Stage 4: Performing database maintenance.")
        logging.info("\n--- STAGE 4: Performing database maintenance and state upload ---")

        try:
            logging.info(f"Connecting to {local_db_path} to perform data maintenance...")

            with duckdb.connect(local_db_path) as con:
                # Raw incremental tables (temporary holding area for the latest batch)
                incremental_tables_to_clear = [
                    "franchise_cb_sale", "item_sale", "sales_cb", "sales", 
                    "targets_cb", "targets", "discounts", "checkins"
                ]
                
                # DBT transformed models (their final destination is BigQuery)
                dbt_models_to_clear = [
                    "houses_duckdb","brands_duckdb","shades_unpivoting_duckdb","shades_duckdb","fcb_sales_shades_duckdb",
                    "franchisecbprices_duckdb","access_roles_duckdb", "brandcbs_duckdb","customers_duckdb","items_duckdb",
                    "categorycbs_duckdb","cities_duckdb","currencies_duckdb","malls_duckdb","markets_duckdb","shops_duckdb",
                    "productcbs_duckdb","subcategorycbs_duckdb","users_duckdb", "itemsales_duckdb", "fcb_sales_duckdb", "franchisecbs_duckdb",
                    "salescb_duckdb", "sales_duckdb", "discounts_duckdb", "checkins_duckdb", "targets_duckdb", "targetscb_duckdb"
                ]

                all_tables_to_clear = incremental_tables_to_clear + dbt_models_to_clear

                logging.info(f"Clearing data from {len(all_tables_to_clear)} temporary raw and transformed tables...")
                for table in all_tables_to_clear:
                    table_exists_query = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'mongo_select' AND table_name = '{table}'"
                    if con.execute(table_exists_query).fetchone():
                        con.execute(f"DELETE FROM mongo_select.{table};")
                logging.info("Data clearing complete. Performing VACUUM to shrink file size...")
                con.execute("VACUUM")
                logging.info("VACUUM command completed successfully.")
        except Exception as e:
            logging.exception("An error occurred during local database maintenance.")
        
        if os.path.exists(local_db_path):
            try:
                file_size_mb = os.path.getsize(local_db_path) / (1024 * 1024)
                # logging.info(f"Final file size is {file_size_mb:.2f} MB. Uploading state to GCS...")
                update_job_status(job_id, "running", f"Final state size is {file_size_mb:.2f} MB. Uploading to GCS.")
                upload_to_gcs(GCS_BUCKET_NAME, local_db_path, DUCKDB_FILE_NAME)
            except Exception as e:
                # This is a critical failure, log it with the highest severity
                logging.critical(f"FINAL STATE UPLOAD FAILED: The final DuckDB state could not be saved to GCS. {e}")
                raise

        update_job_status(job_id, "completed", "Pipeline finished successfully.")
        logging.info("--- Pipeline script finished ---")

    except Exception as e:
        logging.exception(f"Pipeline job {job_id} failed with an error.")
        error_details = f"An error occurred: {str(e)}"
        update_job_status(job_id, "failed", error_details)
        raise