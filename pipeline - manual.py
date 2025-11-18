import dlt
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline
try:
    from .mongodb import mongodb
except ImportError:
    from mongodb import mongodb
import dlt.destinations
import duckdb
from dlt import dbt
import datetime

# --- STAGE 1: LOAD DATA FROM MONGODB TO DUCKDB ---
def load_mongo_to_duckdb(pipeline: Pipeline) -> LoadInfo:
    print("\n--- STAGE 1: Loading data from MongoDB to DuckDB ---")
    source = mongodb()

    initial_start_date = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(2025, 11, 17, 23, 59, 59, tzinfo=datetime.timezone.utc)

    print(f"Loading data for all incremental tables FROM {initial_start_date.date()} TO {end_date.date()}")

    incremental_resources = {
        "franchiseCBSale": "createdBy.date",
        "salesCB":         "createdBy.date",
        "targetsCB":       "createdBy.date",
        "targets":         "createdBy.date",
        "discounts":       "createdBy.date",
        "itemSale":        "createdBy.date",
        "sales":           "createdBy.date",
        "checkins":        "lastCheckIn"
    }

    for resource_name, cursor_column in incremental_resources.items():
        print(f"Applying hints to '{resource_name}'...")
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
    print("--- STAGE 1: Finished ---")
    return info


# --- STAGE 2: TRANSFORM WITH DBT ---
def transform_data_in_duckdb(dbt_pipeline: Pipeline) -> None:
    print("\n--- STAGE 2: Transforming data in DuckDB with dbt ---")
    dbt_runner = dbt.package(
        pipeline=dbt_pipeline,
        package_location="mongo_transforms"
    )
    dbt_runner.run_all()
    print("--- STAGE 2: Finished ---")


# --- STAGE 3: LOAD ALL DUCKDB TABLES TO BIGQUERY ---
def load_all_duckdb_to_bigquery(duckdb_pipeline: Pipeline):
    print("\n--- STAGE 3: Loading all data from DuckDB to BigQuery ---")
    tables_to_replace = [
        "houses_duckdb","brands_duckdb","shades_unpivoting_duckdb","shades_duckdb","fcb_sales_shades_duckdb","franchisecbprices_duckdb","access_roles_duckdb","brandcbs_duckdb",
        "customers_duckdb","items_duckdb","categorycbs_duckdb","cities_duckdb","currencies_duckdb","malls_duckdb","markets_duckdb","shops_duckdb","productcbs_duckdb",
        "subcategorycbs_duckdb","users_duckdb"
    ]
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
    duckdb_db_path = f"{duckdb_pipeline.pipeline_name}.duckdb"
    schema_name = duckdb_pipeline.dataset_name
    try:
        with duckdb.connect(duckdb_db_path, read_only=True) as con:
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
            all_existing_tables = [table[0] for table in con.execute(query).fetchall() if not table[0].startswith('_dlt_')]
            final_tables_to_load = [table for table in all_existing_tables if table in tables_to_load_to_bigquery]
            if not final_tables_to_load:
                 print(f"Warning: None of the specified tables {tables_to_load_to_bigquery} were found in the DuckDB schema '{schema_name}'.")
                 print("--- STAGE 3: No tables to load. Skipping. ---")
                 return
    except duckdb.IOException as e:
        print(f"Error connecting to DuckDB file: {e}")
        print("Stage 3 will not run.")
        return

    @dlt.source
    def duckdb_full_source(db_path, single_schema, tables_to_load):

        with duckdb.connect(db_path, read_only=True) as con:
            for table in tables_to_load:
                arrow_table = con.execute(f"SELECT * FROM {single_schema}.{table}").fetch_arrow_table()
                
                if table in tables_to_merge:
                    primary_key = tables_to_merge[table]
                    yield dlt.resource(
                        arrow_table, 
                        name=table, 
                        write_disposition='merge', 
                        primary_key=primary_key
                    )
                elif table in tables_to_replace:
                    yield dlt.resource(
                        arrow_table, 
                        name=table, 
                        write_disposition='replace'
                    )

    bigquery_pipeline = dlt.pipeline(
        pipeline_name="duckdb_full_to_bigquery",
        destination=dlt.destinations.bigquery(),
        dataset_name="mongo_dbt_data"
    )

    if final_tables_to_load:
        info = bigquery_pipeline.run(duckdb_full_source(duckdb_db_path, schema_name, final_tables_to_load)
            )
        print("--- STAGE 3: Finished ---")
        print(info)
    else:
        print("--- STAGE 3: No tables found in DuckDB to load. Skipping. ---")


# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    
    dlt.config["schema.naming_convention"] = "direct"

    mongo_pipeline = dlt.pipeline(
        pipeline_name="local_mongo",
        pipelines_dir=".",
        destination='duckdb',
        dataset_name="mongo_select",
    )

    load_info = load_mongo_to_duckdb(mongo_pipeline)

    if not load_info.has_failed_jobs:
        transform_data_in_duckdb(mongo_pipeline)
        load_all_duckdb_to_bigquery(mongo_pipeline)
    else:
        print("Stage 1 (MongoDB to DuckDB) failed. Downstream stages will not run.")