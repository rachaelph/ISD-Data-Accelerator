# Core Components Reference

> Last updated: 2026-03-29

Use this file when the question is about what jobs, processing modules, or stored procedures exist and what they do.

This guide covers Databricks Jobs, processing modules (including the batch_processing.py deep-dive), and metadata warehouse stored procedures.

---

## Table of Contents

- [Databricks Jobs for Data Movement](#databricks-jobs-for-data-movement)
- [Other Utility Jobs](#other-utility-jobs)
- [Setting Up Triggers for Data Movement](#setting-up-triggers-for-data-movement)
- [Processing Modules](#processing-modules)
- [Batch Processing Module: batch_processing.py](#batch-processing-module-batch_processingpy)
- [Other Utility Modules](#other-utility-modules)
- [Metadata Warehouse Stored Procedures](#metadata-warehouse-stored-procedures)

---

# Core Components

## Databricks Jobs for Data Movement

**Keywords:** orchestration job, task orchestration, copy activity, data movement, ETL, ELT, processing method, pipeline_stage_and_batch, batch, pipeline_stage_only

The Databricks Jobs utilized for moving and transforming data between the layers of the medallion architecture. These jobs form the backbone of the ingestion and processing framework, orchestrating tasks such as data extraction, transformation, and loading (ETL/ELT) in an efficient and scalable manner.

The following list describes the function of each orchestration component:

- **Trigger Step Orchestrator**: Entry point for orchestration of all data movement. Loops through each Order of Operations step of the Trigger. Optionally runs exploratory data analysis after all steps complete (controlled by the `run_exploratory_analysis` parameter).
- **Entity Router**: Loops through all metadata table records within the order of operations step. Routes each entity by `Processing_Method`:
	- `pipeline_stage_only` → staging task (stage data only)
	- `pipeline_stage_and_batch` → staging task (stage then process via batch_processing.py)
	- `batch` → batch_processing.py **directly** (fetches its own metadata and handles logging)
	- All other methods (warehouse SP, custom function, external job) → non-framework executor
- **External Data Staging**:
	- Confirms there is no other concurrent ingestion for the metadata table record. If yes, wait until other ingestion finishes
	- Logs start
	- Grab watermark value from source system if one is used for dataset
	- Extract source table DDL (Get Source Metadata) for database sources
	- If full reload, archive existing data within Bronze files
	- Output data to Bronze volume (copy/extract). Optional source partition option if data source is a database
	- For `pipeline_stage_and_batch`: invokes `batch_processing.py` directly to write staged data to the target Delta table
	- Logs end (success or failure)
- **External Data Copy**: For each copy source type, ingest data without any source partitioning
- **External Data Watermark**: For each copy source type, get the latest watermark value for the dataset
- **External Data Metadata**: Extracts column metadata/DDL from source databases (Oracle, Azure SQL, SQL Server, PostgreSQL, MySQL, DB2). Used to get schema information about source tables for downstream DDL creation.
- **Non-Framework Method Executor** (non-batch methods only — batch bypasses this via direct module invocation)
	- Get all existing schemas for Table ID (dataset)
	- Route by processing method: warehouse stored procedure, custom function, or external job
	- Log success or failure
	- If new schema detected, log schema changes

## Other Utility Jobs
- **Exploratory Data Analysis Job**: Runs data profiling on landed data in Silver and Gold for use by data consumers

> **Alerting:** For job failure alerts, configure scheduled job failure notifications in your Databricks workspace. For content-aware alerts (e.g., data quality issues or specific error patterns), query `Data_Pipeline_Logs` or `Data_Quality_Notifications` in the metadata warehouse.

## Setting Up Triggers for Data Movement

> 📖 **Canonical reference:** See [Trigger Design and Dependency Management](Trigger_Design_and_Dependency_Management.md) for the full guide on trigger design patterns, parallelism trade-offs, Order of Operations, wrapper pipelines, and external scheduler integration.

**Quick-start steps:**

1.  **Create a new Databricks Job** that invokes the trigger step orchestrator with the `Trigger Name` parameter.
2.  **Pass the `Trigger Name`** (from Orchestration metadata) as a job parameter.
3.  **Set the schedule** (hourly, daily, etc.) on the job based on the data product's refresh needs.
4.  **Notify DevOps** about new triggers to ensure proper setup in test and prod environments.

## Execute an External Notebook or Job as a Standalone Activity

**Keywords:** standalone notebook, standalone job, execute_databricks_notebook, execute_databricks_job, Tier 3, legacy notebook, existing job, external execution

### Overview
Execute any Databricks notebook or job as part of your orchestrated data pipeline. This allows integration of existing Databricks items into the metadata-driven framework while maintaining centralized logging and error handling.

### Use Cases
- **Legacy jobs**: Integrate existing jobs without refactoring
- **External notebooks**: Run specialized notebooks (e.g., ML training, external API calls)
- **Cross-workspace execution**: Trigger items in different workspaces
- **Centralized monitoring**: Track all data activities in one logging table

### Benefits
- ✅ **Centralized logging**: Success/failure tracked in framework logging table
- ✅ **Orchestration integration**: Execute items in sequence with other job steps
- ✅ **Error handling**: Automatic retry and failure alerting
- ✅ **Cross-workspace support**: Execute items in different workspaces

### Limitations
- ❌ **No schema tracking**: Framework doesn't track schema changes from executed item
- ❌ **No data quality checks**: Framework's built-in quality checks don't apply
- ❌ **No cleansing**: Automated cleansing features not available
- ❌ **No watermarking**: Incremental logic must be implemented in the item itself

### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Run the orchestration job with the inputted `Trigger_Name` value

### Example: Execute Notebook
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES 
    ('ExecuteItem', '1', '42', 'gold', 'dbo.ml_predictions', 'execute_databricks_notebook','1');

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Reference the notebook by its Datastore_Configuration name (CI/CD safe — workspace URLs and paths live in datastore config per environment)
    ('42', 'source_details', 'datastore_name', 'ml_predictions_notebook')
```

> **Datastore Configuration:** Register the notebook alias in `databricks_batch_engine/datastores/datastore_<ENV>.json` under `external_datastores`:
> ```jsonc
> "ml_predictions_notebook": {
>   "kind": "databricks_notebook",
>   "connection_details": {
>     "workspace_url": "https://adb-xxx.azuredatabricks.net",
>     "notebook_path": "/Workspace/Shared/ml_predictions"
>   }
> }
> ```

### Cross-Workspace Execution

To execute items in a different Databricks workspace than the orchestration job, register that item under `external_datastores` in `datastore_<ENV>.json` with the correct `workspace_url` and notebook/job path. Commit and let `/fdp-04-commit` sync the entry.

```sql
-- Orchestration Table (use execute_databricks_notebook or execute_databricks_job)
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES 
    ('ExecuteItem', '1', '42', 'gold', 'dbo.ml_predictions', 'execute_databricks_notebook','1');

-- Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (42, 'source_details', 'datastore_name', 'ml_predictions_notebook')
```

> The `workspace_url`, notebook path, and any other execution details are resolved at runtime from the `Connection_Details` JSON on the `'ml_predictions_notebook'` datastore row. Each environment's `datastore_<ENV>.json` carries the correct values.

**How to find the workspace URL and notebook/job path:**
1. Navigate to the item in the target Databricks workspace
2. Copy the workspace URL (e.g., `https://adb-xxx.azuredatabricks.net`) and the notebook path or job ID from the URL
3. Add them to the `connection_details` object in your `datastore_<ENV>.json`

### Orchestration Patterns

**Sequential Execution:**
```sql
-- Execute items in order using Order_Of_Operations
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES 
    ('DataPipeline', 1, 42, 'bronze', 'raw_data', 'execute_databricks_job', 1),
    ('DataPipeline', 2, 43, 'silver', 'cleansed_data', 'execute_databricks_notebook', 1),
    ('DataPipeline', 3, 44, 'gold', 'aggregated_data', 'execute_databricks_notebook', 1)

-- Each execute_databricks_* Table_ID needs a datastore_name in Primary Config
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (42, 'source_details', 'datastore_name', 'raw_data_job'),
    (43, 'source_details', 'datastore_name', 'cleansed_data_notebook'),
    (44, 'source_details', 'datastore_name', 'aggregated_data_notebook')
```

**Mixed with Framework Ingestion:**
```sql
-- Combine execute_databricks_notebook/job with standard framework processing
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    -- Standard framework ingestion
    ('DataPipeline', 1, 10, 'bronze', 'dbo.sales', 'id', 'pipeline_stage_and_batch', 1),
    -- Execute custom notebook for ML scoring
    ('DataPipeline', 2, 42, 'silver', 'dbo.sales_scored', NULL, 'execute_databricks_notebook', 1),
    -- Standard framework transformation
    ('DataPipeline', 3, 20, 'gold', 'dbo.sales_aggregated', 'date', 'batch', 1)
```

### Monitoring and Logging

All executed items are logged through the framework's logging tables and module log surfaces with:
- **Start/End timestamps**
- **Success/Failure status**
- **Error messages** (if failed)
- **Execution duration**
- **Item name and type**

> **Note:** For batch processing entities, `batch_processing.py` handles logging internally via pyodbc connections to the logging warehouse. It writes high-level `Started` / `Processed` / `Failed` rows to `Data_Pipeline_Logs`, persists structured step-by-step messages to `Activity_Run_Logs`, and records schema changes to `Schema_Logs`. Non-batch processing methods (warehouse stored procedures, custom functions, external jobs) are still logged by the non-framework method executor. Jobs can also write to `Activity_Run_Logs` directly (e.g., logging failure messages when a downstream activity fails to start).

### Best Practices
1. **Use for integration**: Best for incorporating existing Databricks items that don't need framework features
2. **Implement error handling**: Ensure executed items have their own error handling
3. **Monitor performance**: Track execution times to identify bottlenecks
4. **Document dependencies**: Clearly document what each executed item does
5. **Consider refactoring**: For complex items, consider migrating to full framework patterns to leverage quality checks and cleansing

## Processing Modules
This section provides a detailed overview of the PySpark modules that orchestrate data movement and transformations between the layers of the medallion architecture (Bronze, Silver, and Gold). Each module is designed to implement specific tasks, ensuring seamless data flow, transformation, and enrichment across the architecture while leveraging the distributed computing capabilities of PySpark.

## Batch Processing Module: batch_processing.py

**Keywords:** batch_processing.py, batch module, core engine, module parameters, metadata warehouse endpoint, table processing, helper functions, read data, write data, schema detection, module logging, Activity_Run_Logs

### Overview
The batch_processing.py module is the core engine for batch data ingestion and transformation across all medallion layers (Bronze, Silver, Gold). It is invoked directly by the entity router for `batch` processing and by the external data staging task for `pipeline_stage_and_batch` processing. The module **fetches its own metadata** from the metadata warehouse, writes high-level run status to `Data_Pipeline_Logs`, flushes structured step-level messages to `Activity_Run_Logs`, records schema changes in `Schema_Logs`, and manages its own warehouse connections.

> **Architecture change:** In prior versions, the pipeline passed pre-built JSON configuration objects to the module and handled logging via pipeline Script activities. Now the module receives lightweight identifiers (table ID, warehouse endpoints) and fetches everything it needs directly. This makes the module self-contained and reduces orchestration complexity.

### Parameters

The following parameters are set by the calling job task (entity router for `batch`, or external data staging for `pipeline_stage_and_batch`) when invoking the module:

**Warehouse Connection Parameters** (from job configuration):
- **metadata_catalog_name**: Unity Catalog name for the metadata schema (e.g., `my_catalog`)

**Table and Trigger Context** (from job orchestration):
- **table_id**: Table ID from orchestration metadata (cast to int at runtime)
- **trigger_name**: Trigger name for orchestration lookup
- **trigger_step**: Order of operations step number
- **trigger_execution_id**: Trigger execution ID
- **trigger_execution_time**: Trigger execution time
- **start_time**: Processing start time (UTC)
- **target_datastore**: Target datastore name
- **target_entity**: Target table/entity name

**Remaining Pipeline Parameters**:
- **table_ddl**: JSON array with source database DDL for schema replication
- **event_payload**: Optional event data for event-driven scenarios
- **folder_path_from_trigger**: Optional folder path for file-triggered ingestion

**Compute and Environment Parameters**:
- **default_catalog**: Target Unity Catalog catalog
- **default_schema**: Target UC schema
- **spark_environment_id**: Optional compute cluster/policy ID

### Detailed Logic and Steps

The module is organized into 10 numbered sections. Every section's `except` block calls `log_failure_and_cleanup()`, which logs a "Failed" status to `Data_Pipeline_Logs`, flushes the structured run-log buffer to `Activity_Run_Logs`, and closes connections before re-raising the exception.

1. **Module Parameters**
   - Configure Spark compute if `spark_environment_id` is specified
   - Declare all input parameters with default values

2. **Initialize Helper Functions**
   - Import helper modules to load reusable functions into the session:
     - `import helper_functions_1`
     - `import helper_functions_2`
     - `import helper_functions_3`

3. **Connect to Warehouses, Fetch Metadata, and Log "Started"**
    - Reset the structured run-log buffer with `clear_run_log_buffer()`
   - Cast `table_id` and `trigger_step` from string to int
   - Create pyodbc connections to the metadata and logging warehouses via `create_warehouse_connection()`
   - Build a Spark Monitor URL for observability
   - Generate a UUID-based `log_id` and build a `DataMovementLogEntry`
   - Call `log_data_movement(logging_conn, log_entry, status='Started')` to record the run start
   - Call `fetch_metadata_from_warehouse(conn, table_id, target_datastore)` — replaces the pipeline's metadata Script activity
   - Call `fetch_logging_details(conn, table_id, target_datastore)` — replaces the pipeline's logging Script activity
   - Extract `orchestration_metadata`, `primary_config`, `advanced_config`, `datastore_config`, `latest_schema_details`, `watermark_value`, and `full_reload` from the fetched results
   - Close the metadata connection (logging connection stays open for later cells)

4. **Parse Configuration**
   - Parse all configuration objects from the metadata fetched in Cell 3 (no JSON parsing needed — data arrives as Python dicts from warehouse queries)
   - Calls: `parse_source_configuration()`, `parse_target_configuration()`, `parse_file_ingestion_paths()`, `parse_watermark_configuration()`, `extract_lineage_information()`, `parse_advanced_processing_configuration()`, `parse_primary_key_configuration()`, `parse_advanced_configuration_steps()`, `parse_dimension_table_configuration()`, `parse_warehouse_configuration()`, `extract_file_configuration()`
   - Populates lineage fields on the log entry for downstream logging

5. **Configure Spark Session & Initialize Tables**
   - Call `parse_performance_configuration()` and `parse_spark_configuration()`
   - Call `apply_spark_configurations()` to set timestamp rebase modes, change data feed, and Spark settings
   - Call `create_date_dimension()` to populate the date dimension table if required

6. **Full Reload Check & Data Ingestion**
   - Call `determine_first_run_and_table_existence()` to check if target table exists and whether a full reload is needed
   - Call `route_to_ingestion_method()` to ingest from the configured source (Delta tables, files, or external databases)
    - On `NoDataFoundError`: calls `log_no_data_and_cleanup()` to log "Processed" with 0 records, flush structured `Activity_Run_Logs`, then exit gracefully
   - Returns `new_data` DataFrame, `new_watermark_value`, and `source_details`

7. **Column Standardization & Schema Alignment**
   - Call `cleanse_column_names()` — standardizes column names, updates primary keys and clustering columns
   - Call `add_timestamp_metadata_columns()` — adds `delta__created_datetime`, `delta__modified_datetime`
   - Call `create_schema_if_not_exists()` — ensures target schema path exists
   - Call `build_source_column_definitions()` — builds column definitions from DDL
   - Call `apply_source_type_casts()` — aligns source data types to target schema

8. **Data Cleansing, Transformations & Data Quality**
   - Call `data_cleansing()` — blank-to-null conversion, whitespace trimming
   - Call `transformation_functions()` — applies all configured transformations (column rename, derived columns, filters, deduplication, surrogate keys, pivoting, entity resolution, custom functions, etc.)
   - Call `execute_data_quality_checks()` — runs validation rules, returns warnings, failures, and quarantined data. Raises `DataQualityFailureError` (logged as "Failed") if any force-failure rules trigger
   - Call schema change detection: `get_md5_of_string()`, `get_schema_changes()`
   - Call `finalize_processing()` — prepares final DataFrame for write

9. **Write Data & Post-Processing**
   - Call `add_scd2_columns_for_dimensions()` for SCD2 dimension support
   - Call `build_statistics_columns_config()` for query optimization
   - Call `create_delta_table()` — creates the target Delta table with liquid clustering
   - Call `write_data_orchestrator()` — writes data using the configured merge strategy (append, overwrite, merge)
   - Call `write_quarantined_data()` — writes rejected rows to quarantine table
   - Call `post_processing_scd2_update()` — updates SCD2 records
   - Call `drop_external_table_for_shortcut()` — cleans up external table references
   - Call `execute_vacuum_if_needed()` — vacuums old Delta files
   - Call `cleanup_temporary_files()` — removes staging files

10. **Success Logging & Cleanup**
    - Populate the log entry with `source_details`, `event_end_time`, `watermark_value`, `records_processed`, `quarantined_records`, and `data_quality_warnings`
    - Call `log_data_movement(logging_conn, log_entry, status='Processed')` to record the successful run
    - Call `persist_run_log_entries(logging_conn, log_entry.log_id, log_entry.table_id)` to flush structured step-level messages to `Activity_Run_Logs`
    - If a schema change was detected, call `log_new_schema()` to record it in `Schema_Logs`
    - Close the logging warehouse connection

### Function to Helper Module Mapping

This section provides a detailed description of the PySpark helper functions that are used across modules to streamline data movement and transformation tasks within the medallion architecture (Bronze, Silver, and Gold layers). These reusable functions encapsulate common logic, promoting consistency, reducing code duplication, and enhancing the maintainability of the data pipeline framework.

> **Tip:** Run `python scripts/compare_helper_functions.py` to automatically verify that this table stays in sync with the helper module implementations.

**Total Functions:** 275 functions across 3 helper modules
- **helper_functions_1.py:** 169 functions - Surrogate keys, cleansing, transformations, merges, SCD2, ingestion, data quality, schema validation, graceful exit handling
- **helper_functions_2.py:** 39 functions - DDL generation, file ingestion, type mapping, Excel handling, batch readers
- **helper_functions_3.py:** 67 functions - Configuration parsing, entity resolution, Spark optimization, utilities, module loading, volume mounting

#### Core Processing & Orchestration

These functions manage the overall execution flow, including initialization, finalization, and dynamic function loading.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `load_workspace_module_into_globals` | helper_functions_3.py | **Custom Module Loader:** Dynamically loads functions from **custom** modules (e.g., `custom_*`) into the current Spark session when metadata references them via `files_to_run`. Uses a **dual-path strategy**: (1) **Production path** — loads from `.py` files in UC volumes `custom_functions/` deployed via CI/CD, (2) **Dev fallback** — uses workspace API when `.py` files don't exist yet. Includes retry logic with exponential backoff. **Note:** Core helper functions (`helper_functions_1/2/3.py`) are loaded via Python imports, not this function. |
| `_try_load_from_catalog_file` | helper_functions_3.py | Attempts to load custom functions from a `.py` file in the UC volumes `custom_functions/` folder (production path). Reads the file and executes via IPython `run_cell()` so functions share the session namespace with helper functions. Returns `True` if loaded, `False` if file doesn't exist. |
| `_load_via_notebook_api` | helper_functions_3.py | Loads custom functions using the workspace API (development fallback). Used when `.py` files haven't been deployed via CI/CD. Retrieves module content and executes code cells via IPython. |
| `_get_notebook_code_cells` | helper_functions_3.py | Retrieves a module definition, parses it, and returns only the code cells. |
| `_execute_code_cells` | helper_functions_3.py | Executes a list of code cells using IPython's `run_cell()` method, handling both list and string source formats. Now propagates execution errors by checking `result.error_in_exec`. |
| `finalize_processing` | helper_functions_1.py | Finalizes the data processing pipeline, handles failures, and adds schema tracking information before completion. |
| `exit_gracefully_no_data` | helper_functions_1.py | Handles graceful exit when no data is found. On first run (first_run=True), raises an exception indicating a configuration problem. On subsequent runs, exits with status "Processed" and zero records. |
| `log_and_print` | helper_functions_1.py | Provides standardized logging that integrates with the platform's diagnostic system while displaying messages in the console. Supports multiple severity levels (info, warn, error) and enables centralized log collection in Azure Log Analytics for monitoring and alerting. |
| `convert_datetime_to_string` | helper_functions_1.py | Converts datetime values to ISO format strings for watermark tracking and logging. |
| `route_to_ingestion_method` | helper_functions_1.py | Routes to the appropriate ingestion method based on source configuration (Delta table vs raw files). |
| `write_data_orchestrator` | helper_functions_1.py | Orchestrates the complete data writing process including batching logic and merge strategy selection. |

#### Data Ingestion

This group includes functions for ingesting data from various sources, including Delta tables and raw files, with support for incremental loading.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `build_exit_context` | helper_functions_2.py | Builds a standardized exit context dictionary for file ingestion functions, ensuring consistent structure for graceful exit handling across all call sites. |
| `ingest_from_delta_table` | helper_functions_1.py | **Orchestrator:** Ingests data from Delta tables, supporting incremental loads via watermarking. |
| `_extract_new_watermark_value` | helper_functions_1.py | Extracts the latest watermark value from a table to determine the starting point for the next incremental load. |
| `_construct_watermark_filter` | helper_functions_1.py | Builds the SQL filter condition for watermark-based incremental processing. |
| `_extract_and_validate_watermark` | helper_functions_1.py | Retrieves and validates the new watermark value from the source table. |
| `_ingest_with_custom_transformation_function` | helper_functions_1.py | Handles data ingestion using a user-defined custom function for specialized logic. |
| `_ingest_from_table` | helper_functions_1.py | Manages the core logic for ingesting data from Delta tables with watermark filtering. |
| `_ingest_from_table_with_watermark` | helper_functions_1.py | Wraps `_ingest_from_table` with standard watermark extraction logic for traditional incremental loads. |
| `_ingest_from_table_with_cdf` | helper_functions_1.py | **Orchestrator:** Handles ingestion from Delta tables using Change Data Feed with version-based watermarking. Routes to appropriate CDF function based on `first_run` flag: calls `_read_full_table_with_cdf_columns` for initial loads or `_read_cdf_incremental` for incremental loads. |
| `_read_full_table_with_cdf_columns` | helper_functions_1.py | Handles **initial/first run** CDF ingestion by reading the full Delta table and adding CDF metadata columns (_commit_version, _commit_timestamp, _change_type) for downstream compatibility. Sets watermark to current table version + 1. Used when `first_run=True`. |
| `_read_cdf_incremental` | helper_functions_1.py | Handles **incremental** CDF ingestion by reading changes from Delta table using Change Data Feed with `startingVersion` parameter. Filters to relevant change types (insert, update_postimage, delete) - automatically excludes update_preimage. Includes version existence checking to gracefully handle cases where requested version doesn't exist yet. Advances watermark to current table version + 1 when data exists, or preserves watermark when no data after filtering. Used when `first_run=False`. |
| `ingest_raw_files` | helper_functions_2.py | **Orchestrator:** Manages the ingestion of raw files from the Bronze layer, supporting various paths and formats. |
| `ingest_raw_files_with_base_path` | helper_functions_2.py | Ingests files from a time-partitioned base path, ideal for structured, date-based folders. |
| `ingest_raw_files_with_wildcard_path` | helper_functions_2.py | Ingests files using a wildcard path for flexible discovery and incremental processing. |
| `read_all_files_in_paths` | helper_functions_2.py | Reads all files from a list of paths, handling different formats and custom logic. |
| `_read_files_in_batch` | helper_functions_2.py | Processes file paths in parallelized batches to accelerate ingestion when large numbers of files exist. |
| `_read_csv_batch` | helper_functions_2.py | Reads multiple CSV/TSV/TXT files in a single Spark action for efficient batch processing. |
| `_read_json_batch` | helper_functions_2.py | Reads multiple JSON files in a single Spark action for efficient batch processing. |
| `_read_parquet_batch` | helper_functions_2.py | Reads multiple Parquet files in a single Spark action for efficient batch processing. |
| `_extract_schema_and_options` | helper_functions_2.py | Extracts schema from kwargs dict for file readers, separating schema from other options. Logs a warning about silent data loss when schema enforcement is used. |
| `_read_files_sequentially` | helper_functions_2.py | Fallback sequential reader that trades speed for lower memory usage or deterministic ordering. |
| `_should_use_batch_mode` | helper_functions_2.py | Decides whether ingestion should use batched or sequential mode based on file counts and configuration. |
| `_resolve_file_extension` | helper_functions_2.py | Normalizes file extensions and maps them to the correct downstream reader. |
| `get_files_with_modified_date` | helper_functions_2.py | Retrieves file paths and their last modification timestamps for incremental file processing. |
| `_handle_custom_ingestion` | helper_functions_2.py | Manages ingestion using custom functions for proprietary or complex file formats. |
| `_process_file_by_extension` | helper_functions_2.py | Routes to the appropriate file reader based on the file extension. |
| `_handle_csv_tsv_txt` | helper_functions_2.py | Handles the reading of CSV, TSV, and TXT files. |
| `_handle_parquet` | helper_functions_2.py | Handles the reading of Parquet files. |
| `_handle_json` | helper_functions_2.py | Handles the reading of JSON files. |
| `_handle_excel` | helper_functions_2.py | Handles the reading of Excel files. |
| `_load_excel_sheets_to_dataframe` | helper_functions_2.py | Reads one, many, or all Excel sheets into a single pandas DataFrame with sheet metadata. |
| `_validate_excel_sheet_config` | helper_functions_2.py | Validates the `sheet_name` configuration, enforcing single, comma-separated, or wildcard sheet requests. |
| `_build_excel_pandas_kwargs` | helper_functions_2.py | Builds the pandas keyword arguments (dtype, header) used for Excel ingestion to keep behavior consistent. |
| `_parse_requested_sheets` | helper_functions_2.py | Splits and trims comma-separated sheet names while validating that at least one sheet is requested. |
| `_read_requested_excel_sheets` | helper_functions_2.py | Reads all requested sheets (or all sheets via `*`) into pandas DataFrames and returns them keyed by sheet name. |
| `_combine_excel_sheet_frames` | helper_functions_2.py | Appends the `sheet_name` metadata column and concatenates multiple sheet DataFrames into a single pandas DataFrame. |
| `_handle_xml` | helper_functions_2.py | Handles the reading of XML files. |
| `_stage_dataframe_as_csv` | helper_functions_2.py | Stages a DataFrame as CSV for intermediate processing. |
| `extract_file_configuration` | helper_functions_3.py | Extracts and processes file-specific configuration parameters from metadata. |
| `_resolve_file_header_settings` | helper_functions_3.py | Resolves file header configuration settings from metadata. |
| `_resolve_encoding` | helper_functions_3.py | Resolves file encoding configuration from metadata. |
| `_resolve_xml_settings` | helper_functions_3.py | Resolves XML-specific configuration settings. |
| `_resolve_on_bad_records_mode` | helper_functions_3.py | Determines how to handle malformed records during ingestion. |
| `_build_schema_kwargs` | helper_functions_3.py | Builds schema-related keyword arguments for file reading operations. |

#### Volume Mounting Utilities

Functions providing local file system access to volume files for custom ingestion functions that need libraries requiring local paths (e.g., `zipfile`, `PIL`, `PyPDF2`).

| Function | Helper Module | Purpose |
|----------|----------------|--------|
| `_mount_catalog_for_local_access` | helper_functions_3.py | Mounts a volume to a unique `/mount_table_{table_id}` mount point to enable local file system access for custom file ingestion. Extracts the volume root from paths and includes duplicate mount detection. |
| `_mount_abfss_path_for_local_access` | helper_functions_3.py | Convenience wrapper for write-oriented scenarios. Mounts the volume for a single path and returns the resolved local path along with mount details. |
| `_unmount_catalog` | helper_functions_3.py | Unmounts a volume mount point to clean up resources after file processing. Safe to call even if mount point doesn't exist. |
| `_convert_abfss_paths_to_local` | helper_functions_3.py | Converts a list of ABFSS paths to local mounted file system paths for use with standard Python file I/O after mounting. |

#### DDL & Table Management

Functions for creating and managing Delta table schemas, including DDL generation from various source databases.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `build_source_column_definitions` | helper_functions_2.py | **Orchestrator:** Converts source database DDL metadata into source-aligned column definitions by routing to the correct generator based on source type. Uses the `DDL_GENERATORS` registry dictionary — if you add a new database source type, you must register a corresponding `create_<source>_table_ddl` function in this dictionary (see [Adding Custom Ingestion Logic](Extending_the_Accelerator.md#adding-custom-ingestion-logic)). |
| `apply_source_type_casts` | helper_functions_2.py | Applies source-aligned type casts to ingested DataFrame columns before writing; physical table creation is handled by `create_delta_table`. |
| `create_delta_table` | helper_functions_1.py | Dynamically creates a Delta table based on an input DataFrame's schema with optimal configuration including liquid clustering. |
| `create_oracle_table_ddl` | helper_functions_2.py | Converts Oracle DDL to Spark Delta table DDL with Oracle-specific type mappings (NUMBER, VARCHAR2, CLOB, etc.). |
| `create_db2_table_ddl` | helper_functions_2.py | Converts IBM DB2 DDL to Spark Delta table DDL with DB2-specific type mappings (INTEGER, SMALLINT, CHAR, etc.). |
| `create_sql_server_table_ddl` | helper_functions_2.py | Converts Microsoft SQL Server DDL to Spark Delta table DDL with SQL Server-specific mappings (NVARCHAR, DATETIME2, etc.). |
| `create_postgre_sql_table_ddl` | helper_functions_2.py | Converts PostgreSQL DDL to Spark Delta table DDL with PostgreSQL-specific mappings (SERIAL, TEXT, BYTEA, etc.). |
| `create_my_sql_table_ddl` | helper_functions_2.py | Converts MySQL DDL to Spark Delta table DDL with MySQL-specific mappings (TINYINT, MEDIUMTEXT, ENUM, etc.). |
| `_updated_column_definition` | helper_functions_2.py | Updates column definitions based on nullable constraints and configuration settings. |
| `_apply_common_type_casts` | helper_functions_2.py | Applies common type casting logic that's shared across different database sources (DATE, TIMESTAMP, BOOLEAN conversions). |
| `create_schema_if_not_exists` | helper_functions_3.py | Creates database schema in target catalog/schema if it doesn't already exist. |
| `check_target_table_exists` | helper_functions_3.py | Checks if target Delta table exists in the specified catalog and schema. |
| `determine_first_run_and_table_existence` | helper_functions_3.py | Determines if this is first run and validates table existence for merge operations. |
| `drop_table_for_full_reload` | helper_functions_3.py | Drops target table when full reload is requested via parameter. |
| `drop_external_table_for_shortcut` | helper_functions_3.py | Drops external tables created for external references to prevent endpoint conflicts. |

#### Data Cleansing

This group focuses on standardizing and cleaning data to ensure consistency and quality.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `data_cleansing` | helper_functions_1.py | **Orchestrator:** Applies configured string-focused cleansing operations including trim whitespace and blank string to null conversion. Use `change_data_types` when an explicit type cast is required. |
| `cleanse_column_names` | helper_functions_1.py | Applies column name standardization to a DataFrame and its associated metadata including transformations, data quality rules, and primary keys. |
| `column_name_cleansing` | helper_functions_1.py | Standardizes column names according to enterprise naming conventions (title case, removing special characters, handling reserved keywords). |
| `_apply_column_name_cleansing_to_list` | helper_functions_1.py | Applies column name cleansing to lists of column names in metadata configurations. |
| `_trim_column_names` | helper_functions_1.py | Removes leading/trailing whitespace from column names before other normalization steps. |
| `_collapse_underscores` | helper_functions_1.py | Replaces repeated underscores with a single underscore to keep column names tidy. |
| `_apply_case_conversion` | helper_functions_1.py | Converts column names to the configured casing (`lower`, `upper`, or `title`). |
| `_replace_non_alphanumeric` | helper_functions_1.py | Swaps illegal characters in column names with underscores. |
| `add_timestamp_metadata_columns` | helper_functions_1.py | Adds system-generated timestamp columns (`delta__created_datetime`, `delta__modified_datetime`) for lineage and auditing. |
| `_trim_string_data` | helper_functions_1.py | Trims whitespace from string column values according to configuration. |
| `_replace_blank_with_null` | helper_functions_1.py | Converts empty strings to `NULL` for downstream consistency. |
| `_exact_string_replacement` | helper_functions_1.py | Performs deterministic string substitutions that power the `exact_replace` column cleansing option. |
| `_regex_replacement` | helper_functions_1.py | Applies regex-based replacements when column cleansing specifies `regex_find` / `regex_replace`. |

#### Data Quality

Functions for executing a wide range of data quality checks to validate data integrity.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `execute_data_quality_checks` | helper_functions_1.py | **Orchestrator:** Executes a series of data quality checks and captures the results. |
| `_data_quality_checks` | helper_functions_1.py | **Orchestrator:** Adds tracking index, runs corrupt-record and PK checks, evaluates all configured checks in one parallel pass, then applies deterministic quarantine precedence. |
| `_DQ_DISPATCH` | helper_functions_1.py | Module-level dict mapping DQ category name → validation function. |
| `_build_dq_extra_args` | helper_functions_1.py | Builds per-category extra keyword args (e.g. `datastore_config` for referential integrity, `target_table_name` for anomaly). |
| `_dispatch_dq_check` | helper_functions_1.py | Routes a single DQ step to its validation function via `_DQ_DISPATCH`, normalising the return tuple. |
| `_run_all_dq_checks_parallel` | helper_functions_1.py | Evaluates all configured DQ checks against one immutable snapshot and returns warnings, failures, quarantine candidates, and row-level markers. |
| `_apply_deterministic_quarantine_candidates` | helper_functions_1.py | Applies first-match precedence per `delta__idx`, appends quarantined rows, and removes them in one pass. |
| `_record_dq_result` | helper_functions_1.py | Centralized helper for recording data quality check results in a consistent JSON format. |
| `_get_dq_category_display_name` | helper_functions_1.py | Gets user-friendly display name for a data quality category for reporting purposes. |
| `_quarantine_rows` | helper_functions_1.py | Centralized helper for quarantining violating rows from the main DataFrame. |
| `_handle_corrupt_records` | helper_functions_1.py | Identifies and handles records that do not conform to the expected schema. |
| `_validate_primary_keys` | helper_functions_1.py | Validates the uniqueness of primary keys in the dataset. |
| `_validate_condition` | helper_functions_1.py | Validates data using custom SQL-based condition logic defined in metadata. |
| `_validate_batch_size` | helper_functions_1.py | Validates if the batch meets a minimum record count threshold. |
| `_validate_referential_integrity` | helper_functions_1.py | Validates referential integrity by looking up values in a reference table. |
| `_validate_not_null` | helper_functions_1.py | Validates that specified columns do not contain null values. |
| `_validate_unique` | helper_functions_1.py | Validates that values in specified columns are unique across the dataset. |
| `_validate_range` | helper_functions_1.py | Validates that numeric or date values fall within specified minimum and maximum bounds. |
| `_validate_freshness` | helper_functions_1.py | Validates data freshness by checking that the newest record timestamp is within the configured age threshold. |
| `_validate_completeness` | helper_functions_1.py | Validates that non-null percentage of specified columns meets a minimum completeness threshold. |
| `_validate_anomaly` | helper_functions_1.py | **Orchestrator:** Detects statistical anomalies in numeric columns using IQR or Z-score methods. |
| `_validate_anomaly_single_column` | helper_functions_1.py | Applies anomaly detection logic to a single column, calculating baseline statistics and identifying outliers. |
| `_load_anomaly_baseline_data` | helper_functions_1.py | Loads baseline data and validates it has sufficient records for anomaly detection. |
| `_calculate_zscore_statistics` | helper_functions_1.py | Calculates mean and standard deviation for z-score anomaly detection. |
| `_calculate_iqr_statistics` | helper_functions_1.py | Calculates IQR bounds (Q1, Q3, lower/upper bounds) for anomaly detection. |
| `_find_zscore_anomalies` | helper_functions_1.py | Finds anomalies using z-score method based on standard deviation threshold. |
| `_find_iqr_anomalies` | helper_functions_1.py | Finds anomalies using IQR method based on interquartile range bounds. |
| `_handle_anomaly_results` | helper_functions_1.py | Handles anomaly detection results including quarantine logic and result recording. |
| `_validate_pattern` | helper_functions_1.py | **Orchestrator:** Validates data against common patterns using predefined regex templates. Supports 13 pattern types with configurable null handling. |
| `_validate_pattern_single_column` | helper_functions_1.py | Applies pattern validation to a single column using regex matching. |
| `_get_pattern_validation_regex` | helper_functions_1.py | Returns the appropriate regex pattern for a given pattern type (us_phone, uk_phone, email, us_zip, uk_postcode, us_ssn, date formats, url, credit_card, ip_address, alpha_only, alphanumeric_only, numeric_only, guid). |

#### Schema Management

Functions for detecting and managing schema changes over time.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `analyze_schema_changes` | helper_functions_1.py | **Orchestrator:** Extracts and analyzes schema information to detect changes. |
| `handle_schema_change_failures` | helper_functions_1.py | Manages failures or warnings based on schema change detection policies. |
| `get_schema_changes` | helper_functions_3.py | Compares two schemas and identifies all differences (new, dropped, changed columns). |
| `get_md5_of_string` | helper_functions_3.py | Generates an MD5 hash of a schema string for efficient change detection. |
| `update_spark_column_descriptions` | helper_functions_3.py | Updates Spark table column descriptions, often to indicate primary key status. |
| `validate_table_schema_contract` | helper_functions_1.py | Validates that a DataFrame's schema matches the expected schema contract for Delta table sources. |
| `_parse_expected_schema` | helper_functions_1.py | Parses an expected schema string into a dictionary of column names and types. |
| `_normalize_spark_type` | helper_functions_1.py | Normalizes Spark data type names to standard lowercase forms for comparison. |

#### Data Transformation

This group contains functions for applying metadata-driven transformations to shape the data.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `transformation_functions` | helper_functions_1.py | **Orchestrator:** Applies a sequence of metadata-driven transformations to a DataFrame. |
| `_rename_columns` | helper_functions_1.py | Renames columns for schema alignment or business naming conventions. |
| `_create_derived_column` | helper_functions_1.py | Creates new columns using SQL expressions for calculated fields. |
| `_remove_columns` | helper_functions_1.py | Removes columns that are not needed in the target schema. |
| `_select_columns` | helper_functions_1.py | Selects a specific set of columns to be kept in the target schema. |
| `_filter_data` | helper_functions_1.py | Applies row-level filters to subset data based on business rules. |
| `_drop_duplicates` | helper_functions_1.py | **Orchestrator:** Applies drop_duplicates transformations. Routes to exact match or ordered deduplication based on presence of `order_by` attribute. |
| `_parse_drop_duplicates_columns` | helper_functions_1.py | Parses and validates the column_name parameter for drop_duplicates, returning column list or None for all columns. |
| `_parse_order_specifications` | helper_functions_1.py | Parses and validates order_by and order_direction parameters for ordered deduplication. |
| `_drop_exact_duplicates` | helper_functions_1.py | Drops exact duplicate rows using PySpark's native `dropDuplicates()` on specified columns. |
| `_drop_duplicates_with_ordering` | helper_functions_1.py | Drops duplicates keeping one row per partition using window function with `row_number()` based on ordering. |
| `_change_data_types` | helper_functions_1.py | Converts column data types to specified PySpark data types. |
| `_apply_null_handling` | helper_functions_1.py | Handles null values with drop, replace, or convert operations. |
| `_drop_null_rows` | helper_functions_1.py | Drops rows that contain null values in specified columns. |
| `_replace_nulls_with_default` | helper_functions_1.py | Replaces null values with specified default values. |
| `_convert_values_to_null` | helper_functions_1.py | Converts specific values to null based on configuration. |
| `_replace_values` | helper_functions_1.py | Replaces multiple different values across columns with a specific value. |
| `_mask_sensitive_data` | helper_functions_1.py | Masks sensitive data for PII protection using character replacement. |
| `_add_row_hash` | helper_functions_1.py | Adds hash column for change detection and data lineage tracking. |
| `_pivot_data` | helper_functions_1.py | Pivots data to convert rows to columns using group and pivot operations. |
| `_prepare_pivot_configuration` | helper_functions_1.py | Validates the pivot metadata (group, pivot, value columns, aggregation list, output naming pattern) and normalizes column lists before execution. |
| `_build_pivot_aggregation_expressions` | helper_functions_1.py | Maps requested aggregation names (sum, avg, min, max, count, first, last) to the corresponding Spark functions for multi-metric pivots. |
| `_unpivot_data` | helper_functions_1.py | Unpivots data to convert columns to rows (melt operation). |
| `_execute_custom_transformation_function` | helper_functions_1.py | Executes user-defined custom transformation functions for specialized logic. |
| `_validate_transformation_results` | helper_functions_1.py | Validates that transformation steps have not unintentionally removed all data. |
| `_validate_columns_exist` | helper_functions_1.py | Validates that required columns exist in the DataFrame before transformation. |
| `_create_surrogate_key` | helper_functions_1.py | Creates surrogate keys for dimension tables during transformation. |
| `_attach_dimension_surrogate_key` | helper_functions_1.py | Inserts foreign surrogate keys into fact tables. |
| `_join_data` | helper_functions_1.py | Performs join operations between DataFrames. |
| `_add_window_function` | helper_functions_1.py | **Orchestrator:** Applies add_window_function transformations using the helper stack below. |
| `_parse_window_function_config` | helper_functions_1.py | Parses metadata for window functions, normalizes comma-separated lists, and prepares a config dictionary. |
| `_validate_window_function_config` | helper_functions_1.py | Confirms window function inputs are valid, required columns exist, and order directions align with order_by columns. |
| `_log_window_function_configuration` | helper_functions_1.py | Logs partitioning, ordering, and source column details to aid troubleshooting. |
| `_build_window_spec` | helper_functions_1.py | Builds the PySpark `WindowSpec` based on partition and order settings. |
| `_apply_window_function` | helper_functions_1.py | Executes the requested window/ranking/aggregation function against the prepared window spec. |
| `_aggregate_data` | helper_functions_1.py | **Orchestrator:** Applies aggregate_data transformations using the helper stack below. |
| `_parse_aggregate_config` | helper_functions_1.py | Parses aggregate metadata, normalizes comma-separated lists, and builds a reusable config dictionary. |
| `_validate_aggregate_config` | helper_functions_1.py | Ensures aggregate configuration lengths match, functions are allowed, and required columns exist before execution. |
| `_log_aggregate_config` | helper_functions_1.py | Logs aggregate settings (group-by columns, function pairs, output names) for observability. |
| `_build_grouped_dataframe` | helper_functions_1.py | Creates the grouped DataFrame scope or aggregates across the full dataset when no group columns are provided. |
| `_build_aggregate_expressions` | helper_functions_1.py | Maps each aggregation request to the appropriate Spark function and output alias. |
| `_transform_datetime` | helper_functions_1.py | **Orchestrator:** Applies transform_datetime operations using the helper stack below. |
| `_parse_transform_datetime_config` | helper_functions_1.py | Parses and normalizes datetime transformation metadata, ensuring operation and output column requirements are satisfied. |
| `_validate_transform_datetime_config` | helper_functions_1.py | Validates datetime transformation inputs, enforces column requirements, and derives helper values (days, months, formats). |
| `_log_transform_datetime_config` | helper_functions_1.py | Logs the datetime transformation details (operation, columns) for observability and troubleshooting. |
| `_apply_transform_datetime_operation` | helper_functions_1.py | Executes the requested datetime operation using keyword arguments to keep future extensions simple. |
| `_union_data` | helper_functions_1.py | Unions multiple DataFrames together. |
| `_sort_data` | helper_functions_1.py | Sorts DataFrame by one or more columns with configurable sort directions (asc/desc). |
| `_explode_array` | helper_functions_1.py | Explodes array column into multiple rows, duplicating other column values for each array element. |
| `_flatten_struct` | helper_functions_1.py | Flattens nested struct columns into individual columns with configurable prefix and field selection. |
| `_split_column` | helper_functions_1.py | Splits a string column into multiple columns based on a delimiter. |
| `_concat_columns` | helper_functions_1.py | Concatenates multiple columns into a single column with configurable separator and null handling. |
| `_conditional_column` | helper_functions_1.py | Creates a column with conditional logic (CASE WHEN equivalent) using conditions and values from metadata. |
| `_string_functions` | helper_functions_1.py | Applies string transformation functions (upper, lower, trim, ltrim, rtrim, reverse, length, initcap, soundex) to columns. |
| `_normalize_text` | helper_functions_1.py | Normalizes text data for consistency and matching with operations like lowercase, trim, collapse_whitespace, remove_punctuation, etc. |
| `reorder_columns_for_output` | helper_functions_1.py | Reorders columns for output, typically moving metadata columns to the end. |

#### Surrogate Key & Dimension Management

Functions for creating and managing surrogate keys, foreign keys, and Slowly Changing Dimensions (SCD).

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `create_date_dimension` | helper_functions_3.py | Creates a comprehensive date dimension table for analytical workloads. |
| `surr_keys_dim` | helper_functions_1.py | **Orchestrator:** Generates and assigns surrogate keys for dimension table records. |
| `_sk_prepare_existing_data` | helper_functions_1.py | Loads existing dimension data from the target table to prepare for key assignment. |
| `_calculate_base_key` | helper_functions_1.py | Computes the maximum existing key to ensure new keys are sequential. |
| `_prepare_prior_keys` | helper_functions_1.py | Prepares existing key mappings, handling SCD2 active records. |
| `_validate_input_duplicates` | helper_functions_1.py | Guards against duplicate business keys in the incoming data. |
| `_join_with_existing_keys` | helper_functions_1.py | Performs a null-safe join to map existing surrogate keys to incoming data. |
| `_assign_new_keys` | helper_functions_1.py | Assigns new, sequential surrogate keys to rows that do not have one. |
| `_reorder_columns_with_sk_first` | helper_functions_1.py | Moves the surrogate key column to the front of the DataFrame. |
| `_attach_dimension_surrogate_key` | helper_functions_1.py | **Core Logic:** Attaches a foreign surrogate key to a fact table DataFrame, linking it to a dimension. |
| `_extract_surrogate_key_configuration` | helper_functions_1.py | Extracts and processes the configuration for surrogate key insertion. |
| `_setup_temp_views` | helper_functions_1.py | Registers temporary views to be used in SQL join operations. |
| `_extract_dimension_columns` | helper_functions_1.py | Extracts dimension column names from the join logic. |
| `_prepare_additional_columns` | helper_functions_1.py | Prepares additional dimension columns to be added to the fact table. |
| `_validate_join_uniqueness` | helper_functions_1.py | Validates the uniqueness of join keys in the dimension table to prevent ambiguity. |
| `_execute_surrogate_key_join` | helper_functions_1.py | Executes the final join to attach the surrogate keys to the fact data. |
| `insert_null_row` | helper_functions_1.py | Creates and appends a standardized "unknown" member row (with SK -1) to dimension tables. |
| `scd_dimensions` | helper_functions_1.py | **Orchestrator:** Implements the full SCD Type 2 logic, tracking historical changes in dimensions. |
| `_scd2_prepare_existing_data` | helper_functions_1.py | Loads existing active dimension records to prepare for change detection. |
| `_prepare_comparison_columns` | helper_functions_1.py | Identifies the columns to be used for comparing records to detect changes. |
| `_calculate_hash_comparison` | helper_functions_1.py | Calculates hash values for efficient change detection between old and new records. |
| `_get_changed_dimensions_values` | helper_functions_1.py | Retrieves the full data for records that have changed. |
| `_get_new_dimensions_values` | helper_functions_1.py | Retrieves the full data for new records. |
| `_process_deleted_records` | helper_functions_1.py | Handles the logic for processing records that have been deleted at the source. |
| `_process_modified_records` | helper_functions_1.py | Processes records that have been modified but not deleted. |
| `_create_new_versions` | helper_functions_1.py | Inserts new versions for records that have been modified. |
| `_prepare_new_records` | helper_functions_1.py | Inserts completely new records into the dimension. |
| `_execute_final_merge` | helper_functions_1.py | Combines all updates (new, modified, deleted) and executes the final merge into the target table. |
| `add_scd2_columns_for_dimensions` | helper_functions_3.py | Adds the necessary tracking columns (`scd_start_date`, `scd_end_date`, `scd_active`) for SCD2. |
| `_add_scd2_columns_with_deletion_tracking` | helper_functions_3.py | Internal helper that stamps SCD2 columns while honoring soft-delete semantics. |
| `_add_scd2_columns_without_deletion_tracking` | helper_functions_3.py | Adds SCD2 columns for dimensions that do not track deletions. |
| `post_processing_scd2_update` | helper_functions_3.py | Adjusts start dates for single-version dimension records to ensure they are available for all historical facts. Includes guard logic: only runs when output is Delta table, merge_type is 'scd2', and records were processed. |

#### Data Writing & Merging

Functions responsible for writing data to its destination, supporting various merge strategies.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `merge_data` | helper_functions_1.py | **Orchestrator:** Executes data merge operations using one of multiple strategies (merge, append, overwrite, merge_and_delete, merge_mark_unmatched_deleted, merge_mark_all_deleted, replace_where, warehouse_spark_connector, scd2, output_file). |
| `get_merge_condition` | helper_functions_1.py | Generates a null-safe merge condition string (<=> operator) for DataFrame merge operations based on primary keys. |
| `_prepare_merge_config` | helper_functions_1.py | Prepares merge condition and update columns for Delta table merge operations, excluding system columns. |
| `_apply_datetime_substitutions` | helper_functions_1.py | Applies datetime placeholders (%Y, %m, %d, %H, %M, %S, etc.) to file paths with local datetime import to avoid conflicts. |
| `_write_spark_file_with_rename` | helper_functions_1.py | Writes DataFrame using Spark coalesce(1) with configurable delimiter, then renames part-xxxx file to user-specified filename via dbutils.fs. |
| `_write_excel_file` | helper_functions_1.py | Writes DataFrame to Excel file using pandas with row count validation (~1M limit). |
| `_write_output_file` | helper_functions_1.py | **Orchestrator:** Routes file output to format-specific handlers (CSV, TXT, Parquet, JSON, JSONL, Excel) with datetime substitution. |
| `_write_warehouse_spark_connector` | helper_functions_1.py | Writes data to a Databricks SQL warehouse table using the Spark connector with proper authentication. |
| `_handle_first_run` | helper_functions_1.py | Handles the initial write operation for a new table, creating the Delta table structure. |
| `_handle_append` | helper_functions_1.py | Handles a simple append operation, adding all new data to the table without deduplication. |
| `_handle_overwrite` | helper_functions_1.py | Handles a full table replacement, including schema evolution with unionByName. |
| `_handle_merge` | helper_functions_1.py | Handles a standard upsert (update/insert) operation based on primary keys using Delta merge API. |
| `_handle_merge_and_delete` | helper_functions_1.py | Handles a merge that also physically deletes records marked for deletion in the source using whenNotMatchedBySource. |
| `_handle_merge_mark_unmatched_deleted` | helper_functions_1.py | Handles a merge that soft-deletes records in the target that are no longer in the source by setting a deletion flag. |
| `_handle_merge_mark_all_deleted` | helper_functions_1.py | Handles a merge that soft-deletes all records that are matched in the source for specific business scenarios. |
| `_handle_replace_where` | helper_functions_1.py | Handles a selective partition overwrite based on a column's values using Delta replaceWhere operation. |
| `_get_records_processed` | helper_functions_1.py | Retrieves the count of records processed in the last write operation from Delta table history. |
| `_build_filter_expression_for_batch` | helper_functions_1.py | Builds filter expressions for batch processing operations based on batch metadata columns. |
| `get_unique_batch_metadata` | helper_functions_1.py | Retrieves unique batch metadata values for processing large datasets in smaller chunks. |
| `process_single_batch` | helper_functions_1.py | Processes a single batch of data with filtering, transformation, and merge operations. |
| `get_records_written_for_table` | helper_functions_1.py | Gets the number of records written from the Delta table's history using operationMetrics. |
| `write_quarantined_data` | helper_functions_1.py | Writes data quality violations to a separate quarantine table for investigation and remediation. |
| `build_statistics_columns_config` | helper_functions_1.py | Builds the optional Delta statistics table properties for query optimization. Returns `delta.dataSkippingStatsColumns` when explicit columns are configured, returns `delta.dataSkippingNumIndexedCols` when the first-N option is configured, and otherwise leaves both properties unmanaged so tables use the platform's built-in automated statistics behavior. These properties are applied when the accelerator creates a table; existing tables are not reconciled in the per-table module write path. Explicit column mode automatically excludes columns with unsupported types (Boolean, Array, Map, Struct, Binary, Null) to prevent `IllegalArgumentException`. |
| `_filter_unsupported_stats_columns` | helper_functions_1.py | Filters out columns with data types unsupported by Delta data skipping (BooleanType, ArrayType, MapType, StructType, BinaryType, NullType). Applied before configuring statistics to prevent `IllegalArgumentException`. |

#### Configuration & Metadata Parsing

Functions for parsing and processing configuration metadata from the control tables into usable formats.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `parse_json_metadata` | helper_functions_3.py | **Orchestrator:** Main entry point for parsing all JSON metadata parameters passed to notebooks. |
| `clean_advanced_config` | helper_functions_3.py | Cleans and normalizes the advanced configuration list, removing empty or invalid entries. |
| `normalize_schema_details` | helper_functions_3.py | Normalizes schema details from various formats into a consistent structure. |
| `extract_schema_tracking_info` | helper_functions_3.py | Extracts schema tracking information including schema ID and MD5 hash from latest schema details. |
| `parse_source_configuration` | helper_functions_3.py | Parses source configuration including connection details, query logic, and ingestion parameters. |
| `_get_datastore_config` | helper_functions_3.py | Retrieves datastore configuration from the Datastore_Configuration table with helpful error message if missing. |
| `determine_medallion_layer_defaults` | helper_functions_3.py | Determines default configuration values (merge type, default watermark column, column name cleansing) based on medallion layer (Bronze/Silver/Gold). Silver defaults to the `delta__modified_datetime` watermark column for curated tables. |
| `parse_target_configuration` | helper_functions_3.py | Parses target configuration including workspace, datastore, table names, and output settings. |
| `parse_file_ingestion_paths` | helper_functions_3.py | Parses file ingestion path configurations including source files volume path and staging paths. |
| `parse_watermark_configuration` | helper_functions_3.py | Parses watermark configuration for incremental loading including column name, data type, and initial value. |
| `parse_advanced_processing_configuration` | helper_functions_3.py | Parses advanced processing configuration including batch processing, quarantine table, and output format settings. |
| `parse_primary_key_configuration` | helper_functions_3.py | Parses primary key configuration from comma-separated string into list format. |
| `parse_advanced_configuration_steps` | helper_functions_3.py | Parses and categorizes advanced configuration steps into transformations and data quality checks. |
| `parse_dimension_table_configuration` | helper_functions_3.py | Parses dimension table configuration including SCD2 enablement and comparison columns. |
| `parse_warehouse_configuration` | helper_functions_3.py | Parses Databricks SQL warehouse configuration including endpoint, authentication, and connection settings. |
| `parse_performance_configuration` | helper_functions_3.py | Parses performance configuration including liquid clustering, vacuum, and statistics columns. |
| `parse_spark_configuration` | helper_functions_3.py | Parses custom Spark configuration settings for session-level optimizations. |
| `parse_bronze_layer_spark_configuration` | helper_functions_3.py | Returns default Spark configurations optimized for Bronze layer ingestion. |
| `parse_silver_layer_spark_configuration` | helper_functions_3.py | Returns default Spark configurations optimized for Silver layer transformations. |
| `parse_gold_layer_spark_configuration` | helper_functions_3.py | Returns default Spark configurations optimized for Gold layer aggregations and analytics. |
| `apply_spark_configurations` | helper_functions_3.py | Applies custom Spark configurations to the active Spark session including adaptive query execution, broadcast settings, etc. |
| `extract_lineage_information` | helper_functions_3.py | Extracts source and target lineage details (medallion layer and system type) from configuration for logging and data flow tracking through the medallion architecture. |

#### Utility & Helper Functions

Miscellaneous utility functions for common operations.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `create_schema_if_not_exists` | helper_functions_3.py | Creates database schema in target catalog/schema if it doesn't already exist. |
| `drop_table_for_full_reload` | helper_functions_3.py | Drops target table when full reload is requested via parameter. |
| `check_target_table_exists` | helper_functions_3.py | Checks if target Delta table exists in the specified catalog and schema. |
| `determine_first_run_and_table_existence` | helper_functions_3.py | Determines if this is first run and validates table existence for merge operations. |
| `drop_external_table_for_shortcut` | helper_functions_3.py | Drops external tables created for external references to prevent endpoint conflicts. |
| `execute_vacuum_if_needed` | helper_functions_3.py | Evaluates table history and invokes vacuum only when thresholds require it. |
| `check_recent_vacuum_operations` | helper_functions_3.py | Checks Delta table history to determine if vacuum was recently executed. |
| `execute_vacuum_on_table` | helper_functions_3.py | Executes Delta VACUUM operation to remove old file versions based on retention period. |
| `cleanup_temporary_files` | helper_functions_3.py | Removes temporary staging files from file ingestion operations. |

#### Entity Resolution

Advanced functions for Master Data Management (MDM) to match and link records.

| Function | Helper Module | Purpose |
|----------|----------------|---------|
| `entity_resolution` | helper_functions_3.py | **Orchestrator:** Implements a comprehensive entity resolution system with multiple comparison techniques. |
| `ngram_matching` | helper_functions_3.py | Performs efficient N-gram based pre-filtering for fuzzy matching using MinHash LSH to improve scalability. |
| `_get_id_columns` | helper_functions_3.py | Retrieves the primary and secondary dataset ID column names for matching. |
| `_apply_self_comparison_filter` | helper_functions_3.py | Applies a filter to avoid duplicate comparisons when matching a dataset against itself. |
| `_merge_comparison_results` | helper_functions_3.py | Merges new comparison results with the accumulated results from previous steps. |
| `_extract_entity_configuration` | helper_functions_3.py | Extracts and validates the configuration parameters for the entity resolution process. |
| `_prepare_datasets` | helper_functions_3.py | Prepares the primary and secondary datasets with unique identifiers and aliased columns. |
| `_perform_exact_soundex_comparisons` | helper_functions_3.py | Performs exact and phonetic (Soundex) comparisons between datasets. |
| `_perform_fuzzy_comparisons` | helper_functions_3.py | Performs fuzzy matching using Levenshtein distance to find similar but not identical records. |
| `_perform_percent_difference_comparisons` | helper_functions_3.py | Performs percent difference comparisons for numeric fields. |
| `_apply_matching_rules` | helper_functions_3.py | Applies auto-match and manual review logic to categorize the comparison results. |
| `_apply_transitive_mapping` | helper_functions_3.py | Uses graph algorithms (networkx) to find transitive relationships between matched records. |
| `_assemble_final_results` | helper_functions_3.py | Combines the matched and unmatched records into the final, resolved result set. |

### Error Handling
- All major operations wrapped in try-except blocks
- Failures logged to Data Pipeline Logs table
- Schema mismatches can trigger warnings or failures based on configuration
- Data quality violations handled according to metadata rules (warn/quarantine/fail)

### Performance Optimizations
- Dynamic Spark compute via Databricks cluster policies
- Liquid clustering for frequently queried columns
- Statistics computation for join/filter columns
- Efficient merge strategies based on data characteristics
- Parallel processing where applicable

## Other Utility Modules

## Metadata Warehouse Stored Procedures

**Keywords:** stored procedures, FIFO_Status, Get_Pipeline_Tables, Pivot_Primary_Config, Get_Advanced_Metadata, Get_Watermark_Value, Get_Schema_Details, Get_Datastore_Details, Log_Data_Movement, Log_New_Schema, Get_Exploratory_Analysis_Input, Get_Last_Run_Status, Create_Date_Dimension, metadata warehouse, SP

This section provides a detailed overview of the stored procedures defined in the Databricks SQL Metadata Warehouse, which play a crucial role in facilitating data movement between the layers of the medallion architecture (Bronze, Silver, and Gold). These stored procedures encapsulate reusable logic, enabling efficient, consistent, and metadata-driven operations across the data ingestion and transformation process.

The following picture summarizes all the stored procedures defined in the Databricks SQL warehouse:

## FIFO Status

Confirms there is no existing ingestion occurring for a Table ID as a new invocation for the Table ID is kicked off.

### Parameters
- **table_id** int
- **target_datastore** varchar(255)

### Output Columns
- **Proceed** - 'Yes' if no blocking run exists and ingestion can proceed

## Get Datastore Details

Resolves a datastore name to its full `Datastore_Configuration` row. Used by the runtime helpers in `helper_functions_3.py` to look up environment-specific connection details at runtime, so that metadata SQL stays environment-agnostic (no raw workspace IDs or catalog names). If the datastore name is not found, the lookup throws an error listing all available datastores.

### Parameters
- **datastore_name** string - Name of the datastore to look up (e.g., `bronze`, `silver`, `gold`, `metadata`, or an external system name like `sap_erp`)

### Output Columns
- **Datastore_Name** - Logical name of the datastore
- **Datastore_Kind** - Discriminator. `databricks` for medallion layers and the metadata datastore, or an external kind such as `sql_server`, `snowflake`, `rest_api`, `adls_gen2`.
- **Medallion_Layer** - Medallion architecture layer (`bronze`/`silver`/`gold`/`metadata`). NULL for external systems.
- **Workspace_ID** - Databricks workspace ID for `databricks` rows. NULL for external systems.
- **Workspace_URL** - Databricks workspace URL (`https://adb-xxx.azuredatabricks.net`). NULL for external systems.
- **SQL_Warehouse_ID** - Databricks SQL warehouse used for this datastore. NULL for external systems.
- **Catalog_Name** - Unity Catalog catalog for `databricks` rows. NULL for external systems.
- **Connection_Details** - JSON blob with kind-specific fields (host, database, secret scope, base URL, ...) for external systems. NULL for `databricks` rows.

> Per-layer schemas are **not** stored here. Each metadata row's `Target_Entity` carries `"schema.table"`, so one datastore row serves every table in that layer.

## Get Watermark Value

Gets the watermark value for a specific table ID from the Data Pipeline Logs table.

### Parameters
- **table_id** int
- **target_datastore** varchar(255)
- **processing_phase** varchar(50) - 'Staging' (extract to files) or 'Batch' (load to tables). Default: 'Batch'

### Output Columns
- **watermark_value** - The resolved watermark value from the last successful run
- **matched_records** - Count of previous successful runs found
- **full_reload** - 'Yes' if triggered by FULL_RELOAD, NULL otherwise

## Get Schema Details

Gets the schema details value for a specific table ID from the Schema Logs table.

### Parameters
- **table_id** int

### Output Columns
- **Schema_ID** - Unique schema identifier (or 'NoData' if no schema exists)
- **Schema_Details** - PySpark schema representation (or 'NoData' if no schema exists)
- **Schema_Arrival_Time** - Timestamp when the schema was recorded (NULL if no schema exists)

## Log Data Movement

Creates a logging record for when data movement starts, finishes, and fails.

### Parameters
- **log_id** varchar(4000) 
- **table_id** int 
- **source_details** varchar(4000) 
- **target_datastore** varchar(4000)
- **target_entity** varchar(4000) 
- **event_start_time** DATETIME2(7)
- **event_end_time** DATETIME2(7)
- **status** varchar(4000) 
- **watermark_value** varchar(4000) 
- **records_processed** varchar(4000) 
- **quarantined_records** varchar(4000) 
- **data_quality_warnings** varchar(4000) 
- **data_quality_failures** varchar(4000) 
- **job_run_url** varchar(4000) 
- **trigger_name** varchar(4000) 
- **trigger_step** int
- **trigger_id** varchar(4000) 
- **trigger_time** DATETIME2(7)
- **source_medallion_layer** varchar(50) - Medallion layer of the source (Bronze/Silver/Gold)
- **source_type** varchar(100) - Type of source (UC Catalog/Warehouse)
- **target_medallion_layer** varchar(50) - Medallion layer of the target (Bronze/Silver/Gold)
- **target_type** varchar(100) - Type of target (UC Catalog/Warehouse)
- **processing_phase** varchar(50) - 'Staging' (extract to files) or 'Batch' (load to tables). Default: 'Batch' 

## Log New Schema

Logs new schemas in the Schema Logs table and records schema changes in the Schema Changes table.

### Parameters
- **table_id** int
- **datastore_name** varchar(4000) - Name of the datastore (catalog/warehouse)
- **table_name** varchar(4000)
- **schema_id** varchar(4000)
- **schema_details** varchar(max) - PySpark schema representation
- **schema_updates** varchar(max) - JSON array of schema changes (column, data_type, change_type)
- **job_run_url** varchar(4000)
- **end_time** datetime2(6) - Timestamp when schema was processed

## Pivot Primary Config

Extract Primary Config Metadata values for a specific Table ID. Values are pivoted for easy use of values later in data pipeline.

### Parameters
- **table_id** nvarchar(50)

### Output Columns
- Dynamic columns based on `{Configuration_Category}_{Configuration_Name}` pattern for the given Table_ID (e.g., `source_details_datastore_name`, `target_details_merge_type`)

## Get Advanced Metadata

Extract Advanced Config Metadata values for a specific Table ID. Values are concatenated for easy use of values later in data pipeline

### Parameters
- **table_id** nvarchar(50)

### Output Columns
- **Configuration_Category** - Category of the configuration (e.g., `data_transformation_steps`, `data_quality`)
- **Configuration_Name_Instance_Number** - Instance number for ordering multiple configurations of the same type
- **advanced_settings** - JSON string containing all configuration attributes for that category/instance

## Get Exploratory Analysis Input

Get table details needed to run exploratory data analysis on Delta tables. Returns tables that are due for profiling based on their configured `data_profiling_frequency` setting.

### Parameters
- **trigger_name** varchar(255)

### Output Columns
- **Table_ID** - Unique identifier from the Orchestration Metadata table
- **Target_Datastore** - Datastore name (catalog or warehouse)
- **Target_Entity** - Target table name
- **Data_Profiling_Frequency** - Configured profiling frequency (daily/weekly/monthly/never)
- **Last_Run_Datetime** - When EDA was last run on this table
- **Days_Since_Last_Run** - Number of days since last EDA run

## Get Last Run Status

Gets the status of the last pipeline run for a specific table ID.

### Parameters
- **table_id** int

### Output Columns
- **Last_Ingestion_Status** - Status of the last run ('Processed' or 'Failed')

## Get Pipeline Tables

Returns the tables to process for a given trigger, grouped by `Order_Of_Operations`. Each row represents one processing step containing a comma-separated list of table details. The source datastore name is included from Primary Configuration if it exists, otherwise empty.

### Parameters
- **trigger_name** varchar(255) - Name of the trigger to get tables for
- **start_with_step** int (optional, default: `0`) - Minimum `Order_Of_Operations` step to include
- **table_ids** varchar(4000) (optional, default: `'0'`) - Comma-separated list of Table_IDs to filter by, or `'0'` for all tables

### Output Columns
- **Trigger_Name** - Name of the trigger
- **Order_Of_Operations** - Processing step number
- **Pipeline_Table_Details** - Comma-separated list of table details in the format: `'Table_ID:Processing_Method:Target_Datastore:Target_Entity:Source_Datastore:Staging_Volume:Spark_Environment_ID:Log_ID'` (source, staging, and spark-environment values are empty when not configured; `Log_ID` is a pre-generated GUID so the calling job can log to `Activity_Run_Logs` even when the downstream module or task fails to start)

## Create Date Dimension

Creates a comprehensive date dimension table for analytical workloads. Populates the Date_Dimension table with dates from 2025-03-26 to 2035-12-31 if the table is empty.

### Parameters
- None

---

