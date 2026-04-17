# Metadata Reference

> Last updated: 2026-03-29

This file is the canonical metadata reference for accepted configuration categories, names, attributes, and allowed values.

Use this file first when the question is about valid metadata, defaults, required combinations, or accepted values. Use the split workflow guides (e.g., `DATA_INGESTION_GUIDE.md`, `DATA_TRANSFORMATION_GUIDE.md`) for scenario walkthroughs, not as the authoritative metadata dictionary.

## Table of Contents

- [Metadata Overview and Importance](#metadata-overview-and-importance)
- [Metadata Validation](#metadata-validation)
- [Canonical Rules](#canonical-rules)
- [Canonical Reference Map](#canonical-reference-map)
- [Orchestration Details](#orchestration-details)
- [Primary Config - Data Ingestion with Pipeline](#primary-config---data-ingestion-with-pipeline)
- [Primary Config - Curating Data](#primary-config---curating-data)
- [Advanced Config](#advanced-config)
  - [data_quality](#data_quality)
  - [data_transformation_steps](#data_transformation_steps)
- [When to Use Other Canonical Docs](#when-to-use-other-canonical-docs)
- [Migration Status](#migration-status)

## Metadata Overview and Importance

Metadata tables are central to this solution, enabling a flexible, manageable, and scalable data ingestion framework. They store critical information that guides pipeline behavior, supports monitoring, and ensures data consistency. By externalizing configurations, the solution becomes more adaptable to changes.

Custom PySpark functions are stored in notebooks within your workspace (e.g., `custom_Sales_Transforms`) and referenced via the `files_to_run` metadata attribute.

All metadata tables are stored in the Fabric **Metadata Warehouse**.

### Understanding Metadata Table Relationships
*   **Table ID** is the primary key that links records across `Orchestration`, `PrimaryConfig`, and `AdvancedConfig` tables.
*   A single `Table ID` represents data movement to a single target table or file (e.g., oracle table to Bronze, or multiple Silver tables to a Gold fact table).

## Metadata Validation

Run the validation script before you commit or deploy metadata SQL. It checks orchestration rows, primary configuration, advanced configuration, documented value constraints, and a set of cross-record consistency rules.

### Command Line Usage

```bash
python .github/skills/metadata-validation/validate_metadata_sql.py path/to/metadata/metadata_MyTrigger.sql

python .github/skills/metadata-validation/validate_metadata_sql.py

python .github/skills/metadata-validation/validate_metadata_sql.py --base-dir "MyWorkspace"
```

Use the sections below when the validator complains about allowed values or required attributes. Use [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for authoring patterns and [METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md) for SQL file structure.

### Validation Output

The validator reports the violated rule number and the specific configuration problem. Typical failure classes are:
- invalid or undocumented values
- missing required metadata attributes
- inconsistent relationships between orchestration, primary, and advanced records
- invalid custom notebook/function references
- duplicate writers targeting the same datastore and entity

Example output:
```
🔍 Validating: metadata_SalesData.sql
❌ Rule 12 - merge_type value 'upsert' is not valid. Allowed: overwrite, append, merge, merge_and_delete, merge_mark_unmatched_deleted, merge_mark_all_deleted, replace_where, warehouse_spark_connector, scd2, output_file
❌ Rule 23 - Table_ID 'S_100' has watermark_details but merge_type='overwrite'. Watermarks are ineffective with overwrite.
✅ All other checks passed

Found 2 errors in metadata_SalesData.sql
```

### Duplicate Writer Exception

Keep one writer per target by default. If a duplicate writer is intentional, add a same-line orchestration comment so the validator downgrades that case to a warning:

```sql
('LateArriving_Repair', 1, 220, 'silver', 'dbo.customers', 'customer_id', 'batch', 1) -- exception: late-arriving correction process
```

- Without the comment, the duplicate remains an error.
- The comment does not serialize runtime execution; trigger dependencies still need to prevent concurrent writes.

## Canonical Rules

1. Do not invent configuration categories, names, or attributes.
2. Use exact accepted values from this file.
3. Keep metadata environment-agnostic by using datastore references instead of hardcoded IDs.
4. Use built-in transformations before custom code.
5. Validate metadata SQL before delivery.

## Canonical Reference Map

| Topic | Canonical Source | Notes |
|---|---|---|
| Orchestration fields | This file | `Trigger_Name`, `Order_Of_Operations`, `Table_ID`, `Target_Datastore`, `Target_Entity`, `Primary_Keys`, `Processing_Method`, `Ingestion_Active` |
| Primary Config - ingestion | This file | External-source staging configuration and ingestion-only options |
| Primary Config - curating | This file | Source-side and target-side processing configuration |
| Watermark details | This file | Incremental loading and Delta CDF semantics |
| Advanced config patterns | This file | `data_quality` and `data_transformation_steps` attributes |
| Validation behavior | [.github/skills/metadata-validation/SKILL.md](../.github/skills/metadata-validation/SKILL.md) | Validator workflow and expectations |

## Orchestration Details

**Keywords:** orchestration, Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active, pipeline_stage_and_batch, batch, pipeline_stage_only

Governs the overall flow and sequence of data movements.

|column_name|Description|Usage/Accepted Values|
| :-------- | :------- | :-------- |
|Trigger_Name|Identifier for a set of data movements, often representing a data product or source system.||
|Order_of_Operations|Numeric value defining execution sequence and parallelism. Lower numbers execute first. Operations with the same number run in parallel.|Integer > 0|
|Table_ID|Unique incremental integer identifying a distinct ingestion or data movement task. Primary Key.|Integer > 0|
|Target_Datastore|Name of catalog or warehouse in the workspace.|e.g. `bronze`, `silver`, `gold`|
|Target_Entity|Destination folder path or table name including schema, for example `dbo.sales_table`.||
|Primary_Keys|Comma-separated list of primary key columns for the target table.|e.g., `CustomerID,OrderID`|
|Processing_Method|Processing method for ingestion or transformation.|`batch`, `pipeline_stage_and_batch`, `pipeline_stage_only`, `execute_warehouse_sp`, `execute_databricks_notebook`, `execute_databricks_job`|
|Ingestion_Active|Flag to enable or disable this specific data movement record.|`0`, `1`|

## Primary Config - Data Ingestion with Pipeline

Configuration for copying data from external source systems.

|Configuration Category|Configuration Name|Ingestion Patterns it's Required|Ingestion Patterns it's Optional|More Details|Accepted Values|
| :-------- | :------- | :-------- | :-------- | :------- | :-------- |
|source_details|source|All||Specifies the type of the source system for pipeline-owned staging in `PL_02`.|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`|
|source_details|datastore_name|All||Logical name of the external source registered under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json`. The runtime resolves the actual `Connection_Details` from the `Datastore_Configuration` Delta table at runtime (synced on `/fdp-04-commit`). This keeps metadata SQL environment-agnostic.|Datastore name such as `oracle_sales`, `sftp_uploads`|
|source_details|database_name|`azure_sql`, `sql_server`||Name of the source database.||
|source_details|query|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`||SQL query to extract data. Use if `table_name` is not sufficient.||
|source_details|schema_name|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`||Schema name in the source database.||
|source_details|table_name|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`||Table name in the source database.||
|source_details|staging_volume_name|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`||Name of catalog used for staging data.||
|source_details|staging_folder_path|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`||Path in file area of `staging_volume_name` to stage data.||
|source_details|sftp_wildcard_folder_path|`sftp`||SFTP folder path pattern to copy files from. Supports wildcards.||
|source_details|sftp_wildcard_file_name||`sftp`|SFTP file name pattern to copy. Defaults to `*`. Supports wildcards.|e.g., `*.csv`, `data_*.parquet`|
|watermark_details|column_name||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`|Column used for incremental data extraction.|e.g., `LastModifiedDate`|
|watermark_details|data_type||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`|Data type of the watermark column.|`numeric`, `datetime`|
|source_details|partitioning_option||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`|Enables source-side partitioning for faster extraction.||
|source_details|copy_pattern_suffix||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`|Optional suffix appended to the PL_02 copy-switch routing key after `source` and before `partitioning_option`. Current supported suffix is `csv`.|`csv` or blank|
|source_details|encoding||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`|Specify if source data is not UTF-8.||
|source_details|convert_csv_to_parquet||`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`, `rest_api`, `sftp`|If `true`, PL_02 post-staging logic converts staged CSV files to Parquet, then removes the staged CSV files. Default: `false`.|`true`, `false`|
|target_details|enforce_not_null|`azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`|If `true`, attempts to enforce NOT NULL constraints in the Delta table from original source DB. Default: `false`.|`true`, `false`|

## Primary Config - Curating Data

**Keywords:** curating, source_details, target_details, watermark_details, other_settings, column_cleansing, data_cleansing, merge_type, table_name, custom function, custom_table_ingestion_function, custom_source_function, liquid_clustering, overwrite, append, merge, scd2

Configuration for processing data from files or existing Delta tables into target Delta tables.

### source_details

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|table_name|Source table name with schema. Must be 3 part naming convention: `datastore_name.schema_name.table_name`||
|custom_table_ingestion_function_notebook|Name of a PySpark notebook containing your custom table ingestion function. Notebook name without `.py`.|`notebook1`|
|custom_table_ingestion_function|Name of a custom Python function for table or SQL based data extraction.|`func1`|
|custom_file_ingestion_function_notebook|Name of a PySpark notebook containing your custom file ingestion function. Notebook name without `.py`.|`notebook1`|
|custom_file_ingestion_function|Name of a custom Python function for file-based data extraction. Requires `wildcard_folder_path`.|`func1`|
|custom_staging_function_notebook|Name of a PySpark notebook containing your custom staging function. Used with `Processing_Method='batch'`. Notebook name without `.py`.|`notebook1`|
|custom_staging_function|Name of a custom Python function for notebook-owned staging data. Must return a dict with `rows_copied` and `next_watermark_value`.|`func1`|
|exit_after_staging|If `true`, notebook-owned custom staging lands files and exits before table processing. Default: `false`.|`true`, `false`|
|custom_source_function_notebook|Name of a PySpark notebook containing your custom source function. Used for external sources where the function returns a DataFrame for framework processing. Notebook name without `.py`.|`notebook1`|
|custom_source_function|Name of a custom Python function for external data extraction. Returns either a DataFrame or `(DataFrame, next_watermark_value)` and manages its own retrieval contract.|`func1`|
|datastore_name|Source datastore reference. For file ingestion, identifies the source volume when reading files not in bronze. For `execute_databricks_notebook` and `execute_databricks_job`, set this to the `Datastore_Configuration` entry that stores the Fabric item and workspace GUIDs. For `execute_warehouse_sp`, set this to the warehouse host entry.|Datastore name such as `bronze`, `silver`, `gold`, `ml_predictions_notebook`, `sales_warehouse`|
|stored_procedure_name|Stored procedure name for `Processing_Method='execute_warehouse_sp'`. Must be placed in `source_details` and paired with `source_details.datastore_name`.|e.g., `[dbo].[RunSalesLoad]`|
|wildcard_folder_path|Folder path with wildcards such as `myfolder/*.csv`. Required for scheduled file ingestion.||
|schema|Schema contract definition for files or Delta tables. For files, defines expected columns and types when not inferable. For Delta tables, enforces schema contract validation.|e.g., `name STRING, age INT, city STRING`|
|delimiter|Column delimiter for CSV files.|e.g., `,`, `\t`, `\|`|
|file_has_header_row|Indicates if the CSV file or Excel sheet has a header row.|`true`, `false`|
|sheet_name|Name of the sheet or sheets in an Excel file. Provide a single sheet, comma-separated list, or `*` for every sheet.||
|multiline|Set to `true` if records span multiple lines. Applies to JSON and CSV.|`true`, `false`|
|encoding|Overrides file encoding used during ingestion and temporary staging. Defaults to UTF-8 when not provided.|Any supported IANA encoding name|
|xml_xpath|XPath expression evaluated by `pandas.read_xml` when ingesting XML files.|Valid XPath string|
|xml_namespaces_keys|Comma-separated list of XML namespace prefixes when XPath uses namespace-qualified elements. Must be paired with `xml_namespaces_values`.|e.g., `ns1,ns2`|
|xml_namespaces_values|Comma-separated list of namespace URIs aligned with `xml_namespaces_keys`.|e.g., `http://schemas.example.com/ns1,http://schemas.example.com/ns2`|
|on_bad_records|Action for records not matching the schema.|`quarantine`, `fail`, `drop`|
|process_one_file_at_a_time|Whether you want to process new files one at a time. Default: `false`. XML and Excel files are always processed this way.|`true`, `false`|
|spark_timestamp_rebase_mode|Sets Spark configuration values for Parquet datetime rebasing. Default: `CORRECTED`.|`CORRECTED`, `EXCEPTION`, `LEGACY`|

### target_details

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|merge_type|Defines how data is written to the target Delta table. Defaults are `append` for Bronze, `merge` for Silver and Gold, and `merge_and_delete` if `delete_rows_with_value` has a value.|`overwrite`, `append`, `merge`, `merge_and_delete`, `merge_mark_unmatched_deleted`, `merge_mark_all_deleted`, `replace_where`, `warehouse_spark_connector`, `scd2`, `output_file`|
|merge_in_batches_with_columns|Specify one or more columns to control how data is written in sequential batches based on unique values.|e.g., `delta__raw_folderpath`, `delta__created_datetime`|
|replace_where_column|Required when `merge_type = replace_where`. Column used to determine which data to replace in the target table.|Column name such as `sale_date`, `month`, `region`|
|warehouse_write_mode|If writing data to a warehouse with Spark.|`errorifexists`, `ignore`, `append`, `overwrite`|
|quarantine_table_name|Name for the table storing quarantined records. Default: `{target_table_name}_quarantined`.|e.g., `dbo.sales_table_quarantined`|
|delimiter|Column delimiter when outputting to CSV or TXT files instead of Delta.|e.g., `,`, `\t`, `\|`|
|sheet_name|If outputting to Excel instead of Delta, name of the sheet.||
|external_location|Path to an ADLS Gen2 container or folder via Fabric shortcut for externally managed Delta tables. Must start with `Files/`.||
|spark_timestamp_rebase_mode|Sets Spark configuration values for Parquet datetime rebasing in write mode. Default: `CORRECTED`.|`CORRECTED`, `EXCEPTION`, `LEGACY`|
|use_spark_config_for_catalog|Override the default Spark settings for the target datastore.|`bronze`, `silver`, `gold`|
|liquid_clustering_columns|Comma-separated list of columns for Liquid Clustering on the target Delta table.||
|compute_statistics_on_columns|Comma-separated list of columns to persist in `delta.dataSkippingStatsColumns` for the target Delta table. Leave blank for the default accelerator behavior, which does not manage the property and relies on the platform's built-in automated statistics. The notebook applies this when it creates a table; existing tables require an explicit recreate or ALTER TABLE flow outside the per-table processing notebook.|e.g., `column1,column2`|
|compute_statistics_on_first_n_columns|Integer to persist in `delta.dataSkippingNumIndexedCols` for the target Delta table when you explicitly opt into count-based managed stats. Leave blank to avoid managing the property and rely on the platform's built-in automated statistics. The notebook applies this when it creates a table; existing tables require an explicit recreate or ALTER TABLE flow outside the per-table processing notebook.|e.g., `64`|
|enable_change_data_feed|If `true`, change data feed is enabled on the target table when the notebook creates it. Default: `false`. Existing tables require an explicit recreate or ALTER TABLE flow outside the per-table processing notebook.|`true`, `false`|
|fail_on_new_schema|If `true`, pipeline fails if source schema changes leading to Delta schema change. Default: `false`.|`true`, `false`|
|fail_on_column_data_type_change|If `true`, pipeline fails if an existing column changes data type in Delta. Default: `false`.|`true`, `false`|
|if_duplicate_primary_keys|Action for duplicate primary keys when writing to Delta. Default: `fail`.|`warn`, `quarantine`, `fail`|
|column_to_mark_source_data_deletion|Column used to indicate data was deleted at the true source.|Column name|
|delete_rows_with_value|Value within `column_to_mark_source_data_deletion` that identifies a deleted record.|e.g., `1`, `X`|
|source_timestamp_column_name|Required when `merge_type='scd2'`. Specifies the timestamp column used for `scd_start_date` and `scd_end_date` values.|Column name|

### watermark_details

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|column_name|Column used for incremental data extraction. Use comma-separated values to use multiple watermark columns. Defaults to `delta__modified_datetime` for Silver and Gold. Multiple watermark columns are only supported when reading from Delta tables.|`Column name` or comma-separated names|
|data_type|Data type of the watermark column. Default: `datetime`. Not used when `use_change_data_feed = true`.|`numeric`, `datetime`|
|use_watermark_column|Whether incoming data should be filtered with the watermark column and watermark value. Default: `true`.|`true`, `false`|
|use_change_data_feed|Enable Delta Change Data Feed for incremental loading from Delta tables. Default: `false`.|`true`, `false`|
|table_name|Table name to use to extract watermark value. Only use when using a custom ingestion function for ingestion from Delta tables.|e.g., `silver.dbo.Sales`|

### other_settings

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|spark_environment_id|ID of a Databricks compute cluster to use for Spark compute. Controls cluster size, external libraries, and session-level Spark properties.|Databricks compute cluster GUID|
|data_profiling_frequency|Frequency for running data profiling on the target Delta table. Default: `weekly`.|`never`, `daily`, `weekly`, `monthly`|

### column_cleansing

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|trim|If `true`, removes leading and trailing whitespace from all column names.|`true`, `false`|
|apply_case|Converts column names to specified case after other column name transformations.|`lower`, `upper`, `title`|
|replace_non_alphanumeric_with_underscore|If `true`, replaces all non-alphanumeric characters except underscores with underscores in column names, then collapses consecutive underscores.|`true`, `false`|
|regex_find|Regex pattern to find in column names. Used with `regex_replace`.|Valid regex pattern|
|regex_replace|Replacement value for matches found by `regex_find`. Use empty string to remove matched patterns.|Any string or empty string|
|exact_find|Comma-separated list of exact string values to find in column names.|Comma-separated values|
|exact_replace|Replacement value or values for strings found in `exact_find`. Counts must match if comma-separated.|Single value or comma-separated values with exact count match|

### data_cleansing

|Configuration Name|More Details|Accepted Values|
| :--------------- | :----------- | :-------------- |
|replace_blank_with_null_in_string_columns|Comma-separated list of columns to replace empty strings with NULL, or `*` for all string columns.|Blank string, `*`, or comma-separated column names|
|trim_data_in_string_columns|Comma-separated list of columns to trim leading and trailing whitespace, or `*` for all string columns.|Blank string, `*`, or comma-separated column names|

## Advanced Config

**Keywords:** advanced configuration, data_quality, data_transformation_steps, Configuration_Name_Instance_Number, not_null, unique, threshold, range, quarantine, warn, fail, derived_column, join_data, filter_data, aggregate_data, surrogate key, scd2

Stores complex metadata, often spanning multiple rows or categories, for data transformations, quality checks, and surrogate key logic.

Use `Configuration_Name_Instance_Number` to use a configuration multiple times for a single dataset and to define the order in which transformation steps are run.

### data_quality

Every data quality check must include a `message` attribute. This short human-readable description is used in quarantine reasons, marker column details, and warning or failure logs.

|Configuration Name|Configuration Attribute Name|Details|Usage/Accepted Values|
| :--------------- | :--------------------------- | :---------------------------------------------------------------------- | :----------------------------------------------------- |
|validate_condition|if_not_compliant|Action if validation condition is violated.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_condition|condition|Spark SQL expression for the validation condition.|e.g., `column_name > 10 OR YEAR(column_name) = 2012`|
|validate_condition|message|Required. Human-readable description of what the check validates.|e.g., `Amount must be positive`|
|validate_referential_integrity|if_not_compliant|Action if referential integrity check fails.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_referential_integrity|current_table_column_name|Column in the current table to use for the lookup.|e.g., `CustomerID`|
|validate_referential_integrity|reference_table_name|Name of the reference or master table including datastore and schema.|e.g., `silver.dbo.DimCustomer`|
|validate_referential_integrity|reference_table_column_name|Column in the reference or master table to join on.|e.g., `CustomerSK`|
|validate_referential_integrity|message|Required. Human-readable description of what the check validates.|e.g., `Customer must exist in DimCustomer`|
|validate_pattern|if_not_compliant|Action if pattern validation fails.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_pattern|column_name|Column or columns to validate against the pattern.|e.g., `PhoneNumber` or `home_phone, work_phone, mobile_phone`|
|validate_pattern|pattern_type|Type of pattern to validate.|`us_phone`, `uk_phone`, `email`, `us_zip`, `uk_postcode`, `us_ssn`, `date_mmddyyyy`, `date_yyyymmdd`, `url`, `credit_card`, `ip_address`, `ipv4_address`, `ipv6_address`, `alpha_only`, `alphanumeric_only`, `numeric_only`, `guid`|
|validate_pattern|allow_null|Whether null values should be considered valid. Default: `true`.|`true`, `false`|
|validate_pattern|message|Required. Human-readable description of what the check validates.|e.g., `Phone number must be valid US format`|
|validate_batch_size|if_not_compliant|Action if minimum record count per batch is not met.|Default: `warn`. Values: `warn`, `fail`|
|validate_batch_size|min_rows|Minimum expected records in a single processing batch.|Integer|
|validate_batch_size|message|Required. Human-readable description of what the check validates.|e.g., `Batch must contain at least 100 rows`|
|validate_anomaly|if_not_compliant|Action if statistical anomalies are detected.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_anomaly|column_name|Numeric column or columns to analyze for anomalies.|e.g., `transaction_amount` or `amount, quantity, price`|
|validate_anomaly|method|Statistical method for anomaly detection.|`z_score`, `iqr`|
|validate_anomaly|threshold|Detection threshold. For `z_score`, number of standard deviations. For `iqr`, IQR multiplier.|e.g., `3`, `1.5`, `2.5`|
|validate_anomaly|minimum_baseline_records|Minimum records required in target table for baseline statistics.|Default: `30`. Integer|
|validate_anomaly|message|Required. Human-readable description of what the check validates.|e.g., `Transaction amount must not be anomalous`|
|validate_not_null|if_not_compliant|Action if NULL values are found.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_not_null|column_name|Column or columns to check for NULL values.|e.g., `email` or `email, phone, customer_id`|
|validate_not_null|message|Required. Human-readable description of what the check validates.|e.g., `Email must not be null`|
|validate_unique|if_not_compliant|Action if duplicate values are found.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_unique|column_name|Column or columns that should contain unique values.|e.g., `order_id` or `customer_id, order_date`|
|validate_unique|message|Required. Human-readable description of what the check validates.|e.g., `Order ID must be unique`|
|validate_range|if_not_compliant|Action if values are outside the specified range.|Default: `warn`. Values: `warn`, `mark`, `quarantine`, `fail`|
|validate_range|column_name|Numeric or date column or columns to validate.|e.g., `amount` or `quantity, price, discount_pct`|
|validate_range|min_value|Minimum allowed value inclusive. At least one of `min_value` or `max_value` is required.|e.g., `0`, `0.0`, `1`, `2024-01-01`|
|validate_range|max_value|Maximum allowed value inclusive. At least one of `min_value` or `max_value` is required.|e.g., `1.0`, `100.0`, `1000000`, `2024-12-31`|
|validate_range|message|Required. Human-readable description of what the check validates.|e.g., `Amount must be between 0 and 1000000`|
|validate_freshness|if_not_compliant|Action if data is stale.|Default: `warn`. Values: `warn`, `fail`|
|validate_freshness|column_name|Timestamp or date column to check for freshness.|e.g., `last_updated`, `modified_date`|
|validate_freshness|max_age|Maximum allowed age as a numeric value.|e.g., `30`, `24`, `7`|
|validate_freshness|unit_of_measure|Time unit for `max_age`.|`minute`, `hour`, `day`, `week`, `month`|
|validate_freshness|message|Required. Human-readable description of what the check validates.|e.g., `Data must be updated within 24 hours`|
|validate_completeness|if_not_compliant|Action if column completeness falls below threshold.|Default: `warn`. Values: `warn`, `fail`|
|validate_completeness|column_name|Column or columns to check for completeness.|e.g., `email` or `phone, address, city`|
|validate_completeness|min_completeness_percent|Minimum required non-null percentage from 0 to 100.|Default: `95`. e.g., `90`, `99`, `100`|
|validate_completeness|message|Required. Human-readable description of what the check validates.|e.g., `Email must be at least 95% complete`|

### data_transformation_steps

|Configuration Name|Configuration Attribute Name|Details|Usage/Accepted Values|
| :--------------- | :--------------------------- | :----------------------------------------------------------------------------------- | :----------------------------------------------------- |
|columns_to_rename|existing_column_name|Current name of the column to be renamed. Can be comma-separated for multiple columns.|e.g., `cust_id` or `field1, field2`|
|columns_to_rename|new_column_name|New name for the column. Can be comma-separated for multiple columns.|e.g., `customer_id` or `field1_renamed, field2_renamed`|
|derived_column|column_name|Name of the new derived column. Use `configuration_name_instance_number` for multiple derived columns.|e.g., `full_name`|
|derived_column|expression|Spark SQL expression to create the derived column.|e.g., `lower(old_column_name)`, `concat(first_name, ' ', last_name)`|
|filter_data|filter_logic|Spark SQL expression to filter rows from the dataset.|e.g., `status = 'active'`|
|remove_columns|column_name|Name of the column to remove. Can be comma-separated for multiple columns.|e.g., `temporary_field` or `field1, field2`|
|select_columns|column_name|Name of the column to select. To select multiple columns, use comma-separated values. Wrap columns in backticks if they contain spaces or special characters.|e.g., `field1` or `field1, field2`|
|select_columns|retain_metadata_columns|Whether to keep `delta__` and `scd_` columns in the output. Default: `true`.|`true`, `false`|
|create_surrogate_key|type|Process used to create surrogate keys in a dimension table. Currently only `auto_increment` is supported.|`auto_increment`|
|create_surrogate_key|column_name|Name of the surrogate key column to create in the dimension table.|e.g., `customer_key`, `product_sk`, `dim_id`|
|attach_dimension_surrogate_key|dimension_table_name|Name of the dimension table to look up for surrogate keys.|e.g., `gold.dbo.dim_date`|
|attach_dimension_surrogate_key|dimension_table_join_logic|Spark SQL join condition between the current dataset and the dimension table using `a.` and `b.` aliases.|e.g., `a.order_date = b.date_attribute`|
|attach_dimension_surrogate_key|dimension_table_key_column_name|Required. Column name in the dimension table for the surrogate key.|Column name|
|attach_dimension_surrogate_key|dimension_key_output_column_name|Required. Output column name in the fact table for the surrogate key.|Column name|
|attach_dimension_surrogate_key|dimension_columns_to_add_to_fact|Optional. Comma-separated dimension-table columns to add to the fact table.|Column name or comma-separated names|
|drop_duplicates|column_name|Required. Columns to deduplicate on. Use `*` for all columns.|`*` or comma-separated column names|
|drop_duplicates|order_by|Optional. Columns to determine which row to keep when ordered deduplication is used.|Column name or comma-separated names|
|drop_duplicates|order_direction|Optional. Sort direction for `order_by` columns. Default: `desc`.|`asc`, `desc`, or comma-separated directions|
|change_data_types|column_name|Comma-separated list of column names to change data types for.|Column name or comma-separated names|
|change_data_types|new_type|PySpark data type to convert the columns to.|`string`, `int`, `long`, `float`, `double`, `boolean`, `date`, `timestamp`, `decimal(xx,xx)`, `binary`|
|apply_null_handling|column_name|Name of the column to apply null handling to. Can be comma-separated for multiple columns.|Column name or comma-separated names|
|apply_null_handling|action|Type of null handling action to perform.|`drop_rows`, `replace_with_default`, `convert_values_to_null`|
|apply_null_handling|logic|Optional when `action` is `drop_rows`. Determines whether to drop rows where any or all specified columns are null. Default: `any`.|`any`, `all`|
|apply_null_handling|default_value|Required when `action` is `replace_with_default`.|Any value convertible to the target type|
|apply_null_handling|values_to_convert|Optional when `action` is `convert_values_to_null`. Comma-separated list of values to convert to null.|Comma-separated values|
|apply_null_handling|values_delimiter|Optional delimiter for parsing `values_to_convert`. Default: `,`.|e.g., `\|`, space|
|apply_null_handling|convert_empty_space|Optional when `action` is `convert_values_to_null`. Default: `false`.|`true`, `false`|
|replace_values|column_name|Comma-separated list of column names to apply value replacement to.|Column name or comma-separated names|
|replace_values|values_to_replace|Comma-separated list of exact values to be replaced.|Comma-separated values|
|replace_values|replacement_value|Single value to replace all specified exact matches with.|Any value appropriate for the column data type|
|replace_values|values_delimiter|Optional delimiter for parsing `values_to_replace`. Default: `,`.|e.g., `\|`, space|
|mask_sensitive_data|column_name|Name of the column to mask. Can be comma-separated for multiple columns.|Column name or comma-separated names|
|mask_sensitive_data|upper_char|Optional replacement for uppercase letters. Default: `X`.|Single character|
|mask_sensitive_data|lower_char|Optional replacement for lowercase letters. Default: `X`.|Single character|
|mask_sensitive_data|digit_char|Optional replacement for digits. Default: `X`.|Single character|
|mask_sensitive_data|other_char|Optional replacement for other characters. If omitted, they remain unchanged.|Single character or blank|
|add_row_hash|column_name|Comma-separated list of columns to include in hash calculation, or `*` for all columns.|Comma-separated names or `*`|
|add_row_hash|output_column_name|Optional name of the new hash column. Default: `row_hash`.|Column name|
|add_row_hash|hash_algorithm|Optional hash algorithm. Default: `xxhash64`.|`xxhash64`, `sha256`|
|pivot_data|group_columns|Comma-separated list of columns to group by during pivot.|Comma-separated names|
|pivot_data|pivot_column|Column whose values become new column names.|Column name|
|pivot_data|value_column|Column whose values populate the pivoted columns. Can be comma-separated for multiple value columns.|Column name or comma-separated names|
|pivot_data|aggregation|Required aggregation function or functions to apply.|`sum`, `avg`, `min`, `max`, `count`, `first`, `last`, or comma-separated values|
|unpivot_data|id_columns|Comma-separated list of columns to keep as identifiers.|Comma-separated names|
|unpivot_data|value_columns|Comma-separated list of columns to unpivot into rows.|Comma-separated names|
|unpivot_data|variable_name|Optional name for the new column containing the original column names. Default: `variable`.|Column name|
|unpivot_data|value_name|Optional name for the new column containing the values. Default: `value`.|Column name|
|join_data|join_type|Type of join operation to perform. Default: `inner`.|`inner`, `left`, `outer`, `left_outer`, `full_outer`, `semi`, `anti`|
|join_data|right_table_name|Name of the right table to join with, including datastore and schema.|e.g., `Silver.dbo.Customers`|
|join_data|join_condition|SQL join condition using `a.` for the left table and `b.` for the right table.|e.g., `a.customer_id = b.customer_id AND a.region = b.region`|
|join_data|left_columns|Optional columns to select from left table, or `*` for all. Default: `*`.|Comma-separated names or `*`|
|join_data|right_columns|Optional columns to select from right table, or `*` for all. Default: `*`.|Comma-separated names or `*`|
|join_data|broadcast_hint|Optional broadcast hint for the right table. Default: `false`.|`true`, `false`|
|add_window_function|column_name|Source column name for aggregation or analytics functions. Not needed for ranking functions.|Column name|
|add_window_function|output_column_name|Required. Name for the output column.|Column name|
|add_window_function|window_function|Type of window function to apply.|`row_number`, `rank`, `dense_rank`, `first_value`, `last_value`, `sum`, `avg`, `count`, `min`, `max`|
|add_window_function|partition_by|Optional comma-separated list of columns to partition by.|Comma-separated names|
|add_window_function|order_by|Optional comma-separated list of columns to order by.|Comma-separated names|
|add_window_function|order_direction|Optional sort direction for `order_by`. Default: `asc`.|`asc`, `desc`, or comma-separated values|
|aggregate_data|group_by_columns|Optional comma-separated list of columns to group by. If empty, aggregates the entire DataFrame.|Comma-separated names|
|aggregate_data|column_name|Comma-separated list of column names to aggregate.|Comma-separated names|
|aggregate_data|aggregation|Comma-separated list of aggregation functions. Must match number of columns.|`sum`, `avg`, `count`, `min`, `max`, `first`, `last`, `count_distinct`|
|aggregate_data|output_column_name|Required. Comma-separated list of output column names. Must match number of aggregated columns.|Comma-separated names|
|transform_datetime|column_name|Name of the date or time column to transform.|Column name|
|transform_datetime|operation|Type of datetime transformation to apply.|`year`, `month`, `day`, `dayofmonth`, `dayofweek`, `dayofyear`, `hour`, `minute`, `second`, `quarter`, `weekofyear`, `add_days`, `subtract_days`, `add_months`, `date_diff`, `format_date`, `to_timestamp`, `current_date`, `current_timestamp`|
|transform_datetime|output_column_name|Required. Name for the new output column.|Column name|
|transform_datetime|days|Required when operation is `add_days` or `subtract_days`.|Integer|
|transform_datetime|months|Required when operation is `add_months`.|Integer|
|transform_datetime|end_date_column|Required when operation is `date_diff`.|Column name|
|transform_datetime|date_format|Required when operation is `format_date`.|e.g., `yyyy-MM-dd`|
|transform_datetime|timestamp_format|Optional when operation is `to_timestamp`.|e.g., `yyyy-MM-dd HH:mm:ss`|
|union_data|union_tables|Comma-separated list of table names to union with.|e.g., `silver.dbo.table1, silver.dbo.table2`|
|union_data|union_type|Type of union operation. Default: `by_name`.|`by_name`, `by_position`|
|union_data|allow_missing_columns|Whether to allow missing columns in union by name. Default: `true`.|`true`, `false`|
|union_data|deduplicate|Whether to remove duplicates after union. Default: `false`.|`true`, `false`|
|sort_data|column_name|Required. Column or columns to sort by.|Column name or comma-separated names|
|sort_data|sort_direction|Sort direction or directions for each column. Default: `asc`.|`asc`, `desc`, or comma-separated values|
|explode_array|column_name|Required. Name of the array column to explode into multiple rows.|Column name containing array type|
|explode_array|output_column_name|Name for the exploded column. Default: same as input column.|Column name|
|explode_array|preserve_nulls|If `true`, includes rows where array is null or empty. Default: `false`.|`true`, `false`|
|flatten_struct|column_name|Required. Name of the struct column to flatten into individual columns.|Column name containing struct type|
|flatten_struct|prefix|Prefix for flattened column names. Default: struct column name + `_`.|e.g., `address_`|
|flatten_struct|fields_to_include|Comma-separated list of specific struct fields to extract. Default: all fields.|Comma-separated names|
|flatten_struct|drop_original|If `true`, drops the original struct column after flattening. Default: `true`.|`true`, `false`|
|split_column|column_name|Required. String column to split.|Column name|
|split_column|delimiter|Character or characters to split on. Default: `,`.|e.g., `,`, `\|`, `-`|
|split_column|output_column_names|Required. Comma-separated output column names.|Comma-separated names|
|split_column|max_splits|Maximum number of splits. Default: `-1` for all.|Integer|
|split_column|drop_original|If `true`, drops the original column. Default: `false`.|`true`, `false`|
|concat_columns|column_name|Required. Comma-separated list of columns to concatenate.|Comma-separated names|
|concat_columns|output_column_name|Required. Name for the concatenated output column.|Column name|
|concat_columns|separator|String to use between values. Default: empty string.|e.g., space, `, `, `-`|
|concat_columns|null_handling|How to handle NULL values. Default: `skip`.|`skip`, `empty`, `keep`|
|concat_columns|drop_original|If `true`, drops the original columns. Default: `false`.|`true`, `false`|
|conditional_column|column_name|Required. Name of the output column to create.|e.g., `size_category`, `risk_level`|
|conditional_column|conditions|Required. Comma-separated list of Spark SQL conditions. Must match `values`.|Comma-separated conditions|
|conditional_column|values|Required. Comma-separated list of literal values to assign when conditions are true. Must match `conditions`.|Comma-separated values|
|conditional_column|default_value|Literal value when no conditions match. Default: `null`.|Any literal value|
|conditional_column|values_delimiter|Delimiter for parsing conditions and values. Default: `,`.|e.g., `;`, `\|`|
|string_functions|column_name|Required. Column or columns to transform.|Column name or comma-separated names|
|string_functions|function|Required. String function to apply.|`upper`, `lower`, `trim`, `ltrim`, `rtrim`, `reverse`, `length`, `initcap`, `soundex`|
|string_functions|output_column_name|Required. Name for the output column.|Column name|
|normalize_text|column_name|Required. Column or columns to normalize.|Column name or comma-separated names|
|normalize_text|operations|Comma-separated list of normalization operations to apply in order.|`lowercase`, `uppercase`, `trim`, `collapse_whitespace`, `remove_punctuation`, `remove_digits`, `remove_special_chars`, `ascii_only`, `strip_accents`|
|normalize_text|output_column_name|Output column name. Default: overwrites input column.|Column name|
|custom_transformation_function|functions_to_execute|Name of a custom Python function or functions for data processing. Run in list order.|Comma-separated function names|
|custom_transformation_function|files_to_run|Name of PySpark notebooks to execute to instantiate those functions. Notebook name without `.py`.|Comma-separated notebook names|
|entity_resolution|primary_dataset_alias|Required alias for the primary dataset.|e.g., `customers_a`|
|entity_resolution|secondary_dataset_alias|Required alias for the secondary dataset.|e.g., `customers_b`|
|entity_resolution|secondary_dataset_query|Required SQL query to fetch the secondary dataset.|e.g., `select * from silver.dbo.other_customers`|
|entity_resolution|primary_dataset_comparison_fields|Required comma-separated column names from the primary dataset for comparison.|Comma-separated names|
|entity_resolution|secondary_dataset_comparison_fields|Required comma-separated column names from the secondary dataset for comparison.|Comma-separated names|
|entity_resolution|comparison_types|Required comma-separated comparison types.|`soundex`, `fuzzy`, `exact`, `percent_difference`|
|entity_resolution|only_fuzzy_match_if_one_exact_match|If `true`, fuzzy matching is only applied to entities that have at least one exact or soundex match. Default: `true`.|`true`, `false`|
|entity_resolution|use_ngram_filtering_for_fuzzy_comparisons|If `true`, applies ngram filtering before levenshtein matching. Default: `false`.|`true`, `false`|
|entity_resolution|auto_match_logic|Required Spark SQL filter expression to identify automatic matches.|Spark SQL expression|
|entity_resolution|match_with_manual_review_logic|Required Spark SQL filter expression to identify matches needing manual review.|Spark SQL expression|

## When to Use Other Canonical Docs

- Use [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md) when the question is procedural.
- Use [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) when the question is escalation or built-in vs custom choice.
- Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) when the question is which custom function type to use.
- Use [INGESTION_PATTERNS_REFERENCE.md](INGESTION_PATTERNS_REFERENCE.md) when the question is source-pattern selection.

## Migration Status

- Phase 1: this file is the canonical metadata reference.
- Phase 2: shared instructions and prompts route here instead of the embedded Data Engineer section.
- Phase 3: the contract can move to a structured machine-readable source later without changing the external lookup surface.
