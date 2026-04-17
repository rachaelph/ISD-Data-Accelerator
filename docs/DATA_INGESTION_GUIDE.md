# Data Ingestion Guide

> Last updated: 2026-03-29

Use this file when the question is about how to ingest data from external databases, SFTP, UC Volumes, or custom source functions into Delta tables.

This guide covers all ingestion scenarios — external databases, SFTP, UC Volumes, and custom functions.

Use the thinner canonical docs when the question is really about contract or decision authority:
- [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for accepted metadata values and configuration rules
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for metadata authoring patterns and SQL structure
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for custom-function choice
- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for escalation from built-ins to custom code
- [INGESTION_PATTERNS_REFERENCE.md](INGESTION_PATTERNS_REFERENCE.md) for ingestion pattern selection

---

## Table of Contents

- [Ingest data from an external database](#ingest-data-from-an-external-database-eg-azure-sql-oracle-postgresql-into-a-delta-table)
- [Ingest data from SFTP](#ingest-data-from-sftp-into-a-delta-table)
- [Ingest file data from the UC Volumes section](#ingest-file-data-from-the-catalog-files-section-into-a-delta-table)
- [Custom Function Decision Guide](#custom-function-decision-guide)
- [Using a custom function to ingest files](#using-a-custom-function-to-ingest-files)
- [Using a custom staging function](#using-a-custom-staging-function)
- [Using a custom source function](#using-a-custom-source-function)
- [Advanced Topics: Ingesting Large Source Database Tables](#ingesting-large-source-database-tables)

---

## Ingest data from an external database (e.g. Azure SQL, Oracle, PostgreSQL) into a delta table

**Keywords:** database ingestion, Azure SQL, Oracle, SQL Server, PostgreSQL, MySQL, DB2, external database, connection ID, watermark, incremental load, full load, copy activity, staging, pipeline_stage_and_batch, source partitioning

### Overview
This pattern enables metadata-driven ingestion from external relational databases with support for incremental loading via watermark columns, parallel extraction, and automatic schema evolution.

### Prerequisites
1.  **(Optional) Set Up On-Premises Data Gateway**:
    *   Required for on-premises data sources (e.g., SQL Server, Oracle on your local network).
    *   Instructions: [Create an On-Premises Data Gateway](https://learn.microsoft.com/en-us/fabric/data-factory/how-to-access-on-premises-data#create-an-on-premises-data-gateway).

2.  **Create connection for your data source**:
    *   For on-premises or cloud databases like Oracle, DB2, SQL Server, PostgreSQL, or MySQL, create the corresponding connections.
    *   Navigate to [Manage connections and gateways](https://app.fabric.microsoft.com/groups/me/gateways?experience=power-bi) to create them. Use the *On-Premises* or *Cloud* connection option as appropriate.
    *   Retrieve the connection ID like [this](https://learn.microsoft.com/en-us/fabric/data-factory/data-source-management#method-1-from-the-manage-connections-and-gateways-page-on-microsoft-fabric-service).

3.  **Register the external source as a Datastore**:
    *   Add an entry under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json` with a `kind` discriminator and a `connection_details` object (see [FAQ: How do I find my connection ID?](FAQ.md#how-do-i-find-my-connection-id)).
    *   Use the logical `Datastore_Name` you register (e.g., `'oracle_sales'`) as the `datastore_name` value in your primary config — **not** raw credentials.
    *   `/fdp-04-commit` automatically MERGEs the entry into `Datastore_Configuration` on the metadata SQL warehouse via `sync_datastore_config.py`, so metadata SQL stays environment-agnostic: the same `datastore_name` resolves to different connection details per environment (DEV, QA, PROD).

### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below for your data source
    * **Supported database sources**: `azure_sql`, `sql_server`, `oracle`, `postgre_sql`, `my_sql`, `db2`
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

### Basic Configuration (Full Load)
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('MyDataProductTrigger', 1, 1, 'bronze', 'dbo.MySourceTable', 'CustomerID,OrderID', 'pipeline_stage_and_batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Supported sources: azure_sql, sql_server, oracle, postgre_sql, my_sql, db2
    (1, 'source_details', 'source', 'oracle'), 
    (1, 'source_details', 'datastore_name', 'oracle_sales'),
    -- REQUIRED for azure_sql and sql_server
    --('1', 'source_details', 'database_name', 'AdventureWorks'),
    (1, 'source_details', 'schema_name', 'SYSTEM'),
    (1, 'source_details', 'table_name', 'SALES'),
    -- Full table query for complete extraction
    (1, 'source_details', 'query', 'SELECT * FROM SYSTEM.SALES'),
    -- Staging location in UC Volumes area
    (1, 'source_details', 'staging_volume_name', 'bronze'),
    (1, 'source_details', 'staging_folder_path', 'oracle/Sales/')
```

### Incremental Load with Watermark (Recommended for Large Tables)
```sql
-- Orchestration Table (same as above)
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('MyDataProductTrigger', 1, 1, 'bronze', 'dbo.MySourceTable', 'CustomerID,OrderID', 'pipeline_stage_and_batch', 1)

-- Primary Config Table with Watermark
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'source', 'oracle'), 
    (1, 'source_details', 'datastore_name', 'oracle_sales'),
    (1, 'source_details', 'schema_name', 'SYSTEM'),
    (1, 'source_details', 'table_name', 'SALES'),
    -- Incremental query with {WATERMARKVALUE} placeholder
    -- Pipeline replaces {WATERMARKVALUE} with the last successful watermark from logging table
    -- Syntax varies by database type
    (1, 'source_details', 'query', 'SELECT * FROM SYSTEM.SALES WHERE TIME_ID > TIMESTAMP ''{WATERMARKVALUE}'' '),
    (1, 'source_details', 'staging_volume_name', 'bronze'),
    (1, 'source_details', 'staging_folder_path', 'oracle/Sales/'),
    -- Watermark column configuration
    (1, 'watermark_details', 'column_name', 'TIME_ID'),
    -- Data types: numeric or datetime
    (1, 'watermark_details', 'data_type', 'datetime')
```

### Advanced Configurations

**Parallel Extraction for Large Tables**
```sql
-- Enable source-side partitioning for faster extraction
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'partitioning_option', 'DynamicRange')
    -- See: https://learn.microsoft.com/en-us/fabric/data-factory/connector-oracle-database-copy-activity#parallel-copy-from-oracle-database
```

**Handle Non-UTF-8 Encodings**
```sql
-- Specify encoding if source data is not UTF-8
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'encoding', 'ISO-8859-1')
    -- See: https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text#dataset-properties
```

**Enforce NOT NULL Constraints**
```sql
-- Attempt to enforce NOT NULL constraints from source DB in target Delta table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'target_details', 'enforce_not_null', 'true')
    -- Default: false
```

### Database-Specific Query Examples

**SQL Server / Azure SQL**
```sql
(1, 'source_details', 'query', 'SELECT * FROM Sales.Orders WHERE ModifiedDate > ''{WATERMARKVALUE}''')
```

**Oracle**
```sql
(1, 'source_details', 'query', 'SELECT * FROM SYSTEM.SALES WHERE TIME_ID > TIMESTAMP ''{WATERMARKVALUE}''')
```

**PostgreSQL**
```sql
(1, 'source_details', 'query', 'SELECT * FROM public.orders WHERE modified_date > ''{WATERMARKVALUE}''')
```

**MySQL**
```sql
(1, 'source_details', 'query', 'SELECT * FROM orders WHERE modified_date > ''{WATERMARKVALUE}''')
```

**Oracle with Numeric Watermark (Sequence/ID Column)**
```sql
-- When using a numeric column like a sequence or auto-increment ID
(1, 'source_details', 'query', 'SELECT * FROM SYSTEM.SALES WHERE SALE_ID > {WATERMARKVALUE}'),
-- Note: No quotes around {WATERMARKVALUE} for numeric columns
(1, 'watermark_details', 'column_name', 'SALE_ID'),
(1, 'watermark_details', 'data_type', 'numeric')
```

---

## Ingest data from SFTP into a delta table

**Keywords:** SFTP, file transfer, binary copy, remote files, FTP, SFTP ingestion, SFTP connector, file staging, incremental file, modification timestamp

### Overview
This pattern enables metadata-driven ingestion of files from SFTP servers. The pipeline automatically:

1. Copies files **as binary** from the SFTP server to the staging volume
2. Reads the staged files using the appropriate file format (CSV, JSON, Parquet, etc.)
3. Loads the data into a Delta table in Bronze

All three steps happen automatically within a **single orchestration record** - just like database ingestion with `pipeline_stage_and_batch`. Supports incremental loading based on file modification timestamps automatically (no watermark configuration required).

### Prerequisites
1.  **Create connection for SFTP**:
    *   Navigate to [Manage connections and gateways](https://app.fabric.microsoft.com/groups/me/gateways?experience=power-bi)
    *   Create a new **SFTP** connection with your server credentials
    *   Retrieve the connection ID

2.  **Register the SFTP source as a Datastore**:
    *   Add an entry under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json` with `kind: "sftp"` and a `connection_details` object. See [FAQ: How do I find my connection ID?](FAQ.md#how-do-i-find-my-connection-id).
    *   Use the logical `Datastore_Name` you register (e.g., `'sftp_uploads'`) as the `datastore_name` value in your primary config. `/fdp-04-commit` syncs the entry into `Datastore_Configuration` automatically.

### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below for your SFTP source
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

### Basic Configuration (Full Load)
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('MySFTPTrigger', 1, 1, 'bronze', 'dbo.SFTPData', '', 'pipeline_stage_and_batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'source', 'sftp'),
    (1, 'source_details', 'datastore_name', 'sftp_uploads'),
    -- Folder path pattern on SFTP server (supports wildcards, no trailing slash)
    (1, 'source_details', 'sftp_wildcard_folder_path', '/uploads/data'),
    -- Optional: File name pattern (defaults to * for all files)
    (1, 'source_details', 'sftp_wildcard_file_name', '*.csv'),
    -- Staging location in UC Volumes area
    (1, 'source_details', 'staging_volume_name', 'bronze'),
    (1, 'source_details', 'staging_folder_path', 'sftp/incoming')
```

### Incremental Loading (Automatic)

SFTP ingestion **automatically supports incremental loading** - no additional configuration needed! The same configuration as Basic works for incremental:

```sql
-- Same configuration as Full Load - incremental is automatic!
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('MySFTPTrigger', 1, 1, 'bronze', 'dbo.SFTPData', '', 'pipeline_stage_and_batch', 1)

INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (1, 'source_details', 'source', 'sftp'),
    (1, 'source_details', 'datastore_name', 'sftp_uploads'),
    (1, 'source_details', 'sftp_wildcard_folder_path', '/uploads/data'),
    (1, 'source_details', 'sftp_wildcard_file_name', 'sales_*.csv'),
    (1, 'source_details', 'staging_volume_name', 'bronze'),
    (1, 'source_details', 'staging_folder_path', 'sftp/incoming')
    -- NO watermark_details needed! Incremental is automatic based on file modification time.
```

> **How Incremental SFTP Works (Automatic):**
> 1. On the **first run**, all matching files are copied (no watermark exists yet)
> 2. After successful ingestion, the pipeline **automatically logs the current timestamp** in the `Data_Pipeline_Metadata_Logging` table
> 3. On **subsequent runs**, the pipeline automatically uses this logged timestamp as the `modifiedDatetimeStart` filter
> 4. Only files with a **last modified time greater than the logged timestamp** are copied
> 5. After each successful run, the timestamp is automatically updated
>
> **No `watermark_details` configuration required!** This is different from database sources where you must specify a watermark column. SFTP uses the file's last modified time automatically.

### SFTP-Specific Configuration Options

| Configuration Name | Required | Description | Example |
|---|---|---|---|
| `sftp_wildcard_folder_path` | ✅ Yes | Folder path on SFTP server. Supports wildcards. No trailing slash. | `/uploads/data/2024*` |
| `sftp_wildcard_file_name` | ❌ No | File name pattern. Defaults to `*` (all files). | `*.csv`, `report_*.xlsx` |

---

## Ingest file data from the UC Volumes section into a delta table

**Keywords:** file ingestion, CSV, JSON, Excel, XML, Parquet, UC Volumes, external volume shortcut, schema inference, file format, delimiter, header, encoding, auto watermark, file modification timestamp

### Overview
This pattern enables ingestion of structured file data (CSV, JSON, Excel, XML, Parquet) from the Files section of a UC catalog. This can be files uploaded directly or files accessed via external volume shortcuts. The framework supports automatic watermarking based on file modification timestamps. The framework supports schema inference, custom schemas, and various file format options.

### Supported File Formats
- **CSV/TSV**: Delimited text files with configurable delimiters and headers
- **JSON**: Single-line or multi-line JSON documents
- **Excel**: `.xlsx` files with sheet selection
- **XML**: XML documents with XPath support
- **Parquet**: Columnar format (native support)

### Prerequisites
1.  Place your files in the `Files` section of a UC catalog. You can do this by:
    *   **Uploading files directly** to the volume.
    *   Creating a **external volume shortcut** to another workspace or external storage like ADLS Gen2, S3, or Google Cloud Storage.
        *   [external volume shortcut Instructions](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
        *   [ADLS Gen 2 Shortcut Instructions](https://docs.databricks.com/en/connect/unity-catalog/volumes.html#create-an-external-volume)
        *   [S3 Shortcut Instructions](https://docs.databricks.com/en/connect/unity-catalog/volumes.html#create-an-external-volume)
        *   [Google Cloud Storage Shortcut Instructions](https://docs.databricks.com/en/connect/unity-catalog/volumes.html#create-an-external-volume)
2.  Determine the folder path to your files within the volume `Files` directory.

### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below for your data source
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

### Basic CSV Configuration with Auto-Watermarking
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('FileData', 1, 45, 'silver', 'dbo.oracle_Sales2', 'Prod_Id, Cust_Id, Time_Id, Channel_Id, Promo_Id', 'batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Watermarking happens by default - only files with a later modified timestamp than last run will be ingested
    -- The wildcard folder path should be based on the path to your files in the volume `Files` directory.
    (45, 'source_details', 'wildcard_folder_path', 'oracle/Sales/*/*.csv'),
    -- Optional: Bronze is default, but if shortcut is in Silver or Gold, specify here
    (45, 'source_details', 'datastore_name', 'bronze')
```

### Excel File Configuration
```sql
-- Orchestration Table (same structure as CSV example above)

-- Primary Config Table for Excel
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (45, 'source_details', 'wildcard_folder_path', 'excel/sales/*.xlsx'),
    -- REQUIRED for Excel files. Provide a single sheet, comma-separated sheet list, or '*' for all sheets.
    (45, 'source_details', 'sheet_name', 'Sheet1,Sheet2'),
    -- Optional: true is default
    (45, 'source_details', 'file_has_header_row', 'true')
```

When multiple sheets are requested (including the `*` wildcard), the ingestion framework unions them together and appends a `sheet_name` column so downstream logic can identify each row's source sheet.

### JSON File Configuration
```sql
-- Primary Config Table for JSON
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (45, 'source_details', 'wildcard_folder_path', 'json/events/*.json'),
    -- Required for multi-line JSON (each record spans multiple lines)
    (45, 'source_details', 'multiline', 'true'),
    -- Optional: Define schema if needed
    (45, 'source_details', 'schema', 'event_id STRING, timestamp TIMESTAMP, user_id INT, event_type STRING')
```

### XML File Configuration
```sql
-- Primary Config Table for XML
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (45, 'source_details', 'wildcard_folder_path', 'xml/orders/*.xml'),
    -- Optional: XPath to project only desired nodes
    (45, 'source_details', 'xml_xpath', '//order/items/item'),
    -- Optional: XML namespaces (if XPath uses namespace-qualified elements)
    -- Must provide both keys and values with same number of entries
    (45, 'source_details', 'xml_namespaces_keys', 'ns1,ns2'),
    (45, 'source_details', 'xml_namespaces_values', 'http://schemas.example.com/ns1,http://schemas.example.com/ns2')
```

### Advanced CSV/TSV Configuration
```sql
-- Primary Config Table with advanced options
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (45, 'source_details', 'wildcard_folder_path', 'delimited/data/*.tsv'),
    -- Custom delimiter (tab-separated in this example)
    (45, 'source_details', 'delimiter', '\t'),
    -- Header row setting
    (45, 'source_details', 'file_has_header_row', 'true'),
    -- Define schema explicitly
    (45, 'source_details', 'schema', 'customer_id INT, name STRING, email STRING, signup_date DATE'),
    -- Handle bad records (drop rows that don't match schema)
    (45, 'source_details', 'on_bad_records', 'drop'),
    -- Alternative bad record handling: quarantine (stores in separate table) or fail (pipeline fails)
    -- (45, 'source_details', 'on_bad_records', 'quarantine'),
    -- (45, 'source_details', 'on_bad_records', 'fail'),
    -- Encoding override (default is UTF-8)
    (45, 'source_details', 'encoding', 'ISO-8859-1')
```

### Schema Definition Options
The `schema` configuration accepts Spark SQL data type definitions:

**Common Data Types:**
- **String**: `STRING` or `VARCHAR(n)`
- **Numeric**: `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL(precision, scale)`
- **Date/Time**: `DATE`, `TIMESTAMP`
- **Boolean**: `BOOLEAN`
- **Binary**: `BINARY`

**Example Schemas:**
```sql
-- Simple schema
(45, 'source_details', 'schema', 'id INT, name STRING, amount DECIMAL(10,2), created_date DATE')

-- Complex schema with nested types
(45, 'source_details', 'schema', 'id INT, name STRING, tags ARRAY<STRING>, metadata MAP<STRING,STRING>')
```

### Schema Enforcement Behavior by File Format

> ⚠️ **IMPORTANT: Schema Enforcement and Silent Data Loss Warning**
>
> Schema enforcement during file ingestion behaves differently depending on the file format:

**CSV/JSON Files (Schema During Read):**
- Schema is applied **during** file reading via Spark's `.schema()` method
- When Spark cannot cast a value to the expected type (e.g., `'ABC'` to `INT`), the value becomes **NULL without error**
- This can cause **silent data loss** if your data contains unexpected values
- The `on_bad_records` modes (`drop`, `quarantine`, `fail`) only catch **structural** CSV errors (wrong column count, unclosed quotes), NOT type casting failures

**Recommendation for CSV/JSON:**
1. **Ingest to Bronze WITHOUT schema** - CSV defaults to all strings, JSON infers types natively
2. Use **`change_data_types`** transformation step in Bronze→Silver to cast columns to desired types
3. If you must use schema enforcement during ingestion, be aware of potential silent NULL conversion

**Parquet Files (Post-Read Validation):**
- Parquet files are **self-describing** with embedded schema metadata
- Schema is **NOT** applied during read - Parquet's embedded schema is always used
- If a `schema` is provided, it is used for **post-read validation** via `validate_table_schema_contract`
- Schema mismatches result in **explicit exceptions** (NOT silent data loss)
- This is the safer approach: mismatches fail loudly rather than silently corrupting data

### Encoding Support
Specify encoding when files are not UTF-8:
```sql
-- Common encodings
(45, 'source_details', 'encoding', 'ISO-8859-1')  -- Latin-1
(45, 'source_details', 'encoding', 'windows-1252') -- Windows Western European
(45, 'source_details', 'encoding', 'UTF-16')       -- UTF-16
(45, 'source_details', 'encoding', 'GB2312')       -- Simplified Chinese
```

See [IANA encoding names](https://www.iana.org/assignments/character-sets/character-sets.xhtml) for full list of supported encodings.

### Error Handling for Schema Mismatches

> ⚠️ **IMPORTANT**: The `on_bad_records` options below apply to **CSV/JSON files only** and handle **structural** parsing errors (wrong column count, unclosed quotes, missing delimiters). They do **NOT** catch type casting failures where values silently become NULL.
>
> For **Parquet files**, schema mismatches are handled via post-read validation which raises explicit exceptions on any mismatch.

**on_bad_records Options (CSV/JSON only):**
- **`drop`**: Skip rows with structural parsing errors
- **`quarantine`**: Store structurally malformed rows in a separate quarantine table (`{target_table_name}_quarantined`)
- **`fail`**: Pipeline fails on first structural parsing error (default for strict validation)

```sql
-- Drop structurally malformed records and continue
(45, 'source_details', 'on_bad_records', 'drop')

-- Quarantine structurally malformed records for review
(45, 'source_details', 'on_bad_records', 'quarantine')
-- Optionally specify custom quarantine table name
('45', 'target_details', 'quarantine_table_name', 'dbo.sales_data_quarantined')
```

---

## Custom Function Decision Guide

> **Canonical reference:** The durable source for custom-function selection is now [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md). This section remains as a compatibility anchor during the migration.

Use [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for the full rules, decision tree, and durable comparison table. This guide stays intentionally short to keep the ingestion guide workflow-oriented instead of owning the canonical contract.

### Short Decision Tree

1. Can built-in metadata transformations solve it?
   If yes, stop. Do not use a custom function.
2. Does the custom logic run after ingestion on `new_data`?
   If yes, use `custom_transformation_function`.
3. Does the framework already have file paths and you only need custom parsing?
   If yes, use `custom_file_ingestion_function`.
4. Are you reading from an internal Delta table with custom logic?
   If yes, use `custom_table_ingestion_function`.
5. Are you reading from an external source?
   If yes, use `custom_staging_function` when the source is an API or returns raw payloads (JSON, XML, CSV) that should be landed as-is before parsing into Bronze (medallion best practice). Use `custom_source_function` when a DataFrame return is sufficient and raw file preservation is not needed.

### Quick-Reference Comparison

| If your need is... | Use this reference |
|---|---|
| Full custom-function rules and distinctions | [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) |
| Escalation from built-ins to custom code | [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) |
| Procedure for authoring metadata and code together | [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md) |

> **Template modules:** Each function type has a template in [`docs/resources/`](resources/) — [Custom_Source_Function.py](resources/Custom_Source_Function.py), [Custom_Staging_Function.py](resources/Custom_Staging_Function.py), [Custom_Table_Ingestion_Function.py](resources/Custom_Table_Ingestion_Function.py).

---

## Using a custom function to ingest files

**Keywords:** custom file ingestion, custom_file_ingestion_function, complex XML, proprietary format, multi-file aggregation, custom parsing, file reader

> 📌 **Not sure this is the right custom function?** See the [Custom Function Decision Guide](#custom-function-decision-guide) selection rules.

### Overview
When standard file readers don't meet your needs (e.g., complex XML parsing, proprietary formats, multi-file aggregation), you can implement custom Python functions to handle file ingestion logic.

### Use Cases
- **Complex XML structures** requiring custom parsing logic
- **Proprietary file formats** not natively supported
- **Multi-file aggregation** where data spans multiple files with custom merge logic
- **Complex transformations** during ingestion (e.g., data enrichment from external sources)

### Prerequisites
1.  Create a new PySpark notebook in either your feature workspace or your development workspace
2.  Create a function in the notebook that has the same parameters and return value as the below example function 
    *   Reference custom function example [here](resources/Custom_File_Ingestion_Function.py)

### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

### Example Configuration
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('FileData', 1, 100, 'bronze', 'dbo.oracle_Sales3', 'Prod_Id, Cust_Id, Time_Id, Channel_Id, Promo_Id', 'batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- wildcard_folder_path should reference where data is located within the UC Volumes
    (100, 'source_details', 'wildcard_folder_path', 'oracle/Sales/*/*.xml'),
    -- Optional: Bronze is default, but if file data is in Silver or Gold, specify here
    (100, 'source_details', 'datastore_name', 'bronze'),
    -- Name of the Python function to execute (without parentheses)
    (100, 'source_details', 'custom_file_ingestion_function', 'ingest_xml_sales_data'),
    -- Name of the notebook containing the function (without extension)
    (100, 'source_details', 'custom_file_ingestion_function_notebook', 'custom_ingest_sales_xml_data')
```

### Custom Function Requirements

Your custom ingestion function must follow specific parameter and return value signatures. 

**For detailed examples and full code implementations, see:**
- [Custom File Ingestion Function Example](resources/Custom_File_Ingestion_Function.py)

The example notebook includes:
- Complete function signature with all required parameters
- XML parsing implementation
- Error handling patterns
- Schema validation approaches
- Performance optimization tips

> **Important:** The `custom_source_function`, `custom_staging_function`, `custom_table_ingestion_function`, and `custom_file_ingestion_function` fields in `source_details` each accept exactly one function name per Table_ID (for ingestion). If you need to orchestrate multiple helpers, call them sequentially from inside that function.  
>  
> This limitation does **not** apply to `custom_transformation_function` transformation steps, which can accept multiple comma-separated function names (e.g., `'calculate_revenue_metrics,apply_business_rules'`). See [Using a custom function as a data transformation step](DATA_TRANSFORMATION_GUIDE.md#using-a-custom-function-as-a-data-transformation-step) for details.

### Best Practices
1. **Imports inside functions**: All `import` statements must be placed **inside your function body**, not at the top of the notebook. Custom notebooks execute via `run_cell()` in the shared IPython namespace — top-level imports pollute `globals()` and can shadow critical symbols like the `f` alias for `pyspark.sql.functions`. The CI/CD validation script enforces this rule. Your function can still call helper functions (`log_and_print`, `_get_datastore_config`, etc.) via Python's LEGB scoping.
2. **No wildcard imports**: Never use `from X import *` — use explicit imports (e.g., `from pyspark.sql.types import StructType, StructField, StringType`).
3. **Error Handling**: Include try-except blocks in your custom function
4. **Logging**: Use print statements for debugging (appears in pipeline logs)
5. **Schema Validation**: Validate DataFrame schema before returning
6. **Performance**: Leverage Spark transformations over pandas when possible for large datasets
7. **Testing**: Test your function independently before integrating with the pipeline

### Deployment: Dev vs Production Environments

Custom function notebooks use a **dual-path loading strategy** that allows developers to work in notebooks during development while using optimized `.py` files in production.

#### How It Works

| Environment | Loading Method | Source |
|-------------|---------------|--------|
| **Development** | Notebook API | Notebook in workspace (via `getDefinition()`) |
| **UAT/Production** | File read + run_cell | `.py` file in catalog `Files/custom_functions/` |

When `load_workspace_module_into_globals("custom_Products")` is called:
1. **First**, it checks for `/catalog/default/Files/custom_functions/custom_Products.py`
2. **If found**, reads the file and executes via `run_cell()` in the session namespace (fast, reliable)
3. **If not found**, falls back to notebook API (dev mode)

#### CI/CD Setup for Production Deployment

Custom function notebooks are deployed to the volume using `Deploy-CustomNotebooks.ps1`, which:
1. Scans metadata SQL files for referenced custom function notebooks
2. Locates matching notebooks in git via `.platform` displayName
3. Uses an MD5 manifest (`manifest.json`) to detect changed/new files
4. Uploads only changed notebooks to `Files/custom_functions/` (renamed from `notebook-content.py`)
5. Removes orphaned `.py` files no longer referenced in metadata

**Add to your Azure DevOps pipeline:**

```yaml
- task: AzurePowerShell@5
  displayName: 'Deploy Custom Notebooks'
  inputs:
    azureSubscription: '$(ServiceConnection)'
    ScriptType: 'FilePath'
    ScriptPath: 'automation_scripts/Deploy-CustomNotebooks.ps1'
    ScriptArguments: >
      -TargetWorkspaceId "$(WorkspaceId)"
      -GitFolderName "$(GitFolder)"
      -SourceDirectory "$(Build.SourcesDirectory)"
    azurePowerShellVersion: 'LatestVersion'
```

> 💡 **Note:** Notebooks in git are already valid Python files (markdown cells are comments). The deployment script simply copies and renames them - no parsing or conversion needed.

#### Benefits of This Approach

| Aspect | Development | Production |
|--------|-------------|------------|
| **Developer Experience** | Full notebook IDE with cell-by-cell execution | N/A |
| **Reliability** | Depends on API availability | Direct file load + session execution |
| **Performance** | API call + JSON parsing | Direct file load |
| **Version Control** | Notebooks in Git | Exported .py files deployed |
| **Debugging** | Interactive in notebook | Logs from pipeline |

#### Naming Convention

Keep notebook and module names consistent:
- Notebook: `custom_products.py`
- Exported file: `custom_Products.py`
- Metadata config: `'custom_Products'` (no extension)

This ensures your metadata configuration works identically in dev and production.

---

## Using a custom staging function

**Keywords:** custom staging, custom_staging_function, API ingestion, REST API, OAuth, web scraping, file-level staging, exit_after_staging, custom connector, external source

> 📌 **Not sure this is the right custom function?** See the [Custom Function Decision Guide](#custom-function-decision-guide) selection rules.

### Overview
Custom staging functions allow you to stage data into UC Volumes using custom Python/PySpark logic when the built-in Copy activity connectors (Azure SQL, Oracle, SFTP, etc.) don't meet your needs. Notebook-owned custom staging runs inside `batch_processing.py` with `Processing_Method='batch'`. If you want the notebook to stop after landing files, set `source_details.exit_after_staging='true'`. If you leave it as `false` or omit it, the notebook stages the files and then continues normal table processing.

### Use Cases
- **Custom API integrations** — **preferred approach for REST APIs** (e.g., paginated REST APIs with OAuth, GraphQL endpoints). Landing raw JSON/XML responses as-is follows medallion architecture best practice: preserve source fidelity in files, then parse into the Bronze Delta table. This enables reprocessing without re-calling the API.
- **Proprietary data sources** not supported by built-in connectors
- **Complex extraction logic** requiring multi-step API calls or data assembly
- **Web scraping** or screen scraping scenarios
- **IoT/streaming data** that needs custom buffering before staging

> **Trade-off vs pipeline `rest_api`:** The pipeline approach (`pipeline_stage_and_batch` with `source = 'rest_api'`) manages credentials through connections with no secrets in code and no Spark overhead during extraction — but offers limited flexibility for complex API patterns. The notebook approach (`custom_staging_function`) gives full Python control over authentication, pagination, response parsing, retries, and multi-step flows — but typically requires a managed private endpoint to Key Vault for credential management, which slows Spark startup times and development speed. Choose pipeline for straightforward REST APIs; choose notebook for complex or non-standard API integrations.

### Prerequisites
1. Create a new PySpark notebook with your custom staging function
2. The function must accept `(metadata, spark)` and return a dict with `rows_copied` and `next_watermark_value`
3. Ensure `batch_processing.py` is deployed and available in the target workspace

### Configuration Steps
1. Update/Enter the appropriate values in the SQL statements below
2. Execute the SQL in the Metadata Warehouse
3. Execute the `Trigger Step Orchestrator` pipeline with the inputted `Trigger_Name` value

### Example Configuration
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('CustomAPI', 1, 200, 'bronze', 'dbo.api_data', '', 'batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Required: staging volume and folder path
    (200, 'source_details', 'staging_volume_name', 'bronze'),
    (200, 'source_details', 'staging_folder_path', 'api_data/raw/'),
    -- Name of the Python function to execute
    (200, 'source_details', 'custom_staging_function', 'my_api_staging_function'),
    -- Name of the notebook containing the function
    (200, 'source_details', 'custom_staging_function_notebook', 'custom_api_staging'),
    -- Optional: stop after staging instead of continuing into table processing
    (200, 'source_details', 'exit_after_staging', 'true'),
    -- Optional: pass custom config values accessible via primary_config
    (200, 'source_details', 'custom_staging_function_api_endpoint', 'https://api.example.com/v2/data'),
    (200, 'source_details', 'custom_staging_function_page_size', '1000')
```

### Custom Function Requirements

Your custom staging function must follow this signature:

```python
def my_api_staging_function(metadata, spark):
    """
    Custom staging function.
    
    Args:
        metadata (dict): All pipeline parameters:
            - watermark_value (str): Previous watermark for incremental loads
            - primary_config (dict): All source_details_* attributes
            - staging_datastore_config (dict): Staging datastore configuration
            - source_datastore_config (dict): Source datastore configuration
            - orchestration_metadata (dict): Orchestration metadata (Table_ID, etc.)
            - target_folderpath_w_ingestion_type (str): Staging folder path with timestamp (no trailing slash)
        spark (SparkSession): Active Spark session
    
    Returns:
        dict: Must contain:
            - rows_copied (int|str): Number of records staged
            - next_watermark_value (str): Updated watermark for next run
    """
    # Access custom config values
    api_endpoint = metadata['primary_config'].get('source_details_custom_staging_function_api_endpoint', '')
    page_size = metadata['primary_config'].get('source_details_custom_staging_function_page_size', '100')
    target_folderpath = metadata['target_folderpath_w_ingestion_type']
    target_folder_abfss_path = f"abfss://.../Files/{target_folderpath}"
    
    # Your function owns file names and extensions inside the staged folder.
    # Append '/{file_name}' when writing a file under the folder path.
    # Write direct child files only. Do not create subfolders under target_folderpath.
    # For non-Spark file writing, mount the target path first:
    # mount_point, local_path, _, _ = _mount_abfss_path_for_local_access(target_abfss_path, table_id)
    
    # Your custom staging logic here...
    
    return {
        "rows_copied": 500,
        "next_watermark_value": "2024-01-17T10:30:00"
    }
```

> **Important:** The `custom_source_function`, `custom_staging_function`, `custom_table_ingestion_function`, and `custom_file_ingestion_function` fields in `source_details` each accept exactly one function name per Table_ID. If you need to orchestrate multiple helpers, call them sequentially from inside that function.

### How It Works (Pipeline Flow)

1. The calling pipeline invokes `batch_processing.py` because `Processing_Method='batch'`
2. `batch_processing.py` fetches metadata, gets the `Staging` watermark, builds the timestamped output folder from the orchestration `trigger_time`, clears that folder if it already exists from an earlier failed attempt in the same run, loads your function, and executes it
3. Your function stages data and returns `rows_copied` + `next_watermark_value`
4. `batch_processing.py` logs the custom staging step with `processing_phase='Staging'`; that staging-phase lineage is recorded as `External` / `Custom Staging Function`, with the staging volume logged as the target
5. If `source_details.exit_after_staging='true'`, `batch_processing.py` exits early with a staging result payload after logging completes
6. Otherwise, `batch_processing.py` reads the newly staged files and continues normal batch processing with `processing_phase='Batch'`

> **Important:** The framework provides the timestamped staging folder path via `target_folderpath_w_ingestion_type` without a trailing slash. The folder name stays in `YYYYMMDDHHMMSS` format and is derived from the orchestration `trigger_time` so retries within the same run reuse the same path. `trigger_time` is required for custom staging idempotency; if it is missing or invalid, the run fails rather than creating a new folder name. Before each custom staging attempt, the framework clears that exact folder so validation only sees files produced by the current attempt. Your custom staging function must write direct child files into that exact folder. Zip files are allowed. Subfolders are not supported. If the folder is empty after staging, the run exits gracefully as no new data.

---

## Using a custom source function

**Keywords:** custom source, custom_source_function, REST API, external database, SDK, Salesforce, SAP, DataFrame return, watermark, custom extraction

> 📌 **Not sure this is the right custom function?** See the [Custom Function Decision Guide](#custom-function-decision-guide) selection rules.

### Overview
Custom source functions allow you to ingest data from external sources (APIs, services, external systems) using custom Python/PySpark logic that returns a DataFrame directly. Unlike `custom_staging_function` (which writes files and returns a dict), `custom_source_function` returns the data as a DataFrame and lets the framework handle downstream table processing — DQ checks, merge, schema handling, and logging. The function manages its own extraction progress via the `watermark_value` passed in the metadata dict, and it must return `next_watermark_value` if you want the run to persist a new watermark.

### Use Cases
- **External databases** not reachable via built-in connectors
- **Third-party SDKs** (e.g., Salesforce, SAP, Dynamics) where a Python client returns tabular data
- **Custom CDC** from external systems where the function tracks its own high-water mark
- **Web services** that return small, already-structured tabular data without an intermediate file stage

> **For REST APIs returning JSON/XML:** Prefer `custom_staging_function` instead — land the raw API response as-is in UC Volumes, then let the framework parse and load it into Bronze. This follows medallion architecture best practice (land raw → parse to table) and preserves source fidelity for reprocessing. For simple REST APIs, also consider `pipeline_stage_and_batch` with the built-in `rest_api` source type in `External Data Staging_Copy` — credentials are managed via connections (no secrets in code, no Spark overhead) but with less flexibility for complex API patterns. See [Using a custom staging function](#using-a-custom-staging-function).

### Prerequisites
1. Create a new PySpark notebook with your custom source function
2. The function must accept `(metadata, spark)` and return either a DataFrame or a tuple of `(DataFrame, next_watermark_value)`
3. If the load is incremental and should advance a watermark, return `(DataFrame, next_watermark_value)` explicitly
3. Ensure `batch_processing.py` is deployed and available in the target workspace

### Configuration Steps
1. Update/Enter the appropriate values in the SQL statements below
2. Execute the SQL in the Metadata Warehouse
3. Execute the `Trigger Step Orchestrator` pipeline with the inputted `Trigger_Name` value

### Example Configuration
```sql
-- Orchestration Table
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('ExternalAPI', 1, 300, 'bronze', 'dbo.api_records', 'record_id', 'batch', 1)

-- Primary Config Table
INSERT INTO dbo.Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Name of the Python function to execute
    (300, 'source_details', 'custom_source_function', 'my_external_api_function'),
    -- Name of the notebook containing the function
    (300, 'source_details', 'custom_source_function_notebook', 'custom_external_api'),
    -- Optional: pass custom config values accessible via primary_config
    (300, 'source_details', 'custom_source_function_api_endpoint', 'https://api.example.com/v2/records'),
    (300, 'source_details', 'custom_source_function_page_size', '500')
```

> **Optional:** `source_details.datastore_name` is not required for `custom_source_function` when the function is fully self-contained and does not need datastore-backed connection metadata. If your function wants to resolve `Workspace_URL`, `SQL_Warehouse_ID`, `Catalog_Name`, or `Connection_Details` via `_get_datastore_config()`, provide a registered datastore alias.

### Custom Function Requirements

Your custom source function must follow this signature:

```python
def my_external_api_function(metadata, spark):
    """
    Custom source function — returns a DataFrame for framework processing.
    
    Args:
        metadata (dict): All pipeline parameters:
            - watermark_value (str): Previous watermark for incremental loads
            - primary_config (dict): All source_details_* attributes
            - datastore_config (list[dict]): All datastore configuration rows
            - advanced_config (dict): Advanced configuration JSON strings
            - event_payload (dict): Event trigger payload, when applicable
            - orchestration_metadata (dict): Orchestration metadata (Table_ID, etc.)
        spark (SparkSession): Active Spark session
    
    Returns:
        DataFrame: Data to be processed by the framework
        — OR —
        tuple: (DataFrame, next_watermark_value) to provide an explicit watermark
    """
    import requests

    # Access custom config values
    primary_config = metadata.get('primary_config', {})
    datastore_config = metadata.get('datastore_config', [])
    api_endpoint = primary_config.get('source_details_custom_source_function_api_endpoint', '')
    page_size = primary_config.get('source_details_custom_source_function_page_size', '100')
    watermark_value = metadata.get('watermark_value', '')

    # Optional: resolve datastore properties from datastore_config when needed
    # This is only necessary if your function actually needs a registered datastore alias.
    # source_datastore_name = primary_config.get('source_details_datastore_name', '')
    # source_connection_details = _get_datastore_config(datastore_config, source_datastore_name, 'Connection_Details')

    # Your custom extraction logic here...
    response = requests.get(api_endpoint, params={'since': watermark_value, 'limit': page_size})
    records = response.json().get('data', [])

    df = spark.createDataFrame(records)

    # Option A: Return just the DataFrame.
    # Use this for full-refresh or stateless pulls where no new watermark should be persisted.
    return df

    # Option B: Return tuple with explicit watermark for incremental processing
    # next_watermark = records[-1]['updated_at'] if records else watermark_value
    # return df, next_watermark
```

> **Important:** The `custom_source_function`, `custom_staging_function`, `custom_table_ingestion_function`, and `custom_file_ingestion_function` fields in `source_details` each accept exactly one function name per Table_ID. If you need to orchestrate multiple helpers, call them sequentially from inside that function.

### How It Works (Pipeline Flow)

1. The calling pipeline routes `Processing_Method='batch'` into `batch_processing.py`
2. `batch_processing.py` detects `custom_source_function` in source config and invokes `_ingest_with_custom_source_function()`
3. Your function receives the full metadata dict (including `watermark_value`) and returns a DataFrame (or DataFrame + next watermark)
4. The framework takes ownership of DQ checks, transformations, merge/overwrite, and logging
5. If your function returns `next_watermark_value`, that value is persisted for the run; if it returns only a DataFrame, no new watermark is recorded
6. No staging folder, no file I/O — the returned DataFrame flows directly into the standard batch pipeline

> **Important:** Unlike `custom_staging_function`, this function does NOT receive `target_folderpath_w_ingestion_type` and should NOT write files. It returns a DataFrame and the framework handles persistence. If you need file-level control over staging, use `custom_staging_function` instead.

---

# Advanced Topics

## Ingesting Large Source Database Tables

**Keywords:** large table, big data, partitioning, splitting, billions of rows, terabytes, ingestion failure, retry, control table, chunking

### Challenges in Large Data Ingestion

1.	Ingesting large volumes of data (e.g., hundreds of millions of rows or terabytes) in a single query is likely to fail.
2.	Failures in the middle of a large ingestion result in incomplete loads, requiring retries from the beginning.
3.	Extended ingestion time (hours/days) increases the likelihood of failure.

### Proposed Solution

To avoid ingestion failures:
1.	**Split Data into Smaller Chunks**:
    - Break down the source data into smaller, manageable portions based on the date/time column (created_date_time or modified_date_time).
    - Use multiple control table records, each targeting a specific time range (e.g., yearly chunks).
2.	**Dynamic querying with Watermarks**:
    - Combine watermark filters with additional conditions like YEAR(created_date_time) to create time-bound partitions.
3.	**Handling Production Data**:
    - Adjust control table records w.r.t partitions based on production data size to ensure the ingestion framework can handle it.

### Implementation Steps — Splitting Control Table Records
- For large tables, create multiple control table records for smaller time ranges. For example:
    ```sql
    WHERE watermark_column_name > '{WATERMARKVALUE}' AND YEAR(created_date_time) = 2012
    WHERE watermark_column_name > '{WATERMARKVALUE}' AND YEAR(created_date_time) = 2013 
    ... 
    WHERE watermark_column_name > '{WATERMARKVALUE}' AND YEAR(created_date_time) >= 2024
    ```

- Each record processes one year (or other suitable time range) of data.
For the final control table record of that particular table, as shown in the above example attach an '>=' for the partitioning filter.
