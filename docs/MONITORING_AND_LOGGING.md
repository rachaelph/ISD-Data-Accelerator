# Monitoring, Logging, and Pre-Built Reports

> Last updated: 2026-03-29

Use this file when the question is about log tables, monitoring pipeline execution, data quality notifications, schema tracking, lineage, exploratory data analysis, or pre-built Power BI reports.

This guide covers the application logging framework, pipeline/DQ/schema/lineage log tables, and pre-built Power BI reports.

---

## Table of Contents

- [Application Logging Framework](#application-logging-framework)
- [Data Pipeline Logs](#data-pipeline-logs)
- [Notebook Run Logs](#notebook-run-logs)
- [Data Quality Notifications](#data-quality-notifications)
- [Schema Logs](#schema-logs)
- [Schema Changes](#schema-changes)
- [Exploratory Data Analysis Results](#exploratory-data-analysis-results)
- [Data Pipeline Lineage](#data-pipeline-lineage)
- [Pre-Built Reports](#pre-built-reports)

---

# Monitoring and Logging

## Application Logging Framework

The solution implements a comprehensive logging framework that integrates with Databricks's diagnostic capabilities and provides structured logging for all data processing operations.

### Logging Architecture

The logging framework is built on Apache Log4j and integrates with Databricks's native spark logging for centralized log collection and analysis. Key components include:

#### Logger Initialization
```python
# Initialize Log4j logger for platform integration
logger = sc._jvm.org.apache.log4j.LogManager.getLogger("spark_application_logger")
```

This creates a logger instance that:
- Integrates with Databricks's diagnostic emitters
- Enables log forwarding to Azure Log Analytics
- Provides structured logging with different severity levels
- Supports custom application-specific log entries

#### Log and Print Function
The solution provides a standardized `log_and_print()` function for consistent logging across all notebooks:

```python
def log_and_print(message: str, level: str = "info"):
    """
    Reusable function to log and print messages with different severity levels.
    
    Args:
        message (str): The message to log and print
        level (str): The logging level - "info", "warn", or "error"
    """
```

**Supported Log Levels:**
- `info`: General informational messages about processing steps
- `warn`: Warning messages for non-critical issues that don't stop processing
- `error`: Error messages for critical failures

**Key Features:**
- **Dual Output**: Messages are both logged to the diagnostic system and printed to console
- **Level Validation**: Ensures only valid log levels are used
- **Formatted Output**: Consistent message formatting with level indicators
- **Integration Ready**: Automatically forwards to Log Analytics when configured

### Log Integration with Databricks

The logging framework leverages Databricks's diagnostic emitters as documented in the [official Fabric documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/azure-fabric-diagnostic-emitters-log-analytics#write-custom-application-logs).

#### Benefits of Databricks Integration:
1. **Centralized Collection**: All logs are collected in a single Log Analytics workspace
2. **Query Capabilities**: Use KQL (Kusto Query Language) to analyze logs
3. **Alerting**: Set up automated alerts based on log patterns
4. **Dashboard Integration**: Incorporate log data into Power BI dashboards
5. **Correlation**: Correlate application logs with Databricks system logs

### Usage Patterns

#### Configuration and Status Logging
The framework extensively logs configuration parsing and processing status:

```python
medallion_log_info = (
    f"The target_datastore_medallion_name is {target_datastore_medallion_name}. "
    f"The default merge type is now set to '{default_merge_type}' and default watermark column is now set to '{default_watermark_column_name}'."
)
log_and_print(medallion_log_info)
```

#### Processing Step Documentation
Each major processing step is logged for tracking and debugging:

```python
target_abfss_path_log_info = f"Target ABFSS Path: {target_abfss_path}"
log_and_print(target_abfss_path_log_info)
```

#### Error and Warning Handling
Critical issues and warnings are logged with appropriate severity levels:

```python
log_and_print("Data quality validation failed", "error")
log_and_print("Schema change detected", "warn")
```

### Log Analysis and Monitoring

#### Log Analytics Queries
When integrated with Log Analytics, you can query application logs using KQL:

```kusto
// Query for all error logs in the last 24 hours
SparkApplicationEvents_CL
| where TimeGenerated > ago(24h)
| where Level_s == "ERROR"
| project TimeGenerated, Message_s, Table_ID_s
| order by TimeGenerated desc
```

#### Common Log Monitoring Scenarios
1. **Processing Status**: Track data movement completion and failures
2. **Configuration Issues**: Identify metadata configuration problems
3. **Performance Monitoring**: Analyze processing times and resource usage
4. **Data Quality**: Monitor data quality rule violations and warnings
5. **Schema Evolution**: Track schema changes across data sources

### Best Practices for Application Logging

1. **Consistent Usage**: Always use `log_and_print()` instead of standalone `print()` statements
2. **Appropriate Levels**: Use correct log levels based on message severity
3. **Meaningful Messages**: Include relevant context like Table_ID, processing step, and values
4. **Structured Information**: Include key identifiers that can be used for log correlation
5. **Error Context**: When logging errors, include enough context for troubleshooting

### Integration with Pipeline Logging

The application logging framework complements the structured pipeline logging stored in the Metadata Warehouse:

- **Application Logs** (via `log_and_print()`): Detailed step-by-step processing information for debugging within the workspace's Log Analytics
- **Pipeline Logs** (via `Data_Pipeline_Logs` table): High-level processing metrics and status for monitoring dashboards
- **Activity Run Logs** (via `Activity_Run_Logs` table): Structured step-by-step messages persisted for notebook-based and pipeline-based runs
- **Data Quality Logs** (via `Data_Quality_Notifications` table): Specific data quality violations and remediation actions

> **Note:** For batch processing, `batch_processing.py` writes directly to `Data_Pipeline_Logs` and `Schema_Logs`, and appends structured step-level messages straight to the Delta-backed `Activity_Run_Logs` table (using `log_data_movement()`, `log_new_schema()`, and `persist_run_log_entries()`). For non-batch processing methods routed through `Non-Framework Method Executor`, logging is performed by pipeline Script activities calling the `Log_Data_Movement` stored procedure.

Together, these provide comprehensive observability across the entire data processing pipeline.

## Data Pipeline Logs

**Keywords:** Data_Pipeline_Logs, Log ID, ingestion status, started, processed, failed, watermark value, records processed, quarantined records, Spark Monitor URL, trigger execution, pipeline logs, run history

The Data Pipeline Logs table includes details about how data was moved from one zone to another

- **Log ID**: Unique identifier for data movement. Values are only inserted into the logging table, so each Log ID has two rows. One row for when ingestion started. One row for when ingestion succeeded or failed
- **Table ID**: Unique identifier from the Orchestration Metadata table.
- **Source Medallion Layer**: Medallion layer of the source (Bronze/Silver/Gold)
- **Source Type**: Type of source (catalog/Warehouse)
- **Data Source Details**: Where data came from and/or how data was queried
- **Target Medallion Layer**: Medallion layer of the target (Bronze/Silver/Gold)
- **Target Type**: Type of target (catalog/Warehouse)
- **Target Datastore**: catalog or Warehouse Name (e.g. Bronze/Silver/Gold)
- **Target Entity**: Target table
- **Ingestion Start Time**: When data movement started
- **Ingestion End Time**: When data movement ended
- **Ingestion Status**: Started/Processed/Failed
- **Processing Phase**: Staging (extract to files) or Batch (load to tables)
- **Watermark Value**: Value used to extract only new data
- **Records Processed**: How many records/rows were moved to the target table or folder path
- **Quarantined Records**: How many records/rows were quarantined based on data quality rules
- **Spark Monitor URL**: URL to view the data pipeline run details in Databricks
- **Trigger Name**: Trigger Name from Orchestration Metadata Table
- **Trigger Step**: Order of Operations value from Orchestration Metadata Table
- **Trigger Execution ID**: GUID created by Fabric for ID of trigger invocation
- **Trigger Execution Start Time**: When the trigger execution started
- **Date Key**: Used for reporting 

### Primary Keys
- Log ID, Processing Phase, Status

## Activity Run Logs

The Activity Run Logs table stores structured, step-level messages for notebook-based and pipeline-based processing runs.

- **Table ID**: The entity's `Table_ID` from Orchestration metadata — enables independent lookup when `Data_Pipeline_Logs` has no matching row (e.g., orphaned entries from early pipeline failures)
- **Log ID**: Foreign-key-style correlation value shared with `Data_Pipeline_Logs`
- **Sequence Number**: Ordering of messages within the run
- **Log Timestamp**: UTC timestamp for the individual log entry
- **Log Level**: Severity (`INFO`, `WARNING`, `ERROR`)
- **Step Name**: Processing phase name — for notebook logs this maps to the notebook cell name (e.g., `Connect & Fetch Metadata`); for pipeline logs this is the failed activity name
- **Step Number**: Processing phase identifier — corresponds to the notebook cell number for notebook logs; always `1` for pipeline-originated entries
- **Message**: Step-level message text emitted through `log_and_print()`
- **Source Type**: Origin of the log entry (`Notebook`, `Pipeline`)

Entries are written via two stored procedures: `Insert_Activity_Log_Entries` (plural) accepts a JSON array of log entries and is used by notebooks to flush buffered messages in bulk, while `Insert_Activity_Log_Entry` (singular) accepts raw scalar parameters and is used by pipelines to log a single error without JSON serialization. Batch notebook runs accumulate messages in memory and flush them at the terminal status transition (including graceful no-data exits). Pipelines write single error entries directly when a downstream activity fails — for example, `Entity Router` logs the error message from the failed activity. Use the `Source_Type` column to distinguish origin. This table complements `Data_Pipeline_Logs`: use `Data_Pipeline_Logs` for high-level run status and `Activity_Run_Logs` for detailed troubleshooting.

## Data Quality Notifications

The Data Quality Notifications table includes all warning or failure messages related to schema changes or data quality rules defined in the Advanced Config metadata table.

- **Log ID**: Unique identifier for data movement. 
- **Datastore Name**: catalog or Warehouse Name (Bronze/Silver/Gold)
- **Table ID**: Unique identifier from the Orchestration Metadata table.
- **Table Name**: Target table name
- **Data Quality Category**: Data quality category (e.g., Validate Condition, Duplicate Primary Keys, Master Table Lookup)
- **Data Quality Result**: Failure or Warning
- **Data Quality Message**: Detailed message about data quality notification
- **Rows Impacted**: Number of rows that triggered this notification
- **Data Quarantined**: Whether data was quarantined from data quality rule (Yes/No)
- **Rows Quarantined**: Number of rows moved to quarantine table
- **Ingestion Start Time**: When data movement started
- **Ingestion End Time**: When data movement ended
- **Spark Monitor URL**: URL to view the data pipeline run details in Databricks
- **Date Key**: Used for reporting
  
### Primary Keys
- Log ID, Data Quality Message

## Schema Logs

This table stores unique schemas for all delta tables.
- **Table ID**: Unique identifier from the Orchestration Metadata table.
- **Datastore Name**: catalog or Warehouse name
- **Table Name**: Target table name
- **Schema ID**: MD5 hash value of the PySpark schema
- **Schema Details**: PySpark Schema Representation
- **Schema Arrival Time**: When schema was processed
- **Spark Monitor URL**: URL to view the data pipeline run details in Databricks
- **Date Key**: Used for reporting

### Primary Keys
- Table ID, Schema ID

## Schema Changes

This table stores schema changes detected when processing data.
- **Table ID**: Unique identifier from the Orchestration Metadata table.
- **Datastore Name**: catalog or Warehouse name
- **Table Name**: Target table name
- **Change Type**: Type of schema change (e.g., added, removed, modified)
- **Column Name**: Name of the column that changed
- **Data Type Details**: Data type information for the changed column
- **Schema Arrival Time**: When the schema change was detected
- **Spark Monitor URL**: URL to view the data pipeline run details in Databricks
- **Date Key**: Used for reporting

### Primary Keys
- Table ID, Column Name, Schema Arrival Time

## Exploratory Data Analysis Results

This table stores the results of exploratory data analysis (EDA) runs on tables.
- **Table ID**: Unique identifier from the Orchestration Metadata table.
- **Datastore Name**: catalog or Warehouse name
- **Target Type**: Type of target (catalog/Warehouse)
- **Target Medallion Layer**: Medallion layer of the target (Bronze/Silver/Gold)
- **Table Name**: Full table name
- **Table Last Modified Time**: When the table was last modified
- **Column Name**: Name of the column being profiled
- **Data Type**: Data type of the column
- **Total Rows**: Total number of rows in the table
- **Total Columns**: Total number of columns in the table
- **Approx Distinct Values**: Approximate count of distinct values in the column
- **Null Count**: Number of null values in the column
- **Null Percent**: Percentage of null values in the column
- **Mean**: Mean value (for numeric columns)
- **Std Dev**: Standard deviation (for numeric columns)
- **Min**: Minimum value
- **Max**: Maximum value
- **Data Profile Execution Time**: When the EDA was run
- **Date Key**: Used for reporting

### Primary Keys
- Table ID, Column Name, Data Profile Execution Time

## Data Pipeline Lineage

**Keywords:** lineage, data lineage, generate_lineage.py, NetworkX, directed graph, source to target, data flow, lineage report, Data_Pipeline_Lineage, lineage generation

This table stores data lineage information generated by the `generate_lineage.py`. The notebook builds a directed graph of data flows across the medallion architecture using NetworkX, tracking how data moves from external sources through Bronze → Silver → Gold layers.

### Lineage Generation Notebook (generate_lineage.py)

The lineage notebook analyzes metadata configuration tables and execution logs to automatically generate comprehensive data lineage records.

#### Key Features

| Feature | Description |
|---------|-------------|
| **Graph-Based Analysis** | Uses NetworkX directed graphs to model data flows between entities |
| **Medallion Layer Tracking** | Automatically derives Bronze/Silver/Gold layer from datastore names |
| **Multi-Source Detection** | Captures direct sources, joins, dimension lookups, and union sources |
| **Custom Notebook Parsing** | Extracts table references from custom ingestion notebooks using regex patterns |
| **Cross-Trigger Lineage** | Detects and flags data flows that span multiple triggers |
| **Change Detection** | Computes fingerprints to detect lineage structure changes between versions |
| **AI-Ready Narratives** | Generates human-readable summaries optimized for Power BI Copilot |

#### Notebook Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `trigger_name` | string | None | Filter lineage output to a specific trigger (graph still includes all triggers for accurate depth calculation) |
| `execution_id` | string | None | Filter to a specific execution ID |
| `persist_to_table` | bool | True | Whether to write results to the Data_Pipeline_Lineage table |

#### Lineage Sources Captured

The notebook extracts lineage from multiple sources in the metadata configuration:

1. **Direct Sources**: Primary data source from `Data_Pipeline_Metadata_Primary_Configuration`
2. **Join Sources**: Tables referenced in `join_data` transformation steps
3. **Dimension Lookups**: Tables referenced in `attach_dimension_surrogate_key` transformations
4. **Union Sources**: Tables referenced in `union_data` transformation steps
5. **Custom Notebook Sources**: Tables/files referenced in custom ingestion notebooks (parsed via regex)

#### Key Functions

| Function | Purpose |
|----------|---------|
| `build_lineage_graph()` | Builds NetworkX DiGraph from metadata tables |
| `generate_lineage_records()` | Creates lineage records by combining graph structure with execution logs |
| `calculate_lineage_depth_and_path()` | Computes hop count and full path from root sources |
| `generate_target_lineage_narrative()` | Creates consolidated human-readable narrative per target entity |
| `detect_table_level_changes()` | Identifies which tables have changed, added, or removed lineage |
| `compute_lineage_fingerprint()` | Generates MD5 hash of structural lineage for change detection |

### Table Schema

- **Lineage_ID**: Unique identifier (UUID) for each lineage record
- **Trigger_Name**: The trigger associated with this data flow
- **Table_ID**: Reference to the Orchestration Metadata table
- **Source_Entity**: Name of the source table/file
- **Source_Datastore**: catalog or external system name for source
- **Source_Medallion_Layer**: Medallion layer of source (External/Bronze/Silver/Gold)
- **Source_Type**: Type of source from Data_Pipeline_Logs
- **Target_Entity**: Name of the target table
- **Target_Datastore**: catalog or Warehouse name for target
- **Target_Medallion_Layer**: Medallion layer of target (Bronze/Silver/Gold)
- **Target_Type**: Type of target from Data_Pipeline_Logs
- **Relationship_Type**: Type of data relationship (direct, join, dimension_lookup, union, custom_transformation_function)
- **Transformation_Applied**: Transformations applied in this data flow
- **Lineage_Depth**: Number of hops from the root/origin source
- **Lineage_Path**: Full path showing data flow (e.g., "Oracle → bronze.orders → silver.orders → gold.fact_sales")
- **Is_Cross_Trigger**: Boolean indicating if this lineage spans multiple triggers
- **Cross_Trigger_Dependencies**: Comma-separated list of triggers involved in cross-trigger lineage
- **Lineage_Summary**: Human-readable summary of this specific data flow edge
- **Source_Lineage_Narrative**: Consolidated narrative describing the source entity's lineage
- **Target_Lineage_Narrative**: Consolidated narrative describing the target entity's full lineage (optimized for Power BI Copilot)
- **Lineage_Generated_At**: Timestamp when lineage was generated
- **Lineage_Version**: Version number (incremented only when lineage structure changes)
- **Date_Key**: Used for reporting

### Primary Keys
- Lineage_ID

### Example Narratives

The notebook generates AI-ready narratives for each target entity. Example:

> "gold.dbo.fact_sales is a Gold layer table. It originates from external source Oracle:SALES.ORDERS. Data flows through 3 hops: Oracle:SALES.ORDERS → bronze.dbo.orders → silver.dbo.orders → gold.dbo.fact_sales. It enriches data by looking up keys from dimension tables: gold.dbo.dim_customer (from Oracle:CUSTOMERS), gold.dbo.dim_product (from Oracle:PRODUCTS). Transformations applied: attach_dimension_surrogate_key, cleanse_data, derived_column."

---

# Pre-Built Reports

**Keywords:** Power BI, reports, dashboard, daily overview, monthly overview, data quality report, data movement report, exploratory data analysis report, lineage report, semantic model, KPI, monitoring report

The solution includes a suite of ready-to-use Power BI reports designed to provide comprehensive monitoring of the data ingestion process. These reports offer clear visibility into key performance indicators (KPIs) and operational metrics.

These reports are part of the **data engineering lifecycle** and should stay in the data engineering repo alongside the pipelines, notebooks, metadata, and deployment assets they monitor. Use a separate reporting repo for consumer-facing semantic models and reports.

These reports are fully interactive, allowing users to filter and slice data by dimensions. They are also easily extendable, enabling customization to meet evolving business requirements.

By leveraging these pre-built Power BI reports, stakeholders can efficiently monitor and maintain the health of the data ingestion pipelines, ensuring data accuracy, consistency, and reliability across the system.

The following picture shows the semantic model and the report as they appear in the Databricks workspace:

## Data Movement

Provides a daily and monthly overview of all data movement and data quality notifications.

### Report for data pipeline daily overview

### Report for data pipeline monthly overview

### Report for data quality daily overview

### Report for data quality monthly overview

## Exploratory Data Analysis

Provides data profiling for exploratory data analysis for all Silver and Gold tables.

### Report for data lake overview

### Report for catalog overview

### Report for table overview

### Report for tracking schema evolution

## Data Lineage

Visualizes data lineage and tracks source-to-target relationships across your data pipelines. This report is powered by the `generate_lineage.py` notebook which analyzes metadata tables and execution logs to build a comprehensive lineage graph using NetworkX.

### Key Features
- **Lineage Overview**: AI-generated narrative summaries explaining the complete data flow from source systems to target tables
- **Table Overview**: Detailed breakdown of transformations and data quality checks applied to each entity
- **Medallion Layer Tracking**: Visualize data flow across Bronze → Silver → Gold layers
- **External Source Identification**: Track data origins from Oracle, SQL Server, PostgreSQL, files, and other external systems
- **Impact Analysis**: Understand upstream and downstream dependencies for change management
- **Copilot Integration**: Natural language summaries powered by Power BI Copilot for easy understanding

### Generating Lineage Data

To populate the lineage report, run the `generate_lineage.py` notebook:
1. Navigate to your Databricks workspace
2. Open the `generate_lineage.py` notebook
3. Configure parameters (optional):
   - `trigger_name`: Filter to a specific trigger (default: all triggers)
   - `execution_id`: Filter to a specific pipeline run (default: all runs)
   - `persist_to_table`: Set to `True` to save lineage to `Data_Pipeline_Lineage` table
4. Run all cells to generate and persist lineage data

If a table shows zero lineage rows during investigation, run `generate_lineage.py` to generate lineage data, then re-run the lineage query or refresh the report.

### Report Pages

#### Lineage Overview
Provides a high-level AI-generated narrative of data lineage for a selected target table, including:
- Target table identification and medallion layer
- External source origin
- Complete data flow path using arrows (Source → Bronze → Silver → Gold)
- Join logic with join types and origins
- Transformations applied (derived_column, join_data, etc.)

#### Table Overview
Detailed view of a selected table's configuration:
- **Transformations**: Summarizes each `data_transformation_steps` configuration in execution order
- **Data Quality Checks**: Summarizes each `data_quality` configuration with plain language explanations
- Primary and advanced configuration attributes

