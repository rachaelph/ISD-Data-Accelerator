# Metadata Warehouse Reference

Use this reference when the active investigation needs full warehouse-table relationships, canonical column names, stored procedures, or documentation cross-links.

## Full Join Rules

- `Data_Pipeline_Logs` row identity is the composite of `Log_ID`, `Processing_Phase`, and `Ingestion_Status`.
- A logical execution may write a `Staging` row, a `Batch` row, or both under the same `Log_ID`.
- Filter to `Processing_Phase = 'Batch'` only when you explicitly want the terminal outcome; otherwise preserve `Processing_Phase` in the result so staging behavior stays visible.
- `Data_Pipeline_Logs.Log_ID` -> `Data_Quality_Notifications.Log_ID`
- `Data_Pipeline_Logs.Log_ID` -> `Activity_Run_Logs.Log_ID`
- `Activity_Run_Logs.Table_ID` enables independent lookup when `Data_Pipeline_Logs` has no matching row (e.g., orphaned entries from early pipeline failures)
- `Table_ID` is the universal key across metadata and logging tables
- `Trigger_Name` links orchestration, run logs, and lineage
- `Orchestration.Target_Datastore` -> `Datastore_Configuration.Datastore_Name`
- `Date_Key` joins to `Date_Dimension`

### Notebook log availability

- `Activity_Run_Logs` is populated by both batch notebook processing and pipeline error logging.
- Expect the most useful structured step logs when `Processing_Method = 'batch'` and the run executed the notebook-based processing flow.
- Non-batch or pipeline-only runs can also have `Activity_Run_Logs` rows with `Source_Type = 'Pipeline'`.

## Metadata Table Relationships

- `Data_Pipeline_Metadata_Orchestration.Table_ID` is the parent row for a table's configuration.
- `Data_Pipeline_Metadata_Primary_Configuration` is a key-value store.
- `Data_Pipeline_Metadata_Advanced_Configuration` is a multi-attribute store with instance numbers.

## Canonical `Data_Pipeline_Logs` Columns

Use these canonical names in run-history and diagnostics queries:

- `Log_ID`
- `Table_ID`
- `Target_Entity`
- `Ingestion_Start_Time`
- `Ingestion_End_Time`
- `Ingestion_Status`
- `Processing_Phase`
- `Records_Processed`
- `Quarantined_Records`
- `Watermark_Value`
- `Trigger_Name`
- `Trigger_Step`
- `Trigger_Execution_ID`
- `Trigger_Execution_Start_Time`
- `Date_Key`

### Quick validation query

```sql
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'Data_Pipeline_Logs'
ORDER BY ORDINAL_POSITION;
```

## Dynamic Query Composition

### Common filters

| User says | SQL pattern |
|-----------|-------------|
| table X / Table_ID Y | `WHERE Table_ID = {value}` |
| last 24 hours | `WHERE Ingestion_Start_Time >= DATEADD(HOUR, -24, GETUTCDATE())` |
| last N days | `WHERE Date_Key >= CAST(FORMAT(DATEADD(DAY, -N, GETUTCDATE()), 'yyyyMMdd') AS INT)` |
| for trigger X | `WHERE Trigger_Name = '{value}'` |

### Common aggregations

| User says | SQL pattern |
|-----------|-------------|
| how many runs | `COUNT(*) ... GROUP BY Table_ID` |
| average duration | `AVG(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time))` |
| total records | `SUM(Records_Processed)` |
| failure rate | `SUM(CASE WHEN Ingestion_Status = 'Failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` |

## Relevant Stored Procedures

| Stored procedure | What it returns |
|------------------|-----------------|
| `dbo.Get_Last_Run_Status` | Latest batch run status for a `Table_ID` |
| `dbo.Get_Schema_Details` | Latest schema payload and `Schema_ID` |
| `dbo.Get_Watermark_Value` | Current watermark value |
| `dbo.FIFO_Status` | In-progress-run check |
| `dbo.Get_Pipeline_Tables` | Orchestration summary by trigger |
| `dbo.Pivot_Primary_Config` | Wide primary-config row |
| `dbo.Get_Advanced_Metadata` | Advanced-config JSON |
| `dbo.Get_Datastore_Details` | Datastore endpoint resolution |
| `dbo.Get_Exploratory_Analysis_Input` | EDA scheduling inputs |

## Notebooks That Populate the Logging Tables

| Table | Written by |
|-------|------------|
| `Data_Pipeline_Logs` | `NB_Helper_Functions_3` |
| `Activity_Run_Logs` | `NB_Helper_Functions_3`, `PL_01b_For_Each_Entity` |
| `Data_Quality_Notifications` | `NB_Helper_Functions_3` |
| `Schema_Logs` | `NB_Helper_Functions_3` |
| `Schema_Changes` | `NB_Helper_Functions_3` |
| `Data_Pipeline_Lineage` | `NB_Generate_Lineage` |
| `Exploratory_Data_Analysis_Results` | `NB_Run_Exploratory_Data_Analysis` |

## Manual SQL Guidance

Prefer `../scripts/invoke_metadata_query.py` for standard metadata-warehouse investigations.

For exact post-run validation after `/fdp-05-run`, prefer these query types before writing custom SQL:

- `LatestRunWithDQByTable` — returns the selected latest run row for a `Table_ID` plus correlated DQ rows for that row's `Log_ID`
- `LatestRunWithDQByTrigger` — returns the latest trigger execution rows for a `Trigger_Name` plus correlated DQ rows for the participating `Log_ID` values; optionally filter to `--table-ids`
- `RunOutcomeWithDQ` — returns run rows plus correlated DQ rows for a `Trigger_Execution_ID`
- `RunByExecution` — returns the exact `Data_Pipeline_Logs` rows for a `Trigger_Execution_ID`
- `DQByLogId` — returns the exact `Data_Quality_Notifications` rows for a correlated `Log_ID`
- `ActivityLogsByExecution` — returns exact run rows plus correlated `Activity_Run_Logs` rows for a `Trigger_Execution_ID`; when `Data_Pipeline_Logs` has no rows and `--table-id` is provided, falls back to querying `Activity_Run_Logs` directly by `Table_ID`
- `ActivityLogsByLogId` — returns the exact `Activity_Run_Logs` rows for a correlated `Log_ID`
- `ActivityLogsByTableId` — returns recent `Activity_Run_Logs` rows for a `Table_ID` within the `--hours` window (default 24h); use when no `Log_ID` or `Trigger_Execution_ID` is available

When you write manual SQL, use only the canonical column names listed in this document and validate them against `INFORMATION_SCHEMA.COLUMNS` before you rely on them.

## Documentation Cross-References

- `docs/FAQ.md` for monitoring, logging, lineage, and datastore questions
- `docs/DATA_TRANSFORMATION_GUIDE.md` for DQ checks and schema handling
- `docs/CORE_COMPONENTS_REFERENCE.md` for function catalogs and metadata population
- `docs/Restarting_Pipelines_From_Failure.md` for failure restart workflows
- `.github/skills/metadata-validation/SKILL.md` for metadata validation rules
- `automation_scripts/Deploy-MetadataRecords.ps1` for metadata deployment behavior
