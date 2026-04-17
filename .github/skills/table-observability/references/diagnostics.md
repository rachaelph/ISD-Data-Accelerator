# Cross-Domain Diagnostics — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to a CD-* template.

## CD-1: Full Health Check (Run + DQ + Schema + Profile)

> **Execution:** Run each part as a separate terminal call when you want incremental feedback. CD-1a + CD-1b + CD-1c + CD-1d may be batched into one terminal call when optimizing for speed because they are independent queries.
>
> **When to use:** User asks "give me the full health check for table X" or "what's the overall status?"

### CD-1a: Latest Run Status

```sql
-- Part A: Latest run status for Table_ID = {Table_ID}
SELECT TOP 5
    Log_ID, Target_Entity, Ingestion_Status, Processing_Phase,
    Records_Processed, Quarantined_Records,
    DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds,
    Ingestion_Start_Time
FROM Data_Pipeline_Logs
WHERE Table_ID = {Table_ID}
ORDER BY Ingestion_End_Time DESC;
```
> Present run history findings. If a failure is visible, note it — the user may want to redirect to failure analysis.
>
> **Tip:** For detailed log messages, query `Activity_Run_Logs` by `Log_ID` from the results.

### CD-1b: Recent DQ Notifications

```sql
-- Part B: Recent DQ notifications for Table_ID = {Table_ID}
SELECT TOP 10
    Data_Quality_Category, Data_Quality_Result,
    Data_Quality_Message, Rows_Impacted, Rows_Quarantined,
    Ingestion_Start_Time
FROM Data_Quality_Notifications
WHERE Table_ID = {Table_ID}
ORDER BY Ingestion_Start_Time DESC;
```
> Present DQ findings. Highlight any warnings or failures.

### CD-1c: Recent Schema Changes

```sql
-- Part C: Recent schema changes for Table_ID = {Table_ID}
SELECT
    Change_Type, Column_Name, Data_Type_Details, Schema_Arrival_Time
FROM Schema_Changes
WHERE Table_ID = {Table_ID}
ORDER BY Schema_Arrival_Time DESC;
```
> Present schema drift findings. Note any columns added, dropped, or type-changed.

### CD-1d: Latest Data Profile Summary

```sql
-- Part D: Latest data profile summary for Table_ID = {Table_ID}
SELECT
    Column_Name, Data_Type, Null_Percent,
    Approx_Distinct_Values, Total_Rows, Data_Profile_Execution_Time
FROM Exploratory_Data_Analysis_Results
WHERE Table_ID = {Table_ID}
  AND Data_Profile_Execution_Time = (
      SELECT MAX(Data_Profile_Execution_Time)
      FROM Exploratory_Data_Analysis_Results
      WHERE Table_ID = {Table_ID}
  )
ORDER BY Column_Name;
```
> Present profiling findings. After all 4 parts are complete, build the final Status Dashboard.

## CD-2: Failure Root Cause (Structured Logs + DQ Combined)

### Step 1: Get the failed run summary and structured log entries

```sql
-- Get the failed run(s) with structured log entries
SELECT
    l.Log_ID,
    l.Table_ID,
    l.Target_Entity,
    l.Ingestion_Status,
    l.Ingestion_Start_Time,
    l.Job_Run_URL,
    rl.Table_ID AS Activity_Table_ID,
    rl.Sequence_Number,
    rl.Log_Timestamp,
    rl.Log_Level,
    rl.Step_Name,
    rl.Step_Number,
    rl.Message
FROM Data_Pipeline_Logs l
LEFT JOIN Activity_Run_Logs rl
    ON l.Log_ID = rl.Log_ID
WHERE l.Table_ID = {Table_ID}
  AND l.Ingestion_Status = 'Failed'
ORDER BY l.Ingestion_Start_Time DESC, rl.Sequence_Number ASC;
```

### Step 2: Get DQ notifications for the same run (if any)

```sql
-- Get DQ results for the failed Log_ID (use Log_ID from Step 1)
SELECT
    Data_Quality_Category,
    Data_Quality_Result,
    Data_Quality_Message,
    Rows_Impacted,
    Rows_Quarantined
FROM Data_Quality_Notifications
WHERE Log_ID = '{Log_ID}'
  AND Table_ID = {Table_ID};
```

> **When to use:** User asks "why did table X fail?" — correlates structured pipeline logs with DQ failures.
>
> **Key advantages over the old `Run_Logs` blob approach:**
> - Filter directly: `WHERE Log_Level = 'ERROR'` → instant root cause
> - Filter by step: `WHERE Step_Name = 'Data Quality & Transformations'` → all DQ-phase messages
> - No truncation issues — each message is a separate row
> - Queryable across runs: `GROUP BY Step_Name` → failure hotspot analysis
>
> **Fallback:** If `Activity_Run_Logs` has no rows for a `Log_ID` (pre-migration runs),
> check the Job_Run_URL from `Data_Pipeline_Logs` for Spark-level error details.
> If `Data_Pipeline_Logs` itself has no rows (pipeline failed before logging "Started"),
> query `Activity_Run_Logs` directly by `Table_ID` — orphaned pipeline error entries are
> written with `Table_ID` by `PL_01b_For_Each_Entity`. Use `ActivityLogsByTableId` or the
> `ActivityLogsByExecution` fallback (pass `--table-id` alongside `--trigger-execution-id`).

> **Availability note:** `Activity_Run_Logs` is populated by both batch notebook processing and pipeline error logging. For non-batch or pipeline-only runs, rows may be present with `Source_Type = 'Pipeline'`.

## Interpreting Activity_Run_Logs

Structured log entries from `Activity_Run_Logs` contain **all `log_and_print()` messages** accumulated during the run, each tagged with `Step_Name`, `Step_Number`, `Log_Level`, `Log_Timestamp`, and `Source_Type`. On failure, entries with `Log_Level = 'ERROR'` contain the error details.

The `Step_Name` field tells you **which phase of the notebook failed** without needing to open the Spark monitor:

| Prefix | Notebook Phase | Common Root Causes |
|--------|---------------|-------------------|
| `[Step 3: Connect & Fetch Metadata]` | Warehouse connections, SP calls | Warehouse offline, token expiry, invalid Table_ID |
| `[Step 4: Parse Configuration]` | Metadata parsing | Missing/invalid primary config, bad JSON, conflicting settings |
| `[Step 5: Spark Configuration]` | Spark session setup, date dim creation | Environment misconfiguration, permission errors |
| `[Step 6: Data Ingestion]` | Source data read | Source unreachable, auth failure, bad file format, schema mismatch |
| `[Step 7: Schema Alignment]` | Column cleansing, DDL casting | Type cast errors, missing columns, schema contract violations |
| `[Step 8: Data Quality & Transformations]` | DQ checks, transformations, schema change detection | DQ force-failures, transformation errors, new schema rejected |
| `[Step 9: Write Data]` | Delta write, merge, post-processing | Merge key conflicts, disk space, concurrent writes, SCD2 errors |

When presenting failures to the user, **always surface both the step and the error text** — e.g., *"Failed at Step 8 (Data Quality & Transformations): 3 force-failure rules triggered."*
