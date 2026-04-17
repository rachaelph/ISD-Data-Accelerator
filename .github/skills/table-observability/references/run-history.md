# Run History — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to an RH-* template.

## Important Logging Model

- A logical pipeline execution can produce a `Staging` row, a `Batch` row, or both in `dbo.Data_Pipeline_Logs`.
- When both exist, they share the same `Log_ID` and represent separate processing phases of the same execution.
- Treat the log-row identity as the composite of `Log_ID`, `Processing_Phase`, and `Ingestion_Status`.
- Choose the phase intentionally: inspect all available phases for end-to-end troubleshooting, `Staging` for landing/extract behavior, and `Batch` for the terminal publish/merge outcome.

## RH-0: Exact Run Rows for a Specific Execution

Prefer `scripts/invoke_metadata_query.py --query-type RunByExecution` or `--query-type RunOutcomeWithDQ` only when you specifically need `Trigger_Execution_ID` correlation. For successful single-table `/fdp-05-run` follow-up, prefer `LatestRunWithDQByTable`. For successful multi-table or trigger follow-up, prefer `LatestRunWithDQByTrigger`.

```sql
-- Exact run rows for a specific execution
SELECT
    Log_ID,
    Table_ID,
    Target_Entity,
    Trigger_Name,
    Trigger_Step,
    Ingestion_Status,
    Processing_Phase,
    Records_Processed,
    Quarantined_Records,
    Ingestion_Start_Time,
    Ingestion_End_Time,
    DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds,
    Job_Run_URL
FROM dbo.Data_Pipeline_Logs
WHERE Trigger_Execution_ID = '{Trigger_Execution_ID}'
  AND ({Table_ID} IS NULL OR Table_ID = {Table_ID})
ORDER BY Ingestion_End_Time DESC, Ingestion_Start_Time DESC;
```
> **When to use:** User asks for execution-correlated rows and you specifically need `Trigger_Execution_ID` from the Python runner.
>
> **Tip:** Prefer the `Batch` row for the terminal outcome, then use `Log_ID` from that row to fetch correlated DQ notifications or `Activity_Run_Logs`.

## RH-1: Last N Runs for a Table

```sql
-- Last {N} runs for Table_ID = {Table_ID}
WITH latest_runs AS (
  SELECT TOP ({N})
    Log_ID,
    MAX(Ingestion_End_Time) AS Run_End_Time
  FROM dbo.Data_Pipeline_Logs
  WHERE Table_ID = {Table_ID}
  GROUP BY Log_ID
  ORDER BY MAX(Ingestion_End_Time) DESC
)
SELECT
  l.Log_ID,
  l.Table_ID,
  MAX(l.Target_Entity) AS Target_Entity,
  MAX(CASE WHEN l.Processing_Phase = 'Staging' THEN l.Ingestion_Status END) AS Staging_Status,
  MAX(CASE WHEN l.Processing_Phase = 'Batch' THEN l.Ingestion_Status END) AS Batch_Status,
  MAX(CASE WHEN l.Processing_Phase = 'Staging' THEN l.Records_Processed END) AS Staging_Records_Processed,
  MAX(CASE WHEN l.Processing_Phase = 'Batch' THEN l.Records_Processed END) AS Batch_Records_Processed,
  MAX(CASE WHEN l.Processing_Phase = 'Batch' THEN l.Quarantined_Records END) AS Batch_Quarantined_Records,
  MIN(l.Ingestion_Start_Time) AS Run_Start_Time,
  MAX(l.Ingestion_End_Time) AS Run_End_Time,
  DATEDIFF(SECOND, MIN(l.Ingestion_Start_Time), MAX(l.Ingestion_End_Time)) AS Duration_Seconds,
  MAX(CASE WHEN l.Processing_Phase = 'Batch' THEN l.Watermark_Value END) AS Batch_Watermark_Value,
  MAX(CASE WHEN l.Processing_Phase = 'Batch' THEN l.Job_Run_URL END) AS Job_Run_URL
FROM dbo.Data_Pipeline_Logs l
JOIN latest_runs r ON l.Log_ID = r.Log_ID
GROUP BY l.Log_ID, l.Table_ID, r.Run_End_Time
ORDER BY r.Run_End_Time DESC;
```
> **When to use:** User asks "show me recent runs for table X" or "what's the latest status?"
>
> **Logging model note:** Staging and Batch phases have separate `Log_ID` values. For `batch`-only tables this query returns one row per run. For `pipeline_stage_and_batch` tables it returns separate rows for the Staging and Batch phases, which lets you see per-phase duration and status independently. Use `Trigger_Execution_ID` when you need to correlate Staging + Batch as a single logical execution.
>
> **Tip:** For successful single-table `/fdp-05-run` follow-up, prefer `LatestRunWithDQByTable` instead of stitching multiple helper calls together.

## RH-2: All Failures in a Time Range

```sql
-- All failed runs in the last {hours} hours
SELECT
    l.Log_ID,
    l.Table_ID,
    l.Target_Entity,
    l.Trigger_Name,
    l.Processing_Phase,
    l.Ingestion_Status,
    l.Ingestion_Start_Time,
    l.Ingestion_End_Time,
    l.Job_Run_URL
FROM dbo.Data_Pipeline_Logs l
WHERE l.Ingestion_Status = 'Failed'
  AND l.Ingestion_Start_Time >= DATEADD(HOUR, -{hours}, GETUTCDATE())
ORDER BY l.Ingestion_Start_Time DESC;
```
> **When to use:** User asks "what failed last night?" or "show me recent failures"
>
> **Phase note:** Keep `Processing_Phase` in the result so you can distinguish staging failures from batch failures.
>
> **Tip:** For detailed log messages, query `Activity_Run_Logs` by `Log_ID` from the results.

## RH-3: Average Processing Time by Table

```sql
-- Average processing duration for Table_ID = {Table_ID} over last {days} days
SELECT
    Table_ID,
    Target_Entity,
    Processing_Phase,
    COUNT(*) AS Run_Count,
    AVG(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Avg_Duration_Seconds,
    MIN(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Min_Duration_Seconds,
    MAX(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Max_Duration_Seconds,
    AVG(Records_Processed) AS Avg_Records_Processed
FROM dbo.Data_Pipeline_Logs
WHERE Table_ID = {Table_ID}
  AND Ingestion_Status = 'Processed'
  AND Ingestion_Start_Time >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY Table_ID, Target_Entity, Processing_Phase;
```
> **When to use:** User asks "how long does table X take?" or "is it getting slower?"
>
> **Tip:** To correlate with specific step durations, query `Activity_Run_Logs` by `Log_ID` and group by `Step_Name`.

## RH-4: Records Processed Trend (Daily)

```sql
-- Daily record counts for Table_ID = {Table_ID}
SELECT
    d.[Date],
    d.Day_Name,
    l.Processing_Phase,
    SUM(l.Records_Processed) AS Total_Records,
    SUM(l.Quarantined_Records) AS Total_Quarantined,
    COUNT(*) AS Run_Count
FROM dbo.Data_Pipeline_Logs l
JOIN dbo.Date_Dimension d ON l.Date_Key = d.Date_Key
WHERE l.Table_ID = {Table_ID}
  AND l.Ingestion_Status = 'Processed'
  AND d.[Date] >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY d.[Date], d.Day_Name, l.Processing_Phase
ORDER BY d.[Date] DESC, l.Processing_Phase;
```
> **When to use:** User asks "what's the volume trend?" or "how many records per day?"
>
> **Phase note:** Group by `Processing_Phase` to avoid blending staging and batch counts into a single total.

## RH-5: Watermark Progression

```sql
-- Watermark progression for Table_ID = {Table_ID}
SELECT TOP {N}
    Log_ID,
    Ingestion_Start_Time,
    Ingestion_Status,
    Watermark_Value,
    Records_Processed
FROM dbo.Data_Pipeline_Logs
WHERE Table_ID = {Table_ID}
  AND Watermark_Value IS NOT NULL
  AND Processing_Phase = 'Batch'
ORDER BY Ingestion_End_Time DESC;
```
> **When to use:** User asks "what's the current watermark?" or "is incremental loading progressing?"
>
> **Tip:** For detailed log messages from a specific run, query `Activity_Run_Logs` by `Log_ID` from the results.

## RH-6: Failure Rate by Trigger

```sql
-- Failure rate summary by Trigger_Name
SELECT
    Trigger_Name,
    Processing_Phase,
    COUNT(*) AS Total_Runs,
    SUM(CASE WHEN Ingestion_Status = 'Failed' THEN 1 ELSE 0 END) AS Failed_Runs,
    SUM(CASE WHEN Ingestion_Status = 'Processed' THEN 1 ELSE 0 END) AS Successful_Runs,
    CAST(SUM(CASE WHEN Ingestion_Status = 'Failed' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS Failure_Rate_Pct
FROM dbo.Data_Pipeline_Logs
WHERE Ingestion_Start_Time >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY Trigger_Name, Processing_Phase
ORDER BY Failure_Rate_Pct DESC, Trigger_Name, Processing_Phase;
```
> **When to use:** User asks "which triggers fail the most?" or "what's the overall reliability?"
>
> **Phase note:** This query surfaces trigger reliability separately for `Staging` and `Batch`.

## RH-7: Stuck / In-Progress Runs

```sql
-- Runs still in 'Started' status (potentially stuck)
SELECT
    Log_ID,
    Table_ID,
    Target_Entity,
    Trigger_Name,
    Processing_Phase,
    Ingestion_Start_Time,
    DATEDIFF(MINUTE, Ingestion_Start_Time, GETUTCDATE()) AS Minutes_Elapsed,
    Job_Run_URL
FROM dbo.Data_Pipeline_Logs
WHERE Ingestion_Status = 'Started'
  AND Ingestion_Start_Time >= DATEADD(HOUR, -24, GETUTCDATE())
ORDER BY Ingestion_Start_Time ASC;
```
> **When to use:** User asks "are there any stuck runs?" or "what's currently running?"
>
> **Tip:** For the latest log messages from a stuck run, query `Activity_Run_Logs` by `Log_ID` — the last `Step_Name` shows where it's currently executing.

## RH-8: Structured Log Messages for a Run

Prefer `scripts/invoke_metadata_query.py --query-type ActivityLogsByExecution` when you have `Trigger_Execution_ID`, or `--query-type ActivityLogsByLogId` when you already know the correlated `Log_ID`.

```sql
-- Structured log entries for Log_ID = '{Log_ID}'
SELECT
    Table_ID,
    Sequence_Number,
    Log_Timestamp,
    Log_Level,
    Step_Name,
    Step_Number,
    Message
FROM dbo.Activity_Run_Logs
WHERE Log_ID = '{Log_ID}'
ORDER BY Sequence_Number ASC;
```
> **When to use:** User asks "show me the logs for this run" or "what happened during run X?" — use `Log_ID` from any RH-* query result.
>
> **Availability note:** These structured notebook rows are primarily expected for `Processing_Method = 'batch'` runs. If they are absent for a non-notebook run, treat that as expected and use `Data_Pipeline_Logs` plus Fabric Monitor instead.
>
> **Filtering tips:**
> - Errors only: add `AND Log_Level = 'ERROR'`
> - Specific step: add `AND Step_Name = '{step}'`
> - Last N messages: change to `SELECT TOP {N} ... ORDER BY Sequence_Number DESC`
