# Data Quality — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to a DQ-* template.

## DQ-1: DQ Summary for a Table

```sql
-- All DQ notifications for Table_ID = {Table_ID}
SELECT
    Log_ID,
    Table_Name,
    Data_Quality_Category,
    Data_Quality_Result,
    Data_Quality_Message,
    Rows_Impacted,
    Data_Quarantined,
    Rows_Quarantined,
    Ingestion_Start_Time,
    Job_Run_URL
FROM dbo.Data_Quality_Notifications
WHERE Table_ID = {Table_ID}
ORDER BY Ingestion_Start_Time DESC;
```
> **When to use:** User asks "what DQ issues does table X have?"

## DQ-0: DQ Results Correlated to an Exact Run

For successful single-table `/fdp-05-run` follow-up, prefer `LatestRunWithDQByTable`. For successful multi-table or trigger follow-up, prefer `LatestRunWithDQByTrigger`. Use `DQByLogId` directly only when you already have the exact `Log_ID`. Use `RunOutcomeWithDQ` only when you explicitly need `Trigger_Execution_ID` correlation.

```sql
-- DQ results for a run identified by Log_ID
SELECT
    dq.Log_ID,
    dq.Table_ID,
    dq.Table_Name,
    dq.Datastore_Name,
    dq.Data_Quality_Category,
    dq.Data_Quality_Result,
    dq.Data_Quality_Message,
    dq.Rows_Impacted,
    dq.Data_Quarantined,
    dq.Rows_Quarantined,
    dq.Ingestion_Start_Time,
    dq.Ingestion_End_Time,
    dq.Job_Run_URL
FROM dbo.Data_Quality_Notifications dq
WHERE dq.Log_ID = '{Log_ID}'
ORDER BY dq.Ingestion_End_Time DESC, dq.Ingestion_Start_Time DESC;
```
> **When to use:** User asks for DQ results from the run that just finished and you already have the correlated `Log_ID`.
>
> **Tip:** For successful single-table `/fdp-05-run`, prefer `LatestRunWithDQByTable` so the helper resolves the latest row and correlated DQ in one call.

## DQ-2: DQ Failures by Category

```sql
-- DQ notification breakdown by category for Table_ID = {Table_ID}
SELECT
    Data_Quality_Category,
    Data_Quality_Result,
    COUNT(*) AS Occurrence_Count,
    SUM(Rows_Impacted) AS Total_Rows_Impacted,
    SUM(Rows_Quarantined) AS Total_Rows_Quarantined
FROM dbo.Data_Quality_Notifications
WHERE Table_ID = {Table_ID}
  AND Date_Key >= {start_date_key}
GROUP BY Data_Quality_Category, Data_Quality_Result
ORDER BY Occurrence_Count DESC;
```
> **When to use:** User asks "what types of DQ issues are most common?"

## DQ-3: Quarantine Trends (Daily)

```sql
-- Daily quarantine trends for Table_ID = {Table_ID}
SELECT
    d.[Date],
    d.Day_Name,
    SUM(dq.Rows_Quarantined) AS Total_Quarantined,
    SUM(dq.Rows_Impacted) AS Total_Impacted,
    COUNT(*) AS DQ_Notification_Count
FROM dbo.Data_Quality_Notifications dq
JOIN dbo.Date_Dimension d ON dq.Date_Key = d.Date_Key
WHERE dq.Table_ID = {Table_ID}
  AND d.[Date] >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY d.[Date], d.Day_Name
ORDER BY d.[Date] DESC;
```
> **When to use:** User asks "is data quality improving or degrading?"

## DQ-4: Tables with Most DQ Warnings

```sql
-- Top tables by DQ notifications in last {days} days
SELECT TOP 20
    dq.Table_ID,
    dq.Table_Name,
    dq.Datastore_Name,
    COUNT(*) AS Total_Notifications,
    SUM(CASE WHEN dq.Data_Quality_Result = 'Failure' THEN 1 ELSE 0 END) AS Failure_Count,
    SUM(CASE WHEN dq.Data_Quality_Result = 'Warning' THEN 1 ELSE 0 END) AS Warning_Count,
    SUM(dq.Rows_Quarantined) AS Total_Rows_Quarantined
FROM dbo.Data_Quality_Notifications dq
JOIN dbo.Date_Dimension d ON dq.Date_Key = d.Date_Key
WHERE d.[Date] >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY dq.Table_ID, dq.Table_Name, dq.Datastore_Name
ORDER BY Total_Notifications DESC;
```
> **When to use:** User asks "which tables have the most DQ issues?"

## DQ-5: DQ Results for a Specific Run

```sql
-- DQ results for a specific pipeline run
SELECT
    dq.Log_ID,
    dq.Table_Name,
    dq.Data_Quality_Category,
    dq.Data_Quality_Result,
    dq.Data_Quality_Message,
    dq.Rows_Impacted,
    dq.Data_Quarantined,
    dq.Rows_Quarantined,
    l.Ingestion_Status,
    l.Records_Processed
FROM dbo.Data_Quality_Notifications dq
JOIN dbo.Data_Pipeline_Logs l ON dq.Log_ID = l.Log_ID AND dq.Table_ID = l.Table_ID
WHERE dq.Log_ID = '{Log_ID}';
```
> **When to use:** User asks "what DQ results came from this specific run?" Obtain `Log_ID` from `Data_Pipeline_Logs` first.
