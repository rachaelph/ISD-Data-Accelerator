# Data Profiling — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to a DP-* template.

## DP-1: Latest Column-Level Profile

Prefer the helper below when you want profile data but the EDA table may be empty:

```powershell
python .github/skills/table-observability/scripts/invoke_profile_summary.py --source-directory . --environment DEV --table-name oracle_customers --medallion-layer silver --pretty
```

The helper returns cached EDA rows when available and falls back to live SQL-visible profiling from `INFORMATION_SCHEMA.COLUMNS` plus dynamic per-column summary queries when EDA is empty.

```sql
-- Latest data profile for Table_ID = {Table_ID}
SELECT
    Table_Name,
    Column_Name,
    Data_Type,
    Total_Rows,
    Approx_Distinct_Values,
    Null_Count,
    Null_Percent,
    Mean,
    Std_Dev,
    [Min],
    [Max],
    Data_Profile_Execution_Time,
    Table_Last_Modified_Time
FROM Exploratory_Data_Analysis_Results
WHERE Table_ID = {Table_ID}
  AND Data_Profile_Execution_Time = (
      SELECT MAX(Data_Profile_Execution_Time)
      FROM Exploratory_Data_Analysis_Results
      WHERE Table_ID = {Table_ID}
  )
ORDER BY Column_Name;
```
> **When to use:** User asks "show me the data profile for table X" or "what are the column statistics?"

## DP-2: Null Percentage Hotspots

```sql
-- Columns with highest null rates across all profiled tables
SELECT TOP 20
    Table_ID,
    Table_Name,
    Column_Name,
    Null_Percent,
    Null_Count,
    Total_Rows,
    Data_Profile_Execution_Time
FROM Exploratory_Data_Analysis_Results
WHERE Data_Profile_Execution_Time = (
    SELECT MAX(Data_Profile_Execution_Time)
    FROM Exploratory_Data_Analysis_Results e2
    WHERE e2.Table_ID = Exploratory_Data_Analysis_Results.Table_ID
)
  AND Null_Percent > 0
ORDER BY Null_Percent DESC;
```
> **When to use:** User asks "which columns have the most nulls?" or "where are the data gaps?"

## DP-3: Data Freshness Check

```sql
-- Data freshness: when was each table last modified and profiled?
SELECT
    Table_ID,
    Table_Name,
    Datastore_Name,
    Target_Medallion_Layer,
    MAX(Table_Last_Modified_Time) AS Last_Modified,
    MAX(Data_Profile_Execution_Time) AS Last_Profiled,
    DATEDIFF(HOUR, MAX(Table_Last_Modified_Time), GETUTCDATE()) AS Hours_Since_Modified
FROM Exploratory_Data_Analysis_Results
GROUP BY Table_ID, Table_Name, Datastore_Name, Target_Medallion_Layer
ORDER BY Hours_Since_Modified DESC;
```
> **When to use:** User asks "how fresh is the data?" or "when was the table last updated?"
