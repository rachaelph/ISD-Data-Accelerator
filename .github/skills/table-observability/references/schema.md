# Schema History & Changes — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to an SC-* template.

## SC-1: Current Schema for a Table (Live Query)

> **🔴 This template queries the LIVE datastore endpoint — NOT the Schema_Logs table.**
> Treat the result as the **SQL-visible schema**, not automatically as the full Spark schema.
> It requires environment selection and target endpoint resolution (same flow as LD-* templates).
> See [live-querying.md](live-querying.md) for the full target-endpoint resolution pattern.

### Prerequisites

1. **If the environment is not already known, ask the user which environment** (dev / test / prod) — scan `<git_folder>/datastores/` for available `datastore_{ENV}.Notebook` folders. Otherwise reuse the environment already provided or resolved from context.
2. **Run `invoke_target_query.py`** — this helper resolves from datastore notebooks, then handles token acquisition and query execution atomically.

### Execution — Single Terminal Call

```powershell
# Get live schema for a table — ONE command, deterministic result:
python ../scripts/invoke_target_query.py --source-directory . --git-folder-name {git_folder} --environment {ENV} --table-id {Table_ID} --query-type Schema --pretty
```

> **When to use:** User asks "what's the schema?", "show me columns", "what columns does table X have?"
>
> **What the helper does internally:**
> 1. Resolves Table_ID → target SQL endpoint + database
> 2. Parses TargetEntity into schema + table name
> 3. Acquires Azure AD SQL access token
> 4. Runs `INFORMATION_SCHEMA.COLUMNS` against the resolved endpoint
> 5. Returns JSON results
>
> **Response contract:**
> - Present this as the table's **SQL-visible** or **SQL-queryable** schema.
> - Do not imply the SQL endpoint result is the complete Spark schema when unsupported Spark data types may exist.
> - If the user reports missing columns, asks whether any columns are hidden from SQL, or asks for schema parity with Spark, run the metadata fallback below.
>
> **🔴 No metadata bootstrap script is needed** — the helper is self-contained.
> **🔴 No dependency chain to wire up** — the entire chain is encapsulated in one deterministic script call.

### Metadata Fallback for Hidden Spark-Only Columns

If the SQL-visible schema may be incomplete, query the latest logged schema snapshot from metadata and summarize the mismatch.

```powershell
# Get the latest logged Spark schema snapshot for the table
python ../scripts/invoke_metadata_query.py --source-directory . --git-folder-name {git_folder} --environment {ENV} --table-id {Table_ID} --query-type SchemaDetails --pretty
```

Use this fallback when:

1. The user says columns are missing from the SQL endpoint result.
2. The user asks whether unsupported Spark data types exist.
3. You need to explain why a `SELECT *` or `INFORMATION_SCHEMA.COLUMNS` result looks incomplete.

When presenting the fallback:

1. Summarize whether the latest `Schema_Details` payload contains columns not represented in the SQL-visible schema.
2. Call out the affected column names and data types when they are clear from the payload.
3. Do not dump raw `Schema_Details` JSON unless the user explicitly asks for it.
4. If metadata shows the column should be SQL-visible but the SQL endpoint still does not reflect it, treat SQL endpoint metadata refresh as an escalation-only step. Load `sql-endpoint-metadata-refresh.md` for the exact safe REST pattern, do not run it by default, and never set `recreateTables` to `true`.

## SC-2: Schema Change History

```sql
-- Schema change history for Table_ID = {Table_ID}
SELECT
    Table_ID,
    Table_Name,
    Change_Type,
    Column_Name,
    Data_Type_Details,
    Schema_Arrival_Time,
    Job_Run_URL
FROM Schema_Changes
WHERE Table_ID = {Table_ID}
ORDER BY Schema_Arrival_Time DESC;
```
> **When to use:** User asks "what schema changes happened to table X?" or "has the schema drifted?"

## SC-3: Tables with Recent Schema Drift

```sql
-- Tables with schema changes in the last {days} days
SELECT
    sc.Table_ID,
    sc.Table_Name,
    sc.Datastore_Name,
    COUNT(*) AS Total_Changes,
    SUM(CASE WHEN sc.Change_Type = 'Column Added' THEN 1 ELSE 0 END) AS Columns_Added,
    SUM(CASE WHEN sc.Change_Type = 'Column Dropped' THEN 1 ELSE 0 END) AS Columns_Dropped,
    SUM(CASE WHEN sc.Change_Type = 'Column Type Changed' THEN 1 ELSE 0 END) AS Types_Changed,
    MAX(sc.Schema_Arrival_Time) AS Latest_Change
FROM Schema_Changes sc
JOIN Date_Dimension d ON sc.Date_Key = d.Date_Key
WHERE d.[Date] >= DATEADD(DAY, -{days}, GETUTCDATE())
GROUP BY sc.Table_ID, sc.Table_Name, sc.Datastore_Name
ORDER BY Total_Changes DESC;
```
> **When to use:** User asks "which tables have had schema drift recently?"

## SC-4: Schema Evolution

> **Use SC-2 (Schema Change History) for schema evolution questions.** The `Schema_Changes` table tracks every column added, dropped, or type-changed over time — this is a cleaner and more actionable view than diffing raw PySpark schema snapshots.
>
> For "compare schemas over time" or "show me schema evolution", run SC-2 and present the changes chronologically. If the user needs to see the full raw schema at a specific point in time (rare), fall back to `Schema_Logs`.
