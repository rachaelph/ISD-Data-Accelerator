---
applyTo: "**/*.DataPipeline/**,data_pipelines/**"
---

# Pipeline Instructions

> Auto-applied when working on data pipeline files.

---

## Pipeline Inventory

| Pipeline | Purpose |
|----------|---------|
| `PL_01a_For_Each_Trigger_Step` | Iterates through Order_Of_Operations steps for a trigger |
| `PL_01b_For_Each_Entity` | Iterates through entities within a step |
| `PL_02_Get_External_Data` | Main external data pipeline (orchestrates Copy/Metadata/Watermark) |
| `PL_02_Get_External_Data_Copy` | Copies data from external source |
| `PL_02_Get_External_Data_Metadata` | Gets metadata from external source |
| `PL_02_Get_External_Data_Watermark` | Handles watermark/incremental loading |
| `PL_03_Execute_Non_Framework_Method` | Orchestrates non-framework execution methods such as warehouse stored procedures, custom Databricks notebooks, and external jobs |
| `PL_Exploratory_Data_Analysis` | Runs EDA notebook |

### Orchestration Hierarchy

```
PL_01a → PL_01b → PL_02 (data movement) → PL_03 (processing)
```

When modifying pipelines:
- Understand the hierarchy (01a → 01b → 02 → 03)
- Check parameter passing between pipelines
- Test with a single entity before full runs

---

## Common Error Patterns & Solutions

### Pattern 1: "Operation on target For Each Entity failed"
**Cause**: Generic wrapper — actual error is in a nested activity
**Solution**: Drill into ForEach → find failed iteration → get actual error

### Pattern 2: "Inner activity name: If Data Staging Required"
**Cause**: Error in the staging or data write phase
**Solution**: Check if source data exists, connection is valid, target catalog/schema accessible

### Pattern 3: Schema Mismatch / Column Not Found
**Cause**: Source schema changed, or metadata doesn't match actual data
**Solution**:
- Check `Schema_Logs` for schema evolution
- Set `fail_on_new_schema = 'false'` for auto-evolution
- Or update metadata to match new schema

### Pattern 4: Data Quality Failures
**Cause**: Records failed validation rules
**Solution**:
- Query `Data_Quality_Notifications` for details
- Check quarantine table: `{target_table}_quarantined`
- Review/fix source data or adjust DQ rules

### Pattern 5: Watermark Issues / Missing Data
**Cause**: Watermark value too high, or late-arriving data
**Solution**:
- Query `Data_Pipeline_Logs` for watermark values
- Reset watermark or trigger full reload
- Consider multiple watermark columns for Delta sources

### Pattern 6: "SQL pool is warming up"
**Cause**: Serverless SQL endpoint in sleep mode
**Solution**: Wait 2-3 minutes and retry; this is expected behavior

### Pattern 7: Notebook Spark Errors
**Cause**: PySpark execution failure
**Solution**:
- Get notebook snapshot
- Copy parameter cell to reproduce locally
- Check for: null handling, type mismatches, missing columns

---

## SQL Queries for Troubleshooting

### Recent Pipeline Failures
```sql
SELECT TOP 20
    Log_ID, Table_ID, Trigger_Name, Target_Entity,
    Ingestion_Status, Ingestion_Start_Time, Ingestion_End_Time,
    Job_Run_URL
FROM Data_Pipeline_Logs
WHERE Ingestion_Status = 'Failed'
ORDER BY Ingestion_Start_Time DESC
```

### Data Quality Issues for a Table
```sql
SELECT *
FROM Data_Quality_Notifications
WHERE Table_ID = @TableID
ORDER BY Ingestion_Start_Time DESC
```

### Watermark History
```sql
SELECT
    Table_ID, Target_Entity, Watermark_Value,
    Ingestion_Start_Time, Records_Processed
FROM Data_Pipeline_Logs
WHERE Table_ID = @TableID
  AND Ingestion_Status = 'Processed'
ORDER BY Ingestion_Start_Time DESC
```

---

## Pre-Built Reports for Troubleshooting

| Report | Use For |
|--------|---------|
| `RP_Data_Pipeline_Monitoring` | Real-time pipeline status, failure analysis |
| `RP_Exploratory_Data_Analysis` | Schema evolution, data profiling |
| `RP_Data_Lineage` | Source-to-target tracing, impact analysis |

---

## Documentation Lookup

Grep across `docs/` for the user's keywords. Pipeline-specific search patterns:

| Topic | File | Search Pattern |
|-------|------|----------------|
| Pipeline & Notebook Failures | `docs/FAQ.md` | `### Troubleshooting pipeline and notebook failures` |
| Where to See Failure Details | `docs/FAQ.md` | `### Where can I see why my pipeline failed?` |
| Schema Mismatch | `docs/FAQ.md` | `### How do I fix schema mismatch errors?` |
| Quarantine Issues | `docs/FAQ.md` | `### What does "quarantine" mean and how do I review quarantined records?` |
| NULL Values in Files | `docs/FAQ.md` | `### Why are my values becoming NULL during file ingestion?` |
| SQL Pool Warming | `docs/FAQ.md` | `### I'm getting "SQL pool is warming up" errors` |
| FIFO Status | `docs/FAQ.md` | `### What does "FIFO_Status" mean in the logs?` |
| Reload Data After Error | `docs/Restarting_Pipelines_From_Failure.md` | Restart strategies |
| Full Reload Process | `docs/DevOps.md` | `## Trigger Full Reloads` |
