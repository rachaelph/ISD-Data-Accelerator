---
applyTo: "**/*.Warehouse/dbo/Tables/Data_Pipeline_Logs*,**/*.Warehouse/dbo/Tables/Data_Quality*,**/*.Warehouse/dbo/Tables/Schema*,**/*.Warehouse/dbo/Tables/Data_Pipeline_Lineage*,**/*.Warehouse/dbo/Tables/Exploratory*,**/*.Warehouse/dbo/Tables/Date_Dimension*,**/*.Warehouse/dbo/StoredProcedures/Log_*,**/*.Warehouse/dbo/StoredProcedures/Get_Schema*,**/*.Warehouse/dbo/StoredProcedures/Get_Last_Run*,**/*.Warehouse/dbo/StoredProcedures/Get_Watermark*,**/*.Warehouse/dbo/StoredProcedures/FIFO*,**/*.Warehouse/dbo/StoredProcedures/Get_Exploratory*"
---

# Observability & Logging Instructions

> Auto-applied when working on logging tables, schema tracking tables, lineage tables, profiling tables, or their associated stored procedures in `Metadata.Warehouse`.

---

## What These Files Are

The accelerator's observability layer consists of **6 logging/profiling tables** and **7 stored procedures** that track every aspect of pipeline execution:

| Table | Purpose | Written By |
|-------|---------|-----------|
| `dbo.Data_Pipeline_Logs` | Run history: status, row counts, errors, watermarks | SP `Log_Data_Movement` via `NB_Helper_Functions_3` |
| `dbo.Data_Quality_Notifications` | DQ check results: warnings, failures, quarantine counts | SP `Log_Data_Movement` (parsed from DQ JSON) |
| `dbo.Schema_Logs` | Schema snapshots as PySpark JSON, hash via `Schema_ID` | SP `Log_New_Schema` via `NB_Helper_Functions_3` |
| `dbo.Schema_Changes` | Column-level drift: added, dropped, type changed | SP `Log_New_Schema` (parsed from changes JSON) |
| `dbo.Data_Pipeline_Lineage` | Source→target lineage graph with depth and narratives | `NB_Generate_Lineage` (direct INSERT) |
| `dbo.Exploratory_Data_Analysis_Results` | Column-level profiling: nulls, distinct counts, stats | `NB_Run_Exploratory_Data_Analysis` (direct INSERT) |

| Supporting Table | Purpose |
|-----------------|---------|
| `dbo.Date_Dimension` | Human-readable date fields. All logging tables FK via `Date_Key` (INT, YYYYMMDD). |

| Stored Procedure | Parameters | Purpose |
|-----------------|------------|---------|
| `dbo.Log_Data_Movement` | Complex (see SP source) | Inserts `Started`/`Processed`/`Failed` records; parses DQ JSON into notifications |
| `dbo.Log_New_Schema` | Complex (see SP source) | Inserts schema snapshot; parses schema diff JSON into changes |
| `dbo.Get_Last_Run_Status` | `@table_id INT` | Returns last batch run status for a Table_ID |
| `dbo.Get_Schema_Details` | `@table_id INT` | Returns latest schema snapshot for a Table_ID |
| `dbo.Get_Watermark_Value` | `@table_id, @target_datastore, @processing_phase` | Returns current watermark for incremental loads |
| `dbo.FIFO_Status` | `@table_id, @target_datastore` | Checks for in-flight runs (24h window) |
| `dbo.Get_Exploratory_Analysis_Input` | See SP source | Determines which tables need profiling |

---

## Querying These Tables (Copilot Connection Method)

When investigating these tables, **execute queries directly** — do not just present SQL to the user.

**Connection pattern (Metadata Warehouse):**
1. Run `invoke_metadata_query.py` for standard warehouse investigations:
   ```powershell
   python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --environment DEV --query-type RunStatus --table-id <id>
   ```
   For successful single-table post-run validation, prefer:
   ```powershell
   python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --environment DEV --query-type LatestRunWithDQByTable --table-id <id>
   ```
   For successful multi-table or trigger post-run validation, prefer:
   ```powershell
   python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --environment DEV --query-type LatestRunWithDQByTrigger --trigger-name <name> [--table-ids "1,2,3"]
   ```
2. If you need raw connection details, run `python .\automation_scripts\resolve_metadata_warehouse_from_datastore.py --source-directory . --environment <ENV> --pretty`
3. Prefer the Python helpers because they are one-shot and avoid session-state issues.
4. In this repo, `--git-folder-name` is the environment folder that contains `datastores/` and `metadata/`, such as `dev`. It is not the repository name.

**Connection pattern (Target Data Endpoints — for live data queries):**
1. If the environment is not already known, ask the user which environment to use; otherwise reuse the provided environment and dynamically locate `datastores/datastore_{ENV}.Notebook/notebook-content.sql` (search recursively — parent folder varies by environment)
2. Look up the target table's `Endpoint` from the `Datastore_Configuration` INSERT statements
3. Reuse the same Azure AD access token; connect to the target SQL Analytics endpoint
4. If the investigation request is already specific about mode, environment, layer, and table, skip broad routing and load only the minimal observability references needed to reach the first query.

See `.github/skills/table-observability/SKILL.md` → **"How to Connect and Execute Queries"** and **"Connecting to Target Data Endpoints"** for full details.

---

## Non-Negotiables

1. **🔴 UNDERSTAND DOWNSTREAM IMPACT** — These tables are written to by **notebooks and stored procedures**. Before changing ANY column, verify it won't break the following:
   - `NB_Helper_Functions_3` → `log_data_movement()`, `log_new_schema()`, `log_failure_and_cleanup()`
   - `NB_Helper_Functions_1` → `execute_data_quality_checks()`, `analyze_schema_changes()`
   - `NB_Generate_Lineage` → lineage generation logic
   - `NB_Run_Exploratory_Data_Analysis` → profiling logic


2. **🔴 PRESERVE `Log_ID` FORMAT** — `Log_ID` is generated by `NB_Helper_Functions_3` (UUID format). Do NOT change the data type or format — it's used as a correlation key across `Data_Pipeline_Logs` and `Data_Quality_Notifications`.

3. **🔴 PRESERVE `Date_Key` FORMAT** — Must remain `INT` in `YYYYMMDD` format (e.g., `20260305`). Populated by stored procedures. Breaking this format breaks all time-based joins to `Date_Dimension`.

4. **🔴 PRESERVE `Schema_Details` FORMAT** — Stores PySpark StructType JSON. The `analyze_schema_changes()` function in `NB_Helper_Functions_1` parses this to detect column additions/drops/type changes. Do NOT change the column type or stored format.

5. **🔴 NO DESTRUCTIVE CHANGES WITHOUT VERIFICATION** — If removing rows from logging tables (e.g., to clear stuck FIFO runs), always confirm the `Log_ID` and `Table_ID` with the user first. Reference the FIFO resolution pattern in `docs/Restarting_Pipelines_From_Failure.md`.

---

## 🚫 FORBIDDEN

| ❌ Never Do This | Why |
|------------------|-----|
| Add `NOT NULL` constraints to logging columns | Notebooks handle nullability — SP inserts would fail |
| Rename columns in logging tables | Breaks 7+ notebooks and 7 stored procedures that reference these columns by name |
| Add foreign keys | Fabric Warehouse does not enforce foreign keys; adding them creates false expectations |
| Change `Date_Key` from INT to DATE | Breaks all YYYYMMDD-format joins across the entire logging layer |
| Modify `Log_Data_Movement` SP without updating `NB_Helper_Functions_3` | The notebook constructs the parameter payload — signature changes break ingestion |
| Modify `Log_New_Schema` SP without updating `NB_Helper_Functions_3` | Same — notebook calls this SP with specific parameter structure |
| DELETE from `Data_Pipeline_Logs` without checking FIFO impact | Could allow overlapping runs; use `FIFO_Status` SP to verify first |
| Change `Lineage_ID` to nullable | It's the only `NOT NULL` column in `Data_Pipeline_Lineage` — used as PK |

---

## Impact Assessment Checklist

Before modifying any DDL or SP in this scope, evaluate:

- [ ] Which notebooks reference this table/SP? (See "Written By" column above)
- [ ] Does the SP `Log_Data_Movement` parse JSON into this table? If so, check JSON key names match column names
- [ ] Does the change affect `Date_Key` joins? Verify `Date_Dimension` compatibility
- [ ] Does the change affect `Log_ID` correlation? Verify both `Data_Pipeline_Logs` and `Data_Quality_Notifications`
- [ ] Is any integration test affected? Check `integration_tests/stored_procedures/test_log_data_movement.py` (14 tests) and `test_log_new_schema.py` (11 tests)

---

## Documentation Lookup

Grep across `docs/` for the user's keywords. Observability-specific search patterns:

| Topic | File | Search Pattern |
|-------|------|----------------|
| 10 built-in DQ check types | `docs/DATA_TRANSFORMATION_GUIDE.md` | `data_quality checks` |
| Schema change detection logic | `docs/DATA_TRANSFORMATION_GUIDE.md` | `_handle_schema_change_validation` |
| DQ function catalog (30+ functions) | `docs/CORE_COMPONENTS_REFERENCE.md` | `Data Quality` |
| Schema management functions (8) | `docs/CORE_COMPONENTS_REFERENCE.md` | `Schema Management` |
| EDA / data profiling notebook | `docs/MONITORING_AND_LOGGING.md` | `Exploratory Data Analysis` |

---

## Metadata Configuration Tables (Cross-Reference)

> These tables define the pipeline configuration that **drives** the data written to logging tables. `Table_ID` is the universal FK connecting them. See `warehouse.instructions.md` for the full table and SP catalog.

| Metadata Table | Purpose |
|----------------|---------|
| `dbo.Data_Pipeline_Metadata_Orchestration` | Master list: triggers, tables, execution order |
| `dbo.Data_Pipeline_Metadata_Primary_Configuration` | Key-value config: source, target, watermark |
| `dbo.Data_Pipeline_Metadata_Advanced_Configuration` | Multi-attribute config: transformations, DQ rules |
| `dbo.Datastore_Configuration` | Connection details: endpoints, workspace IDs |

> For full metadata query templates, see `.github/skills/table-observability/SKILL.md` → Domain 7: Metadata Configuration.
> For live data query templates, see `.github/skills/table-observability/SKILL.md` → Domain 8: Live Data Queries.

---

## Cross-Reference: Integration Tests

| Test File | Covers | Test Count |
|-----------|--------|-----------|
| `integration_tests/stored_procedures/test_log_data_movement.py` | `Log_Data_Movement` SP, `Data_Pipeline_Logs`, `Data_Quality_Notifications` | 14 tests |
| `integration_tests/stored_procedures/test_log_new_schema.py` | `Log_New_Schema` SP, `Schema_Logs`, `Schema_Changes` | 11 tests |
| `integration_tests/stored_procedures/test_get_schema_details.py` | `Get_Schema_Details` SP | See file |

> **After ANY change to DDL or SPs**, remind the user to run integration tests:
> ```powershell
> python -m pytest integration_tests/stored_procedures/ -v
> ```

---

## 🔴 MANDATORY: After Making Changes

- [ ] Verify no notebook references are broken (check notebooks listed in "Written By" column)
- [ ] Verify stored procedure parameter signatures still match notebook calls
- [ ] Verify `Date_Key` format compatibility if date-related columns were changed
- [ ] Run integration tests if SP logic was modified
- [ ] Update the `table-observability` skill (`SKILL.md`) if column names/types changed — the SQL templates reference these columns directly
