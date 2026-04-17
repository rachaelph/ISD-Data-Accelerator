---
name: table-observability
description: "Investigate tables across run history, data quality, schema, lineage, profiling, and live data using metadata-warehouse queries and target-endpoint scripts"
---

# Table Observability Skill

This skill answers table-observability questions with a thin router in `SKILL.md` and focused resources under `references/` and `scripts/`. The goal is HVE Core progressive disclosure: load a small core, then load only the domain file or script required for the current investigation.

## Quick Start

1. Identify the user intent and resolve the table to `Table_ID`.
2. Load only the domain file for that intent from the router below.
3. Prefer `scripts/invoke_metadata_query.py` for reusable metadata-warehouse investigations; fall back to raw metadata-warehouse SQL templates only when no query type fits.
4. Use `scripts/invoke_target_query.py` only for live data and current-schema queries.
5. For connection, terminal, and output rules, load the focused references instead of expanding this file.
6. For exact post-run validation after `/fdp-05-run`, always pass `--environment <ENV>` from the resolved user target; for Dev runs, use `--environment DEV`. For successful single-table runs, prefer `LatestRunWithDQByTable`. For successful multi-table or full-trigger runs, prefer `LatestRunWithDQByTrigger`. Reserve `RunOutcomeWithDQ`, `RunByExecution`, and `ActivityLogsByExecution` for failed runs, notebook investigations, or explicit execution-correlated requests.

## Fast Path

- Fast-path only: when a direct helper or resolver can answer the question, use it. Do not browse the repo, grep metadata, or inspect workspace shape unless that direct path actually fails.
- If the fast path is missing one required input such as environment, medallion layer, or exact table, ask the user for that smallest missing input with `vscode_askQuestions` and then execute the fast path.
- Never go on a broad exploratory search to recover inputs that could have been requested from the user in one bounded question.
- If the user already supplied the investigation mode, environment, layer, and table, skip any broad router step and go straight to the smallest skill plus reference load needed for that path.
- For a specific request like sample rows plus column statistics, do not load unrelated domains such as lineage, diagnostics, or run-history before the first query.
- For the specific request shape "sample data plus column statistics", prefer the one-shot helper `scripts/invoke_table_snapshot.py` so the workflow resolves the table once and returns both artifacts in one command.
- After the minimal read batch completes, resolve the exact `Table_ID` once and execute the helper command directly. Prefer the helper CLI's `--table-name <name> --medallion-layer <layer>` path so the script performs the lookup instead of the agent grepping metadata files. Avoid script-path discovery or repeated workspace-shape probing unless a command actually fails.
- Batch independent read-only context gathering in one step. Do not serialize helper-path lookup, environment confirmation, and metadata lookup when they can be known from the request and repository shape.

## Helper Argument Semantics

- In this repo, the helper scripts live under `.github/skills/table-observability/scripts/`.
- `--source-directory` must be the repository root that contains folders like `databricks_batch_engine/`, `docs/`, and `.github/`.
- `--engine-folder` is optional. When omitted, the helpers auto-discover the single top-level directory that contains `metadata/`, `custom_functions/`, and `datastores/` (default: `databricks_batch_engine`). Pass `--engine-folder <path>` or set `FDP_BATCH_ENGINE_FOLDER` only if you have multiple candidate folders.
- `--environment` (e.g. `DEV`) selects `databricks_batch_engine/datastores/datastore_<ENV>.json`, which supplies the workspace ID, workspace URL, default SQL warehouse, and the catalog/schema map per medallion layer.

## Quick Router

| User intent | Preferred resource | Notes |
|-------------|--------------------|-------|
| Last run, failures, performance, watermark | `references/run-history.md` | Metadata warehouse query templates |
| DQ issues, breakdowns, trends | `references/data-quality.md` | Metadata warehouse query templates |
| Exact post-run outcome from `/fdp-05-run` | `scripts/invoke_metadata_query.py` | For successful single-table runs, use `LatestRunWithDQByTable`; for successful multi-table or trigger runs, use `LatestRunWithDQByTrigger`; use execution-based queries only when correlation by `Trigger_Execution_ID` is required |
| Notebook step logs, notebook errors, "show me the logs for this run" | `scripts/invoke_metadata_query.py` | Use `ActivityLogsByExecution` with `--environment <ENV>` and `--table-id` for an exact run (falls back to `Table_ID` when `Data_Pipeline_Logs` has no rows), `ActivityLogsByLogId` for a known `Log_ID`, or `ActivityLogsByTableId` for recent activity logs by `Table_ID` |
| Current schema, schema drift, schema history | `references/schema.md` | SC-1 uses `scripts/invoke_target_query.py` |
| Upstream/downstream lineage | `references/lineage.md` | For table lineage, fetch upstream and downstream together and render with `renderMermaidDiagram` |
| Profile summary, null hotspots, freshness | `references/profiling.md` | Follow EDA-first guidance in `references/live-querying.md` |
| Profile summary with live fallback | `scripts/invoke_profile_summary.py` | Use when EDA may be empty and you still need column statistics |
| Root cause, full diagnostic | `references/diagnostics.md` | Progressive execution applies |
| Full config, source, target, trigger contents | `scripts/invoke_metadata_query.py` | Use `FullMetadataConfig` for all three metadata tables in one call |
| Datastore connection details, endpoint, workspace | `scripts/invoke_metadata_query.py` | Use `DatastoreConfig` with `--datastore-name` |
| Trigger summary with per-table last run status | `scripts/invoke_metadata_query.py` | Use `TriggerOverview` with `--trigger-name` — returns orchestration + last run per table |
| Watermark progress across a trigger | `scripts/invoke_metadata_query.py` | Use `WatermarkComparison` with `--trigger-name` — shows current vs previous watermark per table |
| Stale/stuck log rows blocking a re-run (FIFO) | `scripts/invoke_metadata_query.py` | Use `StaleLogs` with `--table-id` to find stale rows; add `--resolve` to insert a 'Failed' row that clears the FIFO block without deleting history |
| Reset watermark to force full reload | `scripts/invoke_metadata_query.py` | Use `ResetWatermark` with `--table-id` to see current watermark and reset value; add `--resolve` to insert a 'Processed' row with the reset watermark (auto-detects datetime vs int from config) |
| Live rows, counts, distributions, custom SELECT | `references/live-data.md` | Requires `references/live-querying.md` |
| Live sample plus cached profile | `scripts/invoke_table_snapshot.py` | Use when the user asks for both sample rows and column statistics together |

## Core Rules

### Connection model

- Metadata-warehouse queries should prefer `scripts/invoke_metadata_query.py`; it resolves the warehouse and executes canonical SQL for standard investigation modes.
- For successful single-table `/fdp-05-run` validation, prefer `scripts/invoke_metadata_query.py --environment <ENV> --query-type LatestRunWithDQByTable`. Use `RunOutcomeWithDQ` only when `Trigger_Execution_ID` correlation is actually required.
- Live-data and current-schema queries should prefer `scripts/invoke_target_query.py`.
- Never hardcode the metadata database, target database, or endpoint; resolve them at runtime.

See `references/execution-rules.md` for connection examples, terminal budgets, and progressive execution.

### Execution model

- Execute queries; do not just present SQL.
- Default to one query per terminal call unless the steps form a dependency chain.
- For table-scoped lineage, treat upstream and downstream as one dependency chain and fetch both together in one batched execution step.
- Keep result formatting in the same terminal call as the query.
- Prefer the Python CLIs because they are stateless and return machine-readable JSON.
- Pass `--pretty` when human-readable output helps during an investigation.
- If a required direct-resolution field is missing, ask for it. Do not replace that question with repo exploration.

### Live-data model

- Ask for environment before any live-data or current-schema query.
- Prefer cached EDA for profiling-style questions unless the user explicitly wants live or fresh data.
- Run `INFORMATION_SCHEMA.COLUMNS` before writing ad-hoc live-data queries.

See `references/live-querying.md` for target-endpoint resolution, EDA-first rules, and script usage.

### Output model

- Use compact Minimal-tier output for single-template investigations.
- Use progressive summaries for Standard and Full investigations.
- For run-history, DQ, schema, profiling, config, and notebook-log results, present the primary evidence in a compact Markdown table before the interpretation.
- When multiple DQ rules overlap, include both the unique quarantined row count from the selected run row and the per-rule counts from DQ notifications so the user can see why totals may differ.
- For lineage answers, always render the diagram with `renderMermaidDiagram`; do not stop at a fenced Mermaid code block.
- Use field-by-field `Write-Host` output for large text columns.

See `references/output-format.md` for the full formatting contract.

### Multi-layer resolution

If a table name exists in bronze, silver, and gold, ask the user which layer they want before querying. Do not silently choose a layer or query all layers by default.

## When to Use This Skill

- Table health, recent runs, failures, or run history
- Schema drift, schema history, or current schema
- DQ issues, quarantined records, and DQ trends
- Lineage, profiling, and data freshness
- Metadata configuration, source/target details, or trigger contents
- Live data inspection, sample rows, row counts, and custom read-only queries

## Table Reference

### Logging tables

| Table | Purpose | Primary keys |
|-------|---------|--------------|
| `Data_Pipeline_Logs` | Run status, row counts, timings, watermarks. A logical execution can emit phase-specific rows for `Staging`, `Batch`, or both. | `Log_ID`, `Processing_Phase`, `Ingestion_Status` |
| `Activity_Run_Logs` | Structured per-step messages for a run; populated by batch notebook processing and pipeline error logging. `Table_ID` enables independent lookup when `Data_Pipeline_Logs` has no matching row. | `Log_ID`, `Sequence_Number` |
| `Data_Quality_Notifications` | DQ results and quarantine outcomes | `Log_ID`, `Table_ID`, `Date_Key` |
| `Schema_Logs` | Schema snapshots and `Schema_ID` | `Table_ID`, `Date_Key` |
| `Schema_Changes` | Column-level drift history | `Table_ID`, `Date_Key` |
| `Data_Pipeline_Lineage` | Source-to-target lineage graph | `Source_Table_ID`, `Target_Table_ID`, `Trigger_Name`, `Date_Key` |
| `Exploratory_Data_Analysis_Results` | Cached profiling metrics | `Table_ID`, `Date_Key` |

For `Data_Pipeline_Logs`, treat `Staging` and `Batch` as phase-specific rows under the same logical execution when both exist. Use `Batch` when you only need the terminal outcome, and include `Staging` whenever the question is about landing, extraction, or end-to-end troubleshooting.

### Metadata configuration tables

| Table | Purpose | Primary keys |
|-------|---------|--------------|
| `Data_Pipeline_Metadata_Orchestration` | What runs, where it lands, and in what order | `Trigger_Name`, `Table_ID` |
| `Data_Pipeline_Metadata_Primary_Configuration` | Key-value config such as source, target, watermark | `Table_ID`, `Configuration_Category`, `Configuration_Name` |
| `Data_Pipeline_Metadata_Advanced_Configuration` | Multi-attribute config such as DQ rules and transformations | `Table_ID`, `Configuration_Category`, `Configuration_Name`, `Configuration_Name_Instance_Number` |

#### `Data_Pipeline_Metadata_Orchestration` columns

`Trigger_Name`, `Order_Of_Operations`, `Table_ID`, `Target_Datastore`, `Target_Entity`, `Primary_Keys`, `Processing_Method`, `Ingestion_Active`

### Supporting tables

| Table | Purpose |
|-------|---------|
| `Date_Dimension` | Human-readable date attributes via `Date_Key` |
| `Datastore_Configuration` | Datastore name to endpoint and workspace details |
| `Metadata_Files` | Metadata deployment tracking |

See `references/metadata-warehouse-reference.md` for full join rules, canonical columns, stored procedures, notebook writers, and documentation links.

## Join Relationship Map

```text
Orchestration.Table_ID
    -> Primary_Configuration.Table_ID
    -> Advanced_Configuration.Table_ID
    -> Data_Pipeline_Logs.Table_ID
             -> Activity_Run_Logs.Log_ID
             -> Data_Quality_Notifications.Log_ID
             -> Schema_Logs.Table_ID
             -> Schema_Changes.Table_ID
             -> Exploratory_Data_Analysis_Results.Table_ID

Orchestration.Target_Datastore
    -> Datastore_Configuration.Datastore_Name

All logging tables.Date_Key
    -> Date_Dimension.Date_Key
```

## Known Column Values

| Column | Table | Valid values |
|--------|-------|--------------|
| `Ingestion_Status` | `Data_Pipeline_Logs` | `Started`, `Processed`, `Failed` |
| `Processing_Phase` | `Data_Pipeline_Logs` | `Staging`, `Batch` |
| `Change_Type` | `Schema_Changes` | `Column Added`, `Column Dropped`, `Column Type Changed` |
| `Data_Quality_Result` | `Data_Quality_Notifications` | `Warning`, `Failure` |
| `Data_Quarantined` | `Data_Quality_Notifications` | `Yes`, `No` |
| `Configuration_Category` | Metadata config tables | `source_details`, `target_details`, `watermark_details`, `column_cleansing`, `other_settings`, `data_transformation_steps`, `data_quality` |
| `Ingestion_Active` | `Data_Pipeline_Metadata_Orchestration` | `1`, `0` |

## SQL Query Templates — Domain Index

| Domain | Templates | Reference file |
|--------|-----------|----------------|
| Run history | RH-1 to RH-7 | `references/run-history.md` |
| Data quality | DQ-1 to DQ-5 | `references/data-quality.md` |
| Schema | SC-1 to SC-3 | `references/schema.md` |
| Lineage | LN-1 to LN-3 | `references/lineage.md` |
| Profiling | DP-1 to DP-3 | `references/profiling.md` |
| Diagnostics | CD-1 to CD-2 | `references/diagnostics.md` |
| Metadata config | MC-1 to MC-12 | `references/metadata-config.md` |
| Live data | LD-1 to LD-7 | `references/live-data.md` |

### Loading rules

1. Load this core skill first.
2. Load only the domain file needed for the routed sub-mode.
3. Load `references/execution-rules.md`, `references/live-querying.md`, `references/metadata-warehouse-reference.md`, or `references/output-format.md` only when the active query needs that detail.
4. For cross-domain dependency chains, load only the extra domain files required by the chain.

## Focused References

- `references/execution-rules.md` — connection setup, anti-patterns, progressive execution, terminal budgets
- `references/live-querying.md` — environment selection, target-endpoint resolution, EDA-first decision tree, live-query notes
- `references/sql-endpoint-metadata-refresh.md` — escalation-only SQL endpoint metadata refresh pattern for stale SQL-visible output
- `references/metadata-warehouse-reference.md` — full table relationships, canonical columns, stored procedures, notebook writers, docs map
- `references/output-format.md` — dashboards, lineage output, live-data summary, recommendation sections

## Troubleshooting

- Query failed before output appeared: load `references/execution-rules.md`, prefer the one-shot Python CLI path, and follow the `2>&1` rule for `.ps1` scripts.
- Current schema or live sample is slow: use `references/live-querying.md` and prefer `scripts/invoke_target_query.py`.
- Query used the wrong column names: prefer `scripts/invoke_metadata_query.py`; for manual SQL, load `references/metadata-warehouse-reference.md` and use canonical names.
- SQL endpoint output looks stale, missing columns, or otherwise wrong: load `references/live-querying.md`, compare the SQL-visible schema with `SchemaDetails`, and load `references/sql-endpoint-metadata-refresh.md` only if the mismatch still points to stale endpoint metadata. Never run refresh by default, and never set `recreateTables` to `true`.
- Need exact results for the run that just finished: use `RunOutcomeWithDQ` or `RunByExecution` with the runner's `Trigger_Execution_ID`, then use `DQByLogId`, `ActivityLogsByExecution`, or `ActivityLogsByLogId` when you need correlated DQ rows or structured activity messages.
- Output is unreadable or truncated: load `references/output-format.md` and switch to the correct display pattern.

*🤖 Crafted with precision by ✨Copilot following human instruction, then refined for this repository.*

