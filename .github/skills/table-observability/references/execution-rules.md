# Execution Rules

Use this reference when the active investigation needs connection setup, terminal safety rules, or progressive execution details.

## Metadata-Warehouse Connection Pattern

Use `automation_scripts/resolve_metadata_warehouse_from_datastore.py` to resolve metadata-warehouse connection details when you need them directly.

For standard metadata investigation modes, prefer `../scripts/invoke_metadata_query.py` rather than composing ad hoc SQL. It resolves the warehouse, selects the canonical query, executes it, and returns JSON in one call.

In this repo, run the helper from the repo root. The helpers auto-discover the batch engine folder (``databricks_batch_engine/``). Pass ``--engine-folder`` only if the auto-detection reports multiple candidates.

```powershell
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type RunStatus --table-id {Table_ID} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type RecentRuns --table-id {Table_ID} --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type RunPerformance --table-id {Table_ID} --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type VolumeTrend --table-id {Table_ID} --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type WatermarkProgress --table-id {Table_ID} --top {N} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type TriggerReliability --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type ActiveRuns --hours {hours} --top {N} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type DQSummary --table-id {Table_ID} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type DQBreakdown --table-id {Table_ID} --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type DQTrend --table-id {Table_ID} --days {days} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type DQOverview --days {days} --top {N} --pretty
python .github/skills/table-observability/scripts/invoke_metadata_query.py --source-directory . --engine-folder databricks_batch_engine --environment DEV --query-type FullHealth --table-id {Table_ID} --pretty
```

```powershell
# Resolve the warehouse explicitly when you need endpoint details:
python .\automation_scripts\resolve_metadata_warehouse_from_datastore.py --source-directory . --environment {ENV} --pretty
```

The Python query helper is one-shot and stateless, so there is no terminal session bootstrap to preserve between calls.

## Core Safety Rules

1. Use `isBackground: false` for all investigation commands.
2. Use `timeout: 120000` for warehouse queries; never use `timeout: 0`.
3. Prefer the Python CLIs for deterministic one-shot execution and JSON output.
4. Keep query execution and output formatting in the same terminal call.
5. Reuse explicit parameters such as `--environment` and `--git-folder-name` across related calls.
6. Only fall back to manual SQL when no built-in query type fits.
7. Do not use shell-quoted `python -c` investigation commands when a supported helper exists; extend the helper surface instead.

## Result Formatting Rules

- Use `--pretty` on the Python CLIs when human-readable JSON helps.
- For compact downstream consumption, omit `--pretty` and consume the single-line JSON payload.
- Large text fields such as `Schema_Details` or JSON payloads should be summarized in the response instead of dumped verbatim.

See `output-format.md` for response-shape guidance.

### Batched Output Pattern

If you batch independent queries in one terminal call, prefer simple section-headed JSON output or separate script invocations.

```powershell
python ../scripts/invoke_metadata_query.py --query-type RunStatus --table-id {Table_ID} --pretty
python ../scripts/invoke_metadata_query.py --query-type DQSummary --table-id {Table_ID} --pretty
```

## Terminal Anti-Patterns

| Anti-pattern | What goes wrong | Correct behavior |
|-------------|-----------------|------------------|
| Re-implementing built-in queries by hand | Adds drift and inconsistent output | Use `invoke_metadata_query.py` or `invoke_target_query.py` first |
| Shell-quoted `python -c` one-offs for helper-shaped queries | Fragile quoting and type mismatches | Add or use a supported helper query type |
| Switching to background execution | Makes interactive investigations harder to inspect | Stay on `isBackground: false` |
| Running verification commands | Adds latency without answering the question | Run the real query directly |
| Re-running a command just because output is slow | Duplicates work and can create inconsistent state | Wait for completion with a generous timeout |
| Splitting endpoint resolution and query selection across many manual steps | Multiplies round-trips | Prefer one-shot Python CLIs |
| Dumping raw large JSON payloads without summarizing | Produces unreadable answers | Summarize key rows, counts, and anomalies |

## Progressive Execution

Use one query per terminal call when the queries are independent and the user benefits from incremental feedback.

If the user request is already fully scoped to one table, one environment, and one or two concrete outputs, minimize pre-query overhead: one read batch for the needed references, one targeted `Table_ID` resolution, then the actual helper call.

| Scenario | Execution pattern |
|----------|-------------------|
| Minimal single-template query | One terminal call to the appropriate Python CLI |
| Standard 2-3 template query | One terminal call per template |
| Full multi-part diagnostic (CD-1) | Progressive by default; batching the independent parts is allowed when optimizing for speed. |
| Dependency chain | Keep the chain in one call |

> **CD-1 note:** CD-1a/b/c/d are independent (all filter by `Table_ID`). They may be batched into one terminal call when speed is more important than incremental presentation.

### Dependency-chain examples

| Scenario | Why chained |
|----------|-------------|
| RH-1 -> DQ-5 | DQ query needs `Log_ID` from the run-history result |
| MC-9 -> `INFORMATION_SCHEMA` -> LD-* | Endpoint and schema discovery feed the live query |
| Orchestration -> live schema -> distinct-count query | Later queries depend on prior metadata or schema output |

## Terminal Call Budget

| Tier | Max terminal calls | Typical breakdown |
|------|--------------------|-------------------|
| Minimal | 2 | 1 environment-selection step if needed + 1 query call |
| Standard | 4 | 1 environment-selection step if needed + 2-3 progressive query calls |
| Full | 7 | 1 environment-selection step if needed + up to 6 progressive query calls |
| Full (optimized) | 4 | 1 environment-selection step if needed + 3 targeted query calls |

## Multi-Layer Resolution

If the same entity exists in bronze, silver, and gold, ask the user which layer they want before querying. Do not default to all layers.

Use a popup `vscode_askQuestions` call with `vscode_askQuestions` when the layer is ambiguous.
