# Live Querying

Use this reference for current-schema and live-data investigations against target Databricks SQL Warehouse or catalog endpoints.

## Environment Selection

Before any live-data or current-schema query:

1. Scan `<git_folder>/datastores/` for available `datastore_{ENV}.Notebook` folders.
2. If the environment is not already known, ask the user which environment to use.
3. If the environment is already provided or resolved from context, reuse it instead of asking again.

### Repository Shape Contract

- In this repo, `<git_folder>` means the top-level environment folder that contains `datastores/` and `metadata/`, such as `dev`.
- `--source-directory` points at the repo root. `--git-folder-name` points at the environment folder. Do not use the repository name as `--git-folder-name`.
- If the user already said `dev environment`, reuse `DEV` immediately and use `dev` as the folder argument instead of probing for alternatives first.
- If the user already supplied `environment + table + layer`, that is sufficient for the fast path. Do not inspect the repo to rediscover those same inputs.

## Preferred Script Path

Prefer `../scripts/invoke_target_query.py` for live-data and current-schema operations. It resolves the endpoint from datastore notebooks, acquires the token, executes the query, and returns JSON in one call.

```powershell
python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type Schema --pretty
python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type RowCount --pretty
python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type Sample --sample-size 20 --pretty
python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query "SELECT TOP 10 * FROM [{Schema}].[{Table}]" --pretty
```

For a different environment, keep `--source-directory .` at the repo root and swap both the folder and environment values together, for example `--git-folder-name test --environment TEST`.

## Manual Resolution Pattern

If the built-in script is not sufficient:

```powershell
python .\automation_scripts\agents\resolve_datastore_endpoint.py --source-directory . --environment DEV --table-id {Table_ID} --pretty
python .\automation_scripts\agents\resolve_datastore_endpoint.py --source-directory . --environment DEV --table-name oracle_customers --medallion-layer silver --pretty
```

- Prefer the `--table-name <name> --medallion-layer <layer>` form when the user already gave the table and layer.
- Do not grep metadata files just to discover `Table_ID` before running a custom target SQL query.

## EDA-First Decision Tree

Use cached EDA data first for profiling questions unless the user explicitly asks for live or fresh data.

```text
Question about row counts, nulls, distinct values, or freshness
  -> Check DP-1 / Exploratory_Data_Analysis_Results first
  -> If EDA exists and is <24h old and covers the question, use EDA
  -> Otherwise query the target endpoint live
```

### EDA can answer

- total rows and columns
- column types
- null counts and percentages
- approximate distinct counts
- mean, standard deviation, min, and max
- cached freshness indicators

### Live queries are required for

- sample rows
- current values and current distributions
- ad-hoc filtered reads
- any request explicitly asking for live, fresh, actual, or real-time data

## Specific-Request Execution Rule

- If the user explicitly asks for live sample data plus statistics and already provided the environment and layer, load only the live-querying and profiling references, resolve the exact `Table_ID`, then run the cached profile query and live sample query without any extra exploratory steps.
- If the user asks for an ad hoc `SELECT` against a known table and layer, resolve the endpoint with `resolve_datastore_endpoint.py --table-name ... --medallion-layer ...` or call `invoke_target_query.py --query` directly. Do not browse the repo to rediscover the same table.
- If either environment or layer is missing for a live query, ask the user for that input directly. Do not infer it by searching the workspace first.

## Live Query Notes

- The Python helper acquires a Databricks token automatically.
- The helper resolves git folders from datastore notebooks for investigation workflows; it does not require workspace config lookup.
- SQL Warehouse endpoints are read-only for query execution.
- Tables appear under the configured catalog and schema.
- For large tables, use `TOP`, filters, or aggregations; do not run unbounded `SELECT *`.
- Run `INFORMATION_SCHEMA.COLUMNS` before writing custom queries so you use the real live column names.
- Treat `INFORMATION_SCHEMA.COLUMNS` and `SELECT *` output as the SQL-visible subset of the table.
- If the user asks why columns are missing or whether unsupported Spark data types exist, query the latest metadata schema snapshot via `invoke_metadata_query.py --query-type SchemaDetails` and explain the mismatch.
- If the user says the SQL endpoint output looks stale, a column should exist but does not appear, or live data looks wrong, first compare the SQL-visible result with `SchemaDetails` or other metadata evidence.
- Only after that comparison points to stale endpoint metadata should you consider schema refresh mechanisms specific to the compute environment.
