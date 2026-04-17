---
agent: agent
description: "Investigate a table's run history, data quality, schema changes, lineage, data profiling, and live data using the logging tables and target data endpoints"
---

# Investigate Table

Investigate table: **{{ table_name_or_id }}**

## Execution Order

- Ask for environment before any live-data or current-schema query.
- After the route is clear, load `.github/skills/table-observability/SKILL.md` and only the domain references required for that path.

## Fast-Path-Only Policy

- Use only the direct helper, resolver, or drill-in path for the chosen mode.
- If one key input is missing, ask for that smallest missing input with `vscode_askQuestions` and then continue on the direct path.
- Do not search the workspace, grep metadata files, or explore docs to infer values the user could answer directly.
- If the direct path fails after the required inputs are known, report the failure briefly and ask for the next smallest decision instead of falling back to broad repo exploration.

## `vscode_askQuestions`-First Router

- Question: `What do you want to investigate first?`
- Options:
  - `Run health, failures, or performance`
  - `Data quality, schema, or freshness`
  - `Lineage, profiling, or live data`
- Follow-up dimensions if still broad: exact table/layer, metadata-only vs live-data, minimal answer vs broad health check.

## Investigation Paths

When loading a skill plus domain reference, batch them in one parallel read.

- Run health, failures, performance, or watermark:
	- Load `.github/skills/table-observability/SKILL.md` plus `references/run-history.md` when a built-in metadata query fits.
	- Prefer `scripts/invoke_metadata_query.py` for standard metadata-warehouse investigation modes.
- Data quality, schema drift, or freshness:
	- Load only the matching domain reference: `references/data-quality.md`, `references/schema.md`, or `references/profiling.md`.
	- Use `scripts/invoke_metadata_query.py` when the built-in query type matches the question.
- Lineage, profiling, or metadata config:
	- Load `references/lineage.md`, `references/profiling.md`, or `references/metadata-config.md`.
	- For table-scoped lineage, fetch upstream and downstream together in one batched step, then render the result with `renderMermaidDiagram`.
- Live data or current schema:
	- Ask for environment first.
	- Use `references/live-data.md` or `references/schema.md` with `scripts/invoke_target_query.py`.
	- Treat SQL endpoint results as SQL-visible output when unsupported Spark-only types may be omitted.
- Full health checks or novel cross-domain investigations:
	- Load `references/diagnostics.md` and use progressive execution instead of expanding the full workflow inline.

For terminal execution rules that apply across prompts, use [Terminal Execution Safety](../common-patterns/terminal-execution-safety.md).

## Execution Safety Rules

See [terminal-execution-safety.md](../common-patterns/terminal-execution-safety.md). Investigation-specific additions:

1. Default to one query per terminal call so the user can redirect after each finding.
2. Dependency chains should run in one terminal call.
3. `CD-1` full diagnostic may batch independent parts when speed matters more than incremental presentation.
4. Follow the shared auth-preflight rule in `../common-patterns/terminal-execution-safety.md` before any query-backed investigation.
5. Prefer `scripts/invoke_metadata_query.py` for metadata-warehouse investigations and `scripts/invoke_target_query.py` for live data or current schema.
6. For the specific ask "sample data and column statistics", prefer `scripts/invoke_table_snapshot.py` so the workflow executes one deterministic command after routing.
7. Do not start with workspace config lookup unless the user explicitly asks for workspace-variable or override context.
8. If a table name exists across multiple layers, ask which layer the user wants before querying.

## Workflow

Keep the prompt thin and delegate deep investigation logic to the table-observability skill.

1. Resolve the smallest missing decision with `vscode_askQuestions`.
   - If the user already provided mode, environment, layer, and table, do not ask another router question.
2. Load `.github/skills/table-observability/SKILL.md` plus only the domain reference file required for the routed mode — **read both in one parallel batch**.
3. Resolve the exact table.
	- Use the supplied `Table_ID` directly when available.
	- If the name matches multiple layers, ask the user which layer before querying.
	- Do not grep metadata to discover `Table_ID` when a helper already accepts `--table-name` and `--medallion-layer`.
4. Execute the smallest query path that answers the question.
	- Prefer the Python helper scripts instead of hand-written SQL when a built-in query type fits.
	- For table-scoped lineage, run both `LineageUpstream` and `LineageDownstream` together in one batched execution step before answering.
	- If the user already supplied environment, table, and layer for a sample-plus-stats request, skip extra exploratory steps and call `scripts/invoke_table_snapshot.py` directly.
	- If the user already supplied environment, table, and layer for an ad hoc target `SELECT`, use `scripts/invoke_target_query.py --query` or `automation_scripts/agents/resolve_datastore_endpoint.py --table-name ... --medallion-layer ...` directly instead of searching the repo for `Table_ID`.
	- If a direct helper needs one missing input, ask for it rather than exploring the repo.
	- Use `scripts/invoke_target_query.py` only for live data and current schema.
5. Escalate only when the user asked for a broad health check or the first result exposes a cross-domain issue.

## Output Contract

- Use Minimal output for single-mode investigations unless the user asked for a broad health check.
- For query-backed run-history, DQ, schema, profiling, config, and notebook-log answers, present the primary evidence in a compact Markdown table before the explanation.
- For successful-run DQ or quarantine questions, include one table for the selected run row and one table for the correlated DQ notifications.
- For lineage answers, include a compact Mermaid flowchart rendered with `renderMermaidDiagram`, not just fenced Mermaid text, and keep the prose summary brief.
- For failures, route to `/fdp-06-troubleshoot`.
- For metadata changes or DQ-rule changes, route to `/fdp-03-author`.
- For reruns or deployment follow-up, route to `/fdp-05-run` or `/fdp-04-commit`.
- For live-data or current-schema answers from the SQL endpoint, say explicitly when the result is only the SQL-visible subset rather than the full Spark schema.
- Do not recommend built-in DQ checks that already run automatically.

## Guardrails

- Execute queries instead of only presenting SQL.
- Prefer progressive disclosure: load the core skill first, then only the domain files required for the question.
- Do not hardcode the metadata warehouse, target database, endpoint, or workspace.
- Do not imply that a SQL endpoint omission means the table itself is missing Spark-only columns.