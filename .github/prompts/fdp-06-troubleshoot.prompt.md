---
agent: agent
description: "Debug pipeline failures and notebook errors; route successful data quality or quarantine investigations to /fdp-06-investigate"
---

# Troubleshoot Pipeline Failure

Debug the following pipeline issue: **{{ error_description }}**

Do not stop at identifying the failing symptom. The goal of this workflow is to determine both the root cause and the most likely fix, with concrete evidence when possible.

Use the `table-observability` skill as the default path for warehouse-backed troubleshooting. Keep this prompt focused on user context, drill-in decisions, and fix recommendations.

## Evidence-Before-Fix Contract

- Do not recommend a fix until the possible issue has been thoroughly reviewed on the direct troubleshooting path.
- A generic top-level symptom, pipeline status, or single log headline is not enough.
- Keep drilling until you have the deepest concrete failure evidence you can reach quickly from the supplied context.
- If you cannot reach that evidence yet, do not guess. Ask for the smallest missing troubleshooting input or state that the investigation is not yet deep enough for a defensible fix recommendation.

Minimum bar before proposing a fix:

1. The deepest failing step, activity, or notebook cell reviewed so far
2. The concrete error text from that step
3. The evidence source used to verify it, such as notebook output, nested activity output, `Activity_Run_Logs`, `Data_Pipeline_Logs`, or a compact query-backed row sample

Allowed before that bar is met:

- summarize reviewed evidence
- name bounded hypotheses as unverified
- continue drilling on the direct path

Not allowed before that bar is met:

- prescribing metadata edits
- prescribing code changes
- prescribing source-data cleanup
- presenting speculative causes as the answer

## Triage Rule

- Treat `/fdp-06-troubleshoot` as a failure-debugging workflow first.
- If the user is asking about quarantined rows, warnings, or DQ outcomes from a run that completed successfully, this is an investigation request, not a troubleshooting request.
- In that case, route to `/fdp-06-investigate` and continue with the DQ investigation path there instead of doing failure drill-in.
- Stay in `/fdp-06-troubleshoot` only when the DQ issue caused a failed pipeline, failed notebook, blocked load, or the user explicitly wants root-cause debugging of a failed execution.

## Execution Order

- Before loading troubleshooting references, decide whether the request is a failure-debugging case or a successful-run DQ investigation.
- Follow the shared auth-preflight rule in `../common-patterns/terminal-execution-safety.md` before any script-backed troubleshooting drill-in.
- For successful-run DQ or quarantine questions, route immediately to `/fdp-06-investigate` with the same user context.

## Fast-Path-Only Policy

- Use only the direct helper, resolver, or drill-in path for the chosen mode.
- If one key input is missing, ask for that smallest missing input with `vscode_askQuestions` and then continue on the direct path.
- Do not search the workspace, grep metadata files, or explore docs to infer values the user could answer directly.
- If the direct path fails after the required inputs are known, report the failure briefly and ask for the next smallest decision instead of falling back to broad repo exploration.

## `vscode_askQuestions`-First Router

- Question: `Where is the failure showing up?`
- Options:
  - `Pipeline or activity failure`
  - `Notebook error or batch step failure`
  - `Data quality or quarantine issue`
- Follow-up dimensions if still broad: actual failed run vs successful run with quarantined rows, known trigger/table vs unknown scope, exact error vs generic, monitor output vs warehouse-log investigation.

## Workflow

0. **Triage the request**: If the run status is `Processed` and the user is asking why rows were quarantined, marked, or warned, route to `/fdp-06-investigate` instead of continuing this workflow.
1. **Resolve the smallest missing failure input**: Ask only for the next missing direct-path input such as environment, Trigger_Name, Table_ID or table plus layer, error text, Fabric Monitor URL, `Trigger_Execution_ID`, or `Log_ID`.
2. **Drill into nested activities directly**: If the user supplied a Fabric Monitor URL or run context, open the failed run, find the failed activity, click Output, follow nested pipelines or `ForEach`, and stop at the deepest concrete error instead of broadening into repo research.
3. **For notebook failures**: Open the notebook snapshot and capture the first failing cell output plus the parameter cell.
4. **Query the strongest direct evidence path**: Use observability skill helpers when warehouse-backed evidence is the fastest way to reach root cause. For successful single-table follow-up, prefer `LatestRunWithDQByTable` in the resolved environment. For successful multi-table or full-trigger follow-up, prefer `LatestRunWithDQByTrigger` with `--trigger-name` and optional `--table-ids`. Use `RunOutcomeWithDQ` or `RunByExecution` and `ActivityLogsByExecution` only when `Trigger_Execution_ID` correlation is actually required. Use `ActivityLogsByLogId` or `DQByLogId` directly when `Log_ID` is already known. Fallback: `diagnostics.md` CD-2 only after the direct helper path fails. Note: `Activity_Run_Logs` stores logs from both notebook and pipeline sources; filter by `Source_Type` when needed.
  Always pass the resolved metadata environment to helper-script queries; for Dev investigations, use `--environment DEV`.
  If the strongest evidence is likely in the upstream source data rather than the warehouse logs, query the source-side dataset directly and use that result as first-class troubleshooting evidence.
5. **Diagnose before fix**: Match the deepest concrete error to known issues, verify the evidence trail, and only then provide fix steps and offer to implement.
  - If the current evidence is still shallow or multiple hypotheses remain plausible, continue drilling and gather the next smallest evidence set before recommending changes.
  - Do not stop at a generic restatement such as `duplicate primary keys` or `schema mismatch`.
  - When the failure points to duplicate rows, conflicting merge keys, or duplicate primary keys, gather concrete proof rows and then recommend the fix.
  - Prefer the smallest direct evidence set that explains the issue: a few offending rows, the duplicate key values, or the conflicting source/target records.
  - Source-data evidence is a valid primary path when it best explains the failure, especially for duplicate keys, schema drift, unexpected nulls, malformed payloads, or source-side ordering problems.
  - State whether the right fix is in metadata, source data, target cleanup, or run sequencing.
  - When there is more than one plausible remediation, present the options in a compact Markdown table instead of prose-only bullets.
  - Even when there is one recommended fix, summarize it in a short Markdown table with columns such as `Option`, `Where to change`, `Why it works`, and `Recommended` when that improves readability.

6. **Duplicate-key and duplicate-row drill-in**: When the concrete failure is about duplicate primary keys, merge conflicts, or duplicate business keys, continue past the log lookup.
  - Show a compact example of duplicate rows or duplicate key groups instead of only quoting the log error.
  - If the configured primary key is present in metadata, treat that as the first candidate key and test the evidence against it.
  - If the configured key is missing, incomplete, or clearly wrong, identify the most likely key columns using direct evidence such as orchestration metadata, source/target schema, duplicate-group patterns, and profile results.
  - Explain why the current configuration fails. Example: deduplication is running on `*` instead of the actual business key columns, so rows with the same key but different timestamps survive until mandatory PK validation fails.
  - Recommend the concrete remediation pattern. Example: change `drop_duplicates.column_name` from `*` to the true key columns and keep ordered deduplication on the latest timestamp columns.

## Routing Boundary

- `/fdp-06-troubleshoot`: failed pipeline runs, failed notebook executions, blocked loads, or explicit root-cause debugging of a failed execution.
- `/fdp-06-investigate`: successful runs with DQ warnings, quarantined rows, schema drift review, freshness checks, profiling, or "why did these rows get flagged/quarantined?" questions.
- If the request starts in the wrong workflow, say so briefly and switch to the correct workflow behavior in the same turn.

## Information to Collect

| Information | Why Needed |
|-------------|------------|
| Trigger_Name | Filter logs, identify pipeline context |
| Table_ID | Pinpoint specific entity that failed |
| Error message text | Pattern matching to known issues |
| Fabric Monitor URL | Direct access to pipeline run details |
| Notebook snapshot link | For notebook failures — contains actual error |
| Source query or sample source rows | Lets Copilot prove whether the failure originates upstream rather than in downstream metadata or target state |
| Sample offending rows | Lets the user see the actual duplicate or conflicting records |
| Candidate key columns | Lets Copilot recommend the most likely metadata fix, not just describe the symptom |
| Recent changes | What changed since it last worked? |

## Evidence Query Guidance

- Load the `table-observability` skill first.
- Source-data queries are also valid when the fastest path to root cause is upstream evidence rather than warehouse evidence.
- Prefer the helper script over ad hoc SQL:
  - Successful single-table follow-up: `LatestRunWithDQByTable`
  - Successful multi-table or trigger follow-up: `LatestRunWithDQByTrigger`
  - Exact execution-correlated run status: `RunOutcomeWithDQ` or `RunByExecution`
  - Structured activity logs: `ActivityLogsByExecution` or `ActivityLogsByLogId`
  - Exact DQ rows: `DQByLogId`
- After the failure type is known, continue with the smallest direct evidence query needed to explain the fix. For duplicate-key failures, this means retrieving a compact sample of duplicate rows or duplicate key groups rather than stopping at the DQ row.
- If warehouse-side evidence is insufficient or the likely defect is upstream, query the source dataset directly for the smallest proof set needed to explain the failure.
- Pass `--environment <ENV>` to those helper-script calls; for Dev investigations, use `DEV`.
- Use `references/diagnostics.md` only when you need the fallback manual templates or step-prefix interpretation.
- If the user already supplied enough direct-path context, execute the matching helper immediately instead of searching the repo for identifiers you can ask for or derive from the supplied run context.
- If a direct helper needs one missing input, ask for it instead of exploring the repo.
- When presenting failures, always surface both the failing step and the error text when notebook log rows exist.
- When interpreting `Activity_Run_Logs` rows, note that `Step_Number` maps to the notebook cell number for `Source_Type = 'Notebook'` entries and is always `1` for `Source_Type = 'Pipeline'` entries. `Step_Name` is the notebook cell name for notebook logs and the failed activity name for pipeline logs.
- For query-backed failure or DQ answers, whether from warehouse logs or source data, present the primary evidence in a compact Markdown table before the diagnosis.
- For duplicate-key or duplicate-row failures, include three things in the answer whenever direct evidence is available: the failing key or duplicate condition, a small example row set, and the most likely corrected key or deduplication configuration.

## Fix Recommendation Contract

Every completed troubleshooting answer should include both:

1. **What is wrong**
  - The deepest failing step
  - The concrete error text
  - The evidence table or example rows that prove it

2. **What to change**
  - The most likely fix in metadata, data, or orchestration
  - Why that fix addresses the failure
  - Whether Copilot can implement the fix locally or whether the user needs to inspect Fabric/source data first
  - If there are multiple ways to resolve the failure, show them in a compact Markdown table and mark the recommended option explicitly

If the Evidence-Before-Fix Contract is not yet satisfied, stop after section 1, say the investigation is not yet deep enough for a trustworthy fix recommendation, and continue with the next direct troubleshooting step.

For duplicate primary key failures specifically, the answer should try to include:

1. The configured `Primary_Keys` value
2. A few example duplicate rows or duplicate key values
3. The most likely deduplication columns
4. A concrete recommendation such as changing `drop_duplicates.column_name`, adjusting `order_by`, fixing the source duplicates, or changing `if_duplicate_primary_keys`

### Output Contract — Resolution Table Shape

When presenting resolution choices, prefer a compact Markdown table like this:

| Option | Where to change | Why it works | Recommended |
|--------|-----------------|--------------|-------------|
| Change dedup to business key columns | Metadata | Removes reprocessed duplicates before PK validation | Yes |
| Clean duplicates at the source | Source system or landing data | Prevents duplicate keys from arriving downstream | Sometimes |
| Change duplicate-key behavior to warn/quarantine | Metadata | Allows load continuation but changes data quality behavior | Only when intentional |