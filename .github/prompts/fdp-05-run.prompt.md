---
agent: agent
description: "Execute a single table or full trigger against a Databricks workspace using the Python runner and shared observability tooling"
---

# Run Pipeline

Run the data pipeline for **{{ table_description }}**.

This workflow is **Variable-Library-driven**.

## Execution Order

- `/fdp-05-run` is Variable-Library-driven; do not resolve this workflow from datastore notebooks.
- After routing, batch independent `grep_search`, `read_file`, and skill loads in one parallel call so resolution work does not serialize unnecessarily.
- After the route is clear, resolve the exact target and execute the runner exactly once.

## `vscode_askQuestions`-First Router

- Question: `What do you want to run?`
- Options:
    - `A single table or Table_ID`
    - `A full trigger or trigger step`
    - `Inspect a recent run before deciding`
- Follow-up dimensions if still broad: table vs trigger, notebook vs pipeline mode, dev/base vs feature-workspace.

Core flow: resolve workspace config, resolve trigger or table metadata, execute exactly once via `automation_scripts/agents/run_pipeline.py`, then inspect logs and DQ.

> **Need to deploy code or metadata changes first?** Use `/fdp-04-commit` before running this command.

## Execution Safety Rules

Start with the shared terminal rules in `../common-patterns/terminal-execution-safety.md`.

Prompt-specific additions for `/fdp-05-run`:

1. Invoke `automation_scripts/agents/run_pipeline.py` exactly once per requested run.
2. Keep `isBackground: false`.
3. If execution is cancelled, stop and ask before any re-run because the prior Databricks run may still be active.
4. Follow the shared auth-preflight rule in `../common-patterns/terminal-execution-safety.md` before the run flow starts.
5. Use the canonical command shape below instead of reconstructing arguments from the script source.
6. Load the `table-observability` skill for post-run lookups instead of re-deriving query selection logic in the prompt.
7. For failed or timed-out runs, do not propose fixes from the runner result alone. Review the deepest available evidence first and follow the `/fdp-06-troubleshoot` evidence-before-fix contract before suggesting remediation.

## Workflow

1. Load context and resolve parameters in one pass.
    - Apply the shared auth-preflight rule from `../common-patterns/terminal-execution-safety.md` before any execution or observability helper that depends on workspace access.
    - `devops.instructions.md` is auto-loaded for `automation_scripts/**`; do not read it manually.
    - When metadata resolution, execution-context resolution, and required skill/reference reads are independent, load them in one parallel batch.
    - Resolve the table or trigger from metadata SQL.
    - Resolve execution context from the merged workspace config by using `automation_scripts/agents/resolve_execution_context.py`.
    - Apply defaults such as start step `0` and exploratory analysis `false` unless the user explicitly asked to change them.
2. Apply the feature-workspace safety guard.
    - If `HasOverrides` is false, confirm dev or base workspace intent only when the user has not already approved the same resolved target in the current chat.
    - If overrides are active, or the same target was already approved earlier in the conversation, proceed without another confirmation step.
3. Display the resolved execution summary.
    - Include execution mode, git folder, branch, feature-override status, workspace ID, target `Table_ID` or `Trigger_Name`, and processing method.
    - Ask the user only if a required value is genuinely unresolved.
    - Keep the summary compact so execution can start immediately once the target is resolved.
4. Execute `automation_scripts/agents/run_pipeline.py` exactly once.
    - Pass only the parameters needed for the resolved mode.
    - Use one of these canonical commands and substitute only the resolved values.
    - Do not infer extra flags from the script source.

      | Resolved mode | Canonical command |
      |---|---|
      | Single table notebook mode | `python automation_scripts/agents/run_pipeline.py --source-directory . --table-id <TABLE_ID> --pretty` |
      | Full trigger pipeline mode | `python automation_scripts/agents/run_pipeline.py --source-directory . --trigger-name "<TRIGGER_NAME>" --pretty` |
      | Multi-table pipeline mode | `python automation_scripts/agents/run_pipeline.py --source-directory . --trigger-name "<TRIGGER_NAME>" --table-ids "<TABLE_IDS>" --pretty` |

    - Use `--table-id` only for a single-table run.
    - Use `--table-ids` only when the user explicitly wants multiple tables within a pipeline run.
    - Do not pass both `--table-id` and `--table-ids` in the same command.
    - Leave exploratory analysis at the script default of `false` unless the user explicitly asks to enable it.
    - Optional flags that may be added only when the user explicitly asked for them or the request requires them: `--start-with-step`, `--trigger-step`, `--event-payload`, `--folder-path-from-trigger`.
    - Include `--pretty` for prompt-driven, user-facing runs so progress stays visible while the workflow waits.
    - Do not show the raw command unless the user explicitly asked for it.
    - Parse the JSON result block from stdout.
5. Handle results by outcome.
    - Load the `table-observability` skill and follow its `/fdp-05-run` post-run routing, including the exact `--environment <ENV>` requirement for metadata lookups.
    - For completed runs, always return the execution result together with the latest correlated run row and DQ summary in a compact, human-readable summary.
    - For completed single-table runs, use the skill's successful single-table lookup path rather than execution-ID-first chasing.
    - For completed multi-table or full-trigger runs, use the skill's trigger-level lookup path and pass the resolved trigger name plus any user-requested table subset.
    - Do not treat the runner result alone as sufficient for successful runs; include the correlated run row and any returned DQ rows.
    - For failed runs, check for correlated `Activity_Run_Logs` first by execution or `Log_ID`.
    - If notebook logs exist, summarize the relevant failure details and include the Job Instance ID when available.
    - If notebook logs do not exist but matching `Data_Pipeline_Logs` rows exist, return the failure result and direct the user to inspect the `Job_Run_URL`.
    - If neither notebook logs nor matching `Data_Pipeline_Logs` rows exist, say that no logs were found and tell the user to inspect the failure in the Databricks Jobs UI.
    - Always include the Job Instance ID for failed or timed-out runs when it is available.
    - For timed-out runs, provide the Job Instance ID and Monitor URL and offer a longer re-poll.
    - For failed starts, review the returned start failure details first. Only then point to auth, missing item, conflicting active run, or workspace sync issues when the returned evidence supports that conclusion.
    - For any failed or timed-out run, follow the `/fdp-06-troubleshoot` evidence-before-fix contract before recommending changes.
6. Iterate only when there is an actionable local fix.
    - If code or metadata changed, tell the user to run `/fdp-04-commit` before re-running.
    - Do not keep retrying when the failure is not yet actionable.

## Script Location

`automation_scripts/agents/run_pipeline.py`

## Shared Context Helper

`automation_scripts/agents/resolve_execution_context.py`

## Output Contract

Return:

1. A single status line that states what was run and whether it completed, failed, or timed out
2. Compact workspace context and Job Instance ID
3. Monitor URL if available
4. Failure summary from `Activity_Run_Logs` when rows exist for the failed run
5. Otherwise, a short log summary from `Data_Pipeline_Logs`, including `Job_Run_URL` when present
6. A short DQ summary from `Data_Quality_Notifications` when rows exist and are already available without extra chasing
7. A clear next step only when the run failed, timed out, or the user asked for deeper follow-up

For failed or timed-out runs:

- Do not lead with potential fixes.
- Show the reviewed failure evidence first, then follow `/fdp-06-troubleshoot` for any remediation.
- If the review is still incomplete, say so explicitly and continue with the smallest direct troubleshooting next step instead of guessing.

Formatting expectations:

- Lead with the outcome, not the process.
- Do not dump raw JSON, poll history, or long lists of log rows unless the user explicitly asked for them.
- When run rows or DQ rows are available, prefer compact Markdown tables over prose-only summaries.
- Prefer short bullets over section-heavy output.

## Post-Run Reuse

- Load and follow the `table-observability` skill before doing any post-run warehouse investigation.
- Reuse `.github/skills/table-observability/scripts/invoke_metadata_query.py` and the skill's focused references instead of restating query-type selection rules here.
- Keep prompt-specific response behavior inline: successful runs must include correlated run rows and DQ, while failed and timed-out runs must surface Job Instance ID, Monitor URL, and the best available log source.