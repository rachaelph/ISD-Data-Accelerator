---
agent: agent
description: "Commit metadata + custom code to Fabric workspace: local git push, Fabric Git sync, and direct metadata SQL deployment via Python automation"
---

# Commit Pipeline

Commit and deploy changes for **{{ change_description }}**.

This workflow commits local changes to Git, syncs the Fabric workspace from Git, and **deploys metadata SQL directly to the warehouse** through Python automation (direct SQL execution, not through Fabric T-SQL notebooks which take ~2 minutes).

## Execution Order

- This prompt executes an exact-once deployment flow after the target git folder and workspace context are resolved.
- Do not run metadata validation here; validation belongs in authoring and conversion workflows.
- Exception for direct invocation: when the user explicitly invokes `/fdp-04-commit` without extra qualifiers, treat it as a request for the default full flow (`commit + sync + metadata deploy`) instead of asking the mode-selection router question first.

## `vscode_askQuestions`-First Router

- Question: `What do you want to do with your current changes?`
- Options:
    - `Commit, sync, and deploy metadata`
    - `Sync or deploy without a new commit`
    - `Diagnose why commit or deploy is blocked`
- Follow-up dimensions if still broad: target git folder/environment, skip mode (`skip commit`, `skip metadata deploy`, or `just sync`), feature-workspace override status.

Router usage rule:
- Use this router only when the user explicitly asks for a non-default mode, when skip behavior is mentioned, or when the target git folder/environment is genuinely ambiguous.
- Do not use this router for a plain direct `/fdp-04-commit` invocation that does not request a special mode.

> **Just want to re-run an existing pipeline or notebook?** Use `/fdp-05-run` instead — it skips all Git and deployment steps.

## Execution Safety Rules

Start with the shared terminal rules in `../common-patterns/terminal-execution-safety.md`.

Prompt-specific additions for `/fdp-04-commit`:

1. Invoke `automation_scripts/agents/commit_pipeline.py` exactly once per requested commit or deploy attempt.
2. Keep `isBackground: false` and use a generous explicit timeout sized for workspace sync and deployment. Use at least `timeout: 300000` for normal runs, and increase it when many workspace items or metadata files may change.
3. If execution is cancelled, stop and ask before any re-run because the prior sync or deploy may still be in progress.
4. Follow the shared auth-preflight rule in `../common-patterns/terminal-execution-safety.md` before the commit flow starts.

## Latency-First Rule

- Optimize for time-to-first-decision. Do not preload or inspect implementation files before the first irreversible decision point.
- For a default `/fdp-04-commit` run, the pre-confirmation path must stay minimal:
  1. Resolve execution context with `automation_scripts/agents/resolve_execution_context.py`
  2. Check whether there are any changes under the resolved git folder
  3. If `HasOverrides` is false and the user has not already approved the same resolved target in the current chat, ask the dev-workspace safety confirmation immediately
- Before that confirmation, do not inspect `automation_scripts/agents/commit_pipeline.py`, do not enumerate every changed file recursively, and do not resolve warehouse display names.
- If the same workspace, branch, git folder, and action were already approved earlier in the conversation, treat that approval as sticky and continue without another confirmation.
- If the user confirms, gather the remaining execution summary details and then run `commit_pipeline.py` exactly once.
- If the user declines or the confirmation picker is cancelled, stop without additional exploration.

## Workflow

0. Run the local auth preflight before any workspace or metadata script by following the shared rule in `../common-patterns/terminal-execution-safety.md`.
1. Load context and resolve the execution target in one pass.
    - `devops.instructions.md` is auto-loaded for `automation_scripts/**`; do not read it manually.
    - Prefer `automation_scripts/agents/resolve_execution_context.py` to resolve the git folder, merged workspace config values, current branch, and `HasOverrides`.
    - Use the resolved git folder to check changes and scope the commit.
    - Default mode for a direct bare `/fdp-04-commit` invocation is full flow: commit, sync, and metadata deploy.
2. Stop early only when there is nothing to do or a required value is genuinely unresolved.
    - If no changes exist under the resolved git folder, report that there is nothing to commit.
    - Batch any genuinely unresolved values into one question instead of asking piecemeal.
3. Apply the feature-workspace safety guard.
    - If feature overrides are missing, confirm dev-workspace intent immediately after the minimal preflight only when there is no earlier same-target approval in the current chat.
    - If overrides are active, or the same target was already approved earlier in the conversation, proceed without an extra confirmation step.
4. Show the resolved execution summary.
    - Include git folder, branch, workspace ID, warehouse endpoint, feature-override status, and files changed under the resolved folder.
    - This summary can be compact before confirmation and expanded only after the user chooses to proceed.
    - Do not show the raw command unless the user explicitly asked for it.
5. Execute `automation_scripts/agents/commit_pipeline.py` exactly once.
    - Default behavior is commit, sync, and metadata deploy.
    - Always include `--pretty` for prompt-driven, user-facing runs so progress and stage results stay readable in the terminal.
    - Respect skip modes such as `--skip-commit`, `--skip-metadata-deploy`, or `just sync` when the user requested them.
    - Parse the JSON result block from stdout.
6. Report results by stage.
    - Treat `localGitStatus` values `Pushed` and `NoChanges` as successful local Git outcomes.
    - Treat `syncStatus` values `Synced` and `Skipped` (when no `--repo-id` was supplied) as successful Databricks Repo sync outcomes.
    - Treat `datastoreConfigSyncStatus` value `Synced` as a successful Datastore_Configuration sync outcome; `Skipped` is only valid when `--skip-datastore-sync` was requested.
    - Treat `metadataDeployStatus` values `Deployed` and `NoChanges` as successful metadata deployment outcomes.
    - If any stage fails, use the script error text as the primary source of truth and give the next actionable fix.
    - **Partial success**: If one stage succeeds but a later stage fails (e.g., commit OK but sync fails, or sync OK but metadata deploy fails), report each stage result individually and route the user to fix only the failed stage. Do not re-run successful stages.

## Prerequisites

- Auth and tooling preflight — follow the shared rule in `../common-patterns/terminal-execution-safety.md` (Azure CLI + `az login`, PowerShell 7, Python packages, verified by `automation_scripts/agents/preflight.py`).
- Git credential helper configured (`git config --global credential.helper manager`)
- Network access to the Azure Databricks workspace host

## Script Location

`automation_scripts/agents/commit_pipeline.py`

## Shared Context Helper

`automation_scripts/agents/resolve_execution_context.py`

## CLI Reference

```
python automation_scripts/agents/commit_pipeline.py \
  [--repo-id <REPO_ID>] \
  [--git-folder-name <FOLDER>] \
  [--source-directory <PATH>] \
  [--environment DEV] \
  [--commit-comment "<message>"] \
  [--skip-commit] [--skip-sync] [--skip-datastore-sync] [--skip-metadata-deploy] \
  [--prune-datastore-config] \
  --pretty
```

The pipeline runs five stages in order: **commit → sync Databricks Repo → sync `Datastore_Configuration` from JSON → deploy metadata SQL**. Stage 3 reads `databricks_batch_engine/datastores/datastore_<ENV>.json` (plus any active `overrides/<branch>.json`) and upserts `{metadata_catalog}.{metadata_schema}.Datastore_Configuration` on the metadata warehouse so pipeline runtime helpers see the same layer → catalog/schema mapping that Git declares.

| Argument | Required | Source | Description |
|----------|----------|--------|-------------|
| `--workspace-id` | **Yes** | `resolve_execution_context` → `Variables.metadata_workspace_id` | Target Fabric workspace |
| `--git-folder-name` | No | `resolve_execution_context` → `GitFolderName` | Git folder to scope changes (e.g. `dev`) |
| `--source-directory` | No | Defaults to `.` (repo root) | Local repo root path |
| `--commit-comment` | No | Build from change description | Conventional commit message |
| `--skip-commit` | No | User request | Skip local git commit+push |
| `--skip-update` | No | User request | Skip Fabric workspace sync |
| `--skip-metadata-deploy` | No | User request | Skip metadata SQL deployment |
| `--pretty` | No (CLI), **Yes for prompt-driven runs** | Required by this workflow for readable progress output | Human-readable step-by-step output |

## Output Contract

Return:

1. Resolved workspace context and git folder
2. Local Git result (`localGitStatus`)
3. Databricks Repo sync result (`syncStatus`)
4. Datastore_Configuration sync result (`datastoreConfigSyncStatus`, `datastoreConfigRowsUpserted`, `datastoreConfigTable`)
5. Metadata deploy result (`metadataDeployStatus` and executed file count)
6. Clear next step when any stage failed

## Commit Guardrails

- Commit only the files under the resolved git folder; mention unrelated changed files outside that scope rather than pulling them into the commit.
- Do not prompt for additional confirmation after the feature-workspace safety guard unless a required parameter is still unresolved or the resolved target/risk has materially changed since the user's earlier approval.
- Suggest `/fdp-05-run` only after a successful sync or metadata deployment path.
