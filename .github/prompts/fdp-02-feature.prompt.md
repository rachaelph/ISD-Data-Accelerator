---
agent: agent
description: "Set up a feature branch with a per-branch datastore override for isolated development"
---

# Set Up Feature Branch Development

Set up a feature branch for: **{{ feature_description }}**

## Execution Order

Prompt-specific input: `{{ feature_description }}`.

## `vscode_askQuestions`-First Router

- Question: `What do you need help with in the feature branch flow?`
- Options:
    - `Create the branch and scaffold a datastore override`
    - `Edit the active branch override`
    - `Promote changes back to dev`
- Route directly once the task is specific; do not dump the full lifecycle if the user only needs one phase.

## Isolation model

Branch isolation on Databricks happens at the **catalog** level inside the
same workspace — never by pointing at a different workspace. A per-branch
override file at
`databricks_batch_engine/datastores/overrides/<sanitized-branch>.json`
redirects writes to a branch-specific Unity Catalog so two developers cannot
clobber each other's tables. Schemas are not overridden because per-table
schemas already live on each metadata row's `Target_Entity` (`schema.table`).

The branch name is auto-detected from `git branch --show-current`; slashes
and other special characters become `-` (e.g. `feature/hschwarz/add-sales`
→ `feature-hschwarz-add-sales.json`).

## Workflow

1. **Create the branch**
   - `git switch -c feature/<user>/<short-description>` from `dev`.
2. **Scaffold the override file**
   - Compute the sanitized filename and create
     `databricks_batch_engine/datastores/overrides/<sanitized>.json`.
   - Minimum payload (Pattern 2 — per-branch catalogs; metadata isolation
     via a branch-specific metadata schema so the feature's copy of
     `Datastore_Configuration` stays scoped):
     ```json
     {
       "layers": {
         "bronze": { "catalog": "dev_bronze_<branch_slug>" },
         "silver": { "catalog": "dev_silver_<branch_slug>" },
         "gold":   { "catalog": "dev_gold_<branch_slug>" }
       },
       "metadata": { "schema": "<branch_slug>_meta" }
     }
     ```
   - See `databricks_batch_engine/datastores/overrides/README.md` for all
     supported keys (`sql_warehouse_id`, `variables`, `external_datastores`,
     etc.). Do **not** patch `layers.<name>.schema` — loader rejects it.
3. **Verify**
   - Run `python automation_scripts/agents/resolve_execution_context.py --source-directory . --git-folder-name <folder> --required-variable sql_warehouse_id --pretty`.
   - Confirm the output shows `"HasOverrides": true` and the expected
     `OverrideSourcePath`.
4. **Develop & test**
   - Author metadata SQL under `databricks_batch_engine/metadata/`.
   - Run `/fdp-04-commit` — it pushes the override file, syncs the Databricks
     Repo, upserts `Datastore_Configuration` in the branch's metadata schema
     from the JSON (including overrides), then deploys metadata SQL. After
     that, `/fdp-05-run` executes against the isolated catalogs.
5. **Commit & PR to dev**
   - Commit both the metadata SQL and the override file. The commit guard
     blocks pushing the override to `main`/`dev` by default.
6. **Promote to shared environment**
   - After PR review, delete (or soft-retire) the override file in the merge
     commit so the shared branch falls back to base `datastore_<ENV>.json`.

## Safety guard

`HasOverrides=true` in the execution context prevents `/fdp-04-commit` from
auto-pushing to protected branches. The guard relies on the override file
living in Git so review is enforced before isolation is lifted.

