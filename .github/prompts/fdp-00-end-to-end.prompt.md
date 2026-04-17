---
agent: agent
description: "Plan and execute an end-to-end accelerator workflow from one intent across authoring, validation, commit, run, investigate, and troubleshoot with approvals only where needed"
---

# Plan and Execute End-to-End Workflow

**{{ task_description }}**

## Execution Order

- Resolve the workflow plan first. Do not ask the user to manually chain `/fdp-03-author`, `/fdp-04-commit`, `/fdp-05-run`, and `/fdp-06-investigate` when the plan already includes them.
- Keep approvals narrow. Only stop for the plan's explicit approval boundaries or when a required input is genuinely unresolved.
- For any script-backed `commit`, `run`, `investigate`, or `troubleshoot` stage, follow the shared auth-preflight rule in `../common-patterns/terminal-execution-safety.md` before execution.
- Prefer deterministic scripts and checked-in contracts over re-deriving stage order from prompt prose.

## Planner Contracts

- Workflow registry: `.github/contracts/workflow-capabilities.v1.json`
- Query registry: `.github/contracts/query-capabilities.v1.json`
- Metadata schema: `.github/contracts/metadata-schema.v1.json`
- Plan resolver: `automation_scripts/agents/resolve_agent_plan.py`

## Workflow Planning

1. Resolve the plan with `python automation_scripts/agents/resolve_agent_plan.py --intent "{{ task_description }}"`.
2. Parse the JSON result and use `matchedRecipeId`, `stages`, `requiredInputs`, `approvalBoundaries`, and `investigationStrategy` as the source of truth.
3. If the result falls back to a single investigation or troubleshooting path, use the most specific existing `/fdp-*` contract instead of inventing a broader chain.

## Stage Rules

- `author`: follow `.github/prompts/fdp-03-author.prompt.md`
- `convert`: follow `.github/prompts/fdp-03-convert.prompt.md`
- `validate_metadata`: automatically run `python .github/skills/metadata-validation/validate_metadata_sql.py <file>` for each new or edited metadata SQL file before moving on
- `commit`: follow `.github/prompts/fdp-04-commit.prompt.md`
- `run`: follow `.github/prompts/fdp-05-run.prompt.md` and execute exactly once
- `investigate`: follow `.github/prompts/fdp-06-investigate.prompt.md` and select the preferred built-in query from `.github/contracts/query-capabilities.v1.json`
- `troubleshoot`: follow `.github/prompts/fdp-06-troubleshoot.prompt.md`

## Approval Boundaries

- `commit`: if the resolved execution context reports `HasOverrides = false`, stop for the dev-workspace safety confirmation before running the commit/deploy stage
- `run`: if the target is unresolved or a prior execution was cancelled, stop and ask before running again
- Do not add extra confirmation prompts between successful stages unless a downstream contract explicitly requires one

## Investigation Defaults

- Successful single-table run: prefer `LatestRunWithDQByTable`
- Successful multi-table or trigger run: prefer `LatestRunWithDQByTrigger`
- Failed or execution-correlated run: prefer `ActivityLogsByExecution` and `RunOutcomeWithDQ`
- Metadata inspection after authoring: prefer `FullMetadataConfig`

## Handoff Rules

- If authoring or conversion changes metadata, validation is mandatory before any commit stage
- If validation fails, fix and re-run validation before proceeding
- If commit partially succeeds, report per-stage outcomes and continue only from the failed stage when safe
- If run fails, gather investigation evidence before proposing fixes
- If run succeeds, include the correlated run row and DQ summary before closing