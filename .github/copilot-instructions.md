# GitHub Copilot Instructions for Databricks Data Engineering Accelerator

## Global Rules

- Context-specific guidance loads automatically via `.github/instructions/` files.
- Use `/commands` from `.github/prompts/` for procedural workflows.
- Follow `.github/common-patterns/continuation-vscode-askquestions-call-pattern.md` for the shared `vscode_askQuestions` contract (routing, continuation, and quality gate).
- **Parallelize doc loading**: When a workflow step lists multiple independent `grep_search`, `read_file`, or skill loads, batch them in one parallel call instead of reading sequentially.

## Turn Completion Gate

Follow the continuation `vscode_askQuestions` contract referenced in Global Rules. Additional rules:
- If a routing `vscode_askQuestions` call was used earlier in the turn, do not emit a closing continuation call unless it clearly improves UX and will not render before the final answer in the current host.
- If `vscode_askQuestions` succeeded earlier in the turn, do not later claim unavailability without an actual tool failure.

---

## What This Repository Is

An **end-to-end data engineering accelerator** for Databricks that covers the full pipeline lifecycle — **author**, **convert**, **deploy**, **run**, **investigate**, and **troubleshoot** — driven by metadata and orchestrated through custom GitHub Copilot commands (`/fdp-*`). It uses a medallion architecture (Bronze → Silver → Gold) with 25+ built-in transformations, automated data quality checks, and observability. Existing SQL/ETL code can be migrated without rewriting, and new pipelines are defined by describing source tables in plain English.

---

## Persistence

Bias toward completion — continue working until the user's request is fully resolved in the current turn. Only stop early when:
- The user explicitly asks you to stop/pause, OR
- You need a clarification where the wrong assumption would waste significant work (e.g., wrong trigger name, wrong Table_ID range) — still provide what you can (template, skeleton, analysis), OR
- You are genuinely blocked by missing inputs/permissions AND you have produced a concrete partial deliverable.

---

## Python Environment Policy

- Use the approved base interpreter already on `PATH`. Do not create, auto-select, or inspect Python environments unless the user explicitly asks.
- Do not call `configure_python_environment`, `get_python_environment_details`, `get_python_executable_details`, `install_python_packages`, or Pylance Python environment tools during normal workflows.
- If an auto-discovered interpreter points to `.venv\Scripts\python.exe` or a missing path, treat it as invalid and use the base interpreter.
- If packages are needed, explain what is missing rather than silently creating `.venv/`.

---

## Answering Questions — Search Order

**Stop at the first step that fully answers the question. Later steps are fallbacks, not a checklist.**

1. **Named artifact?** (specific module, job, SP, script) → open that file directly.
2. **Exact values, defaults, or validator rules?** → read the metadata schema contract (authoritative).
3. **Topic question?** → `grep_search` across `docs/` with the user's keywords. `docs/FAQ.md` is the direct-answer layer; other guides cover specific domains.
4. **Fuzzy/conceptual question?** → `semantic_search`.
5. **Still unanswered?** → read source code.

> Metadata contract = authoritative for exact rules. Markdown docs = explanation and examples.
> Never load a full `docs/` file — grep first, read the matched section only.

---

## FORBIDDEN Actions

- Reading any file already provided as an `<attachment>` in the conversation context
- Making assumptions without loading the specified guidance first
- Loading entire documentation files in `docs/` (use `grep_search` to find sections, then `read_file` from the matched line)
- Delivering **new or edited** metadata SQL files without running the validator — Run: `python .github/skills/metadata-validation/validate_metadata_sql.py <file>` — this applies to `/fdp-03-author` and `/fdp-03-convert` workflows ONLY, **NOT** to `/fdp-04-commit` (which deploys already-validated files)
- Saying "this requires custom code" without implementing it — if logic needs a custom function `.py` file, CREATE IT
- Telling users to "review the documentation" — read and summarize it yourself


## Workspace Resolution

- Do not assume one connection-resolution strategy applies to every workflow.
- Use the prompt- or skill-specific resolution path for the active task.
- `/fdp-06-investigate` is datastore-notebook-driven.
- `/fdp-05-run` and `/fdp-04-commit` are Variable-Library-driven.
- Never hardcode endpoint, database, workspace ID, or override values when a workflow-specific resolver exists.

---

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <summary>`. Full rules in `.github/commit-message-instructions.md`.

---

## Available Prompts — Proactive Suggestions

> Surface matching `/command` as a continuation `vscode_askQuestions` option when a user's question aligns with one below. Answer first, then offer the command.

| Command | When to Suggest |
|---------|-----------------|
| `/fdp-01-onboarding` | User is new or asks "how do I get started?" |
| `/fdp-02-deploy` | User wants to deploy the accelerator IP for initial setup |
| `/fdp-02-feature` | User wants an isolated dev branch/workspace |
| `/fdp-03-author` | User wants to create/edit metadata, entities, DQ rules, transformations, or custom functions |
| `/fdp-03-convert` | User has existing SQL/Python/ETL code to convert into metadata |
| `/fdp-04-commit` | User wants to commit, push, sync workspace, and deploy metadata SQL |
| `/fdp-05-run` | User wants to execute a pipeline or job run |
| `/fdp-06-investigate` | User asks about table health, run history, DQ, schema, lineage, profiling, live data, or freshness |
| `/fdp-06-troubleshoot` | User asks why a pipeline/job failed or needs debugging help |
| `/fdp-99-maintain` | User wants to maintain core accelerator internals (modules, jobs, SPs) |
| `/fdp-99-audit` | User asks about documentation gaps or wants a content review |
| `/fdp-99-changelog` | User wants to generate or update the changelog from recent commits |
| `/fdp-99-prompt` | User wants to create, refactor, or maintain Copilot prompts and customization files |

---

## Response Quality

- **Always ship a deliverable**: Every response must include at least one tangible output (answer, snippet, template, SQL skeleton, or actionable checklist). Never end with only "I will" / "next" / "let me" without producing content.
- **No empty/abrupt replies** — if you must pause for user confirmation, still provide a concrete partial deliverable.
- **Use structured output for explanatory answers**: Include short Markdown section headers (`##` / `###`) in all non-trivial answers unless the user explicitly asks for a brief reply.
- **Use Markdown tables by default for multi-item explanations**: When describing 2 or more components, workflow stages, steps, options, comparisons, or status items, render them as a Markdown table unless a table would clearly hurt readability or the user explicitly asks for prose.
- Use code blocks with syntax highlighting and blockquotes for important notes.

---

## Follow-Up Choice UX

- Use `vscode_askQuestions` as the primary accelerator navigation surface for new users, not just for control flow.
- If a routing option is action-labeled and selected, perform that action in the same turn.
- Preserve slash commands as exact `vscode_askQuestions` option labels when recommending them.
