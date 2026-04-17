---
agent: agent
description: "Generate a curated changelog from raw commits — Highlights (ranked by impact) + Detailed Changes (deduplicated technical entries)"
---

# Update Changelog

Generate a curated changelog update for the **Fabric Data Platform Accelerator**.

## Execution Order

Prompt-specific input: release window, comparison range, or target changelog update scope.

## `vscode_askQuestions`-First Router

Use a bounded `vscode_askQuestions` call before pulling git history when the release scope is not yet clear.

- Top-level changelog `vscode_askQuestions` call
	- Question: `What changelog task do you want?`
	- Options:
		- `Update the Unreleased section`
		- `Cut a versioned release section`
		- `Review raw commits before writing`
- If the request is still broad, ask one more bounded `vscode_askQuestions` call for the missing detail:
	- last tag vs custom commit range
	- write directly to `CHANGELOG.md` vs draft only
	- highlights only vs full curated section
- Once the scope is specific, gather commits and curate them with the structure below.

## Step 1: Gather Raw Commits

**Use the net diff against `main`, not intra-branch commit history.**

Commit-by-commit logs include things added and later removed within the same branch, intermediate refactors, and experiments that never shipped. The changelog must reflect the **net difference** between the current branch and `main`.

### 1a — Get the file-level summary

```shell
git diff main --stat
```

### 1b — Get commit subjects for context

```shell
git log --oneline --no-merges main..HEAD
```

### 1c — Spot-check key changes with `git diff main`

For any file that looks significant (notebooks, pipelines, stored procedures, helpers), run `git diff main -- <path>` or `git show main:<path>` to understand what actually existed on `main` vs what exists now.

### What to look for

- **Files deleted on this branch that exist on `main`** → potential breaking changes or removed artifacts.
- **Files added on this branch that don't exist on `main`** → new features.
- **Functions/classes/tables that were renamed** → breaking changes.
- **Things that were added AND removed within the branch** → these are intra-branch noise and must NOT appear in the changelog.

> ⚠️ **Common mistake:** A feature or prompt is created in an early commit and deleted in a later commit on the same branch. The `git log` shows both, but the net effect is nothing changed. Always verify with `git diff main` before including an entry.

## Step 2: Curate into Two Sections

Transform the raw output into two sections: **Highlights** and **Detailed Changes**.

### Section 1 — Highlights

> This is the section people actually read. Make it count.

Pick the **5–8 most impactful changes** and write them for a human audience — a PM, a new team member, or someone deciding whether to upgrade. Rank by user impact (most valuable first).

**Format per item:**

```markdown
- **Short capability name** — One sentence explaining what it does and why it matters.
```

**Rules:**
- Plain English. No commit-speak, no scope prefixes, no backtick-heavy jargon.
- Each bullet must answer "what can I do now that I couldn't before?" or "what got meaningfully better?"
- Never include: docs-only updates, path fixes, test additions, refactoring, CI/CD plumbing.
- If a feature spans multiple commits, it gets ONE bullet (pick the best framing).
- Order by impact — the thing that saves the most time or unblocks the most users goes first.

**Example:**

```markdown
### Highlights

- **Pipeline error messages now logged to the warehouse** — Every pipeline run captures error details in `Data_Pipeline_Logs` with a direct link to the Fabric Monitor, so you can diagnose failures without leaving the warehouse.
- **One-command workspace deployment** — New `/fdp-04-commit` and `/fdp-05-run` Copilot commands let you deploy changes and trigger pipelines without touching Git or the Fabric UI directly.
- **SQL Analytics endpoints for Lakehouses** — Lakehouses and Warehouses now register their SQL Analytics endpoint in metadata, enabling direct T-SQL queries from notebooks and external tools.
```

### Section 2 — Detailed Changes

Group by category. Deduplicate aggressively.

**Categories (in this order, skip empty ones):**

1. 🚀 Features
2. 🐛 Bug Fixes
3. ⚡ Performance
4. 🔧 Refactoring
5. 📖 Documentation
6. 🧪 Testing
7. 🔄 CI/CD

**Deduplication rules:**
- Multiple commits touching the same feature → **1 bullet** summarizing the net change.
- A commit that adds something + a follow-up commit that fixes it → **1 bullet** (the feature, not the fix).
- Docs updates that describe a feature already listed → omit (the feature bullet is enough).
- Repeated "fix path" or "update docs" commits → group into one bullet like "Corrected instruction file paths across notebooks, pipelines, and observability configs."

**Format per item:**

```markdown
- Brief description of the change (no scope prefix, no commit hash)
```

Keep bullets concise — one line each. Technical detail is fine here, but no fluff.

## Step 3: Write to CHANGELOG.md

Open `CHANGELOG.md` and update the `[Unreleased]` section (or create a new versioned section if the user specifies a version number). Preserve all existing versioned sections below it.

**Full section template:**

```markdown
## [Unreleased]

### Highlights

- **Capability name** — Why it matters in one sentence.
- ...

### Detailed Changes

#### 🚀 Features
- ...

#### 🐛 Bug Fixes
- ...
```

## Rules

- **Always compare against `main` (net diff), not commit-by-commit history.** The changelog describes what shipped, not the journey. If something was added and removed within the same branch, it must not appear. Verify every breaking change and feature entry with `git diff main` or `git show main:<path>`.
- **Never run `git cliff --output CHANGELOG.md`** — that overwrites curated history.
- If the user says "release as vX.Y.Z", change `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD` and add a fresh empty `[Unreleased]` above it.
- Keep the `[1.0.0]` and all prior sections exactly as they are.
- The Highlights section should be readable in 30 seconds. If someone has to parse backtick-heavy jargon to understand what changed, rewrite it.