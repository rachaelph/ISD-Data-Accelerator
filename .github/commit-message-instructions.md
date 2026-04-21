# Commit Message Instructions

Generate commit messages using [Conventional Commits](https://www.conventionalcommits.org/) format for this Databricks Data Platform Accelerator repository.

## Format

```
<type>(<scope>): <short summary>
```

## Rules

1. **Type** (required) — choose the MOST specific match:

| Type | When to use |
|------|-------------|
| `feat` | New feature, new metadata attribute, new helper function, new DQ check, new pipeline capability |
| `fix` | Bug fix, error correction, broken behavior repair |
| `perf` | Performance improvement (Spark config, query optimization) |
| `refactor` | Code restructuring without behavior change |
| `docs` | Documentation only (markdown, docstrings, slide decks, comments) |
| `test` | Adding or updating tests (unit tests, integration tests) |
| `ci` | CI/CD changes (GitHub Actions, ADO pipelines, deployment scripts) |
| `chore` | Maintenance (dependency updates, config, tooling) |

2. **Scope** (optional but preferred) — the area of the codebase affected:

| Scope | Maps to |
|-------|---------|
| `bronze` | Bronze layer notebooks, ingestion |
| `silver` | Silver layer processing |
| `gold` | Gold layer, warehouse, dimensions |
| `metadata` | Metadata SQL files, stored procedures |
| `helper` | NB_Helper_Functions notebooks |
| `dq` | Data quality checks |
| `scd2` | Slowly changing dimensions |
| `entity-res` | Entity resolution / MDM |
| `ddl` | DDL generation and schema management |
| `cicd` | CI/CD pipelines and automation scripts |
| `deploy` | Deployment packages and workspace setup |
| `docs` | Documentation files |

3. **Summary** — imperative mood, lowercase, no period, max 72 chars
   - ✅ `feat(dq): add validate_pattern check with 13 pattern types`
   - ✅ `fix(scd2): correct single-version dimension start date`
   - ❌ `Updated the SCD2 code to fix a bug.`

4. **Breaking changes** — add `!` after type/scope:
   - `feat(metadata)!: rename source_details_custom_function to custom_table_ingestion_function`

5. **Multiple areas changed** — use the most impactful type. If a commit touches both code and docs, prefer the code type (feat/fix).

6. **Keep it to ONE line** — no body or footer needed for routine commits. Only add a body for breaking changes or complex context.
