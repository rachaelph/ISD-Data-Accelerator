# How AI Is Embedded in the Databricks Data Engineering Accelerator

> Last updated: 2026-03-29

> This document explains the ways AI — specifically GitHub Copilot — is woven into the accelerator's design, development workflow, and day-to-day operations.

---

## Overview

The Databricks Data Engineering Accelerator is not just *compatible* with AI — it was **architected from the ground up** to be AI-augmented. GitHub Copilot acts as an intelligent co-pilot across the entire data engineering lifecycle: from metadata generation and pipeline configuration, to troubleshooting, migration, and documentation.

---

## 1. 🧠 AI-Powered Metadata Generation

The core innovation of the accelerator is that pipelines are **metadata-driven** — you configure *what* should happen via SQL metadata tables, and the framework handles *how*. GitHub Copilot is the primary interface for creating that metadata.

| What Copilot Does | How |
|--------------------|-----|
| **Generates complete metadata SQL** | Users describe their scenario in natural language; Copilot produces fully structured SQL metadata configurations |
| **Handles all ingestion patterns** | Database ingestion (Oracle, SQL Server, PostgreSQL, etc.), file ingestion (CSV, JSON, Excel, XML, Parquet), shortcuts, and mirrored sources |
| **Configures transformations** | Joins, aggregations, derived columns, window functions, pivots, data quality checks, SCD Type 2 dimensions |
| **Validates before delivery** | Every generated SQL file is run through an automated validator (`validate_metadata_sql.py`) before being delivered to the user |

**Example interaction:**
```
User: "I need to ingest sales data from an Oracle database into Bronze.
       The table is SALES_SCHEMA.SALES, primary key is sale_id,
       and I want incremental loading using the MODIFIED_DATE column."

Copilot: Generates a complete SQL file with DELETE statements, orchestration
         config, primary config, watermark settings — validated and ready to deploy.
```

---

## 2. 🗂️ Copilot Customization Architecture

The repository uses the **standard GitHub Copilot customization framework** with three complementary layers that automatically provide context-aware assistance:

```
User opens file → Auto-applied Instructions load → Copilot has domain context
User types /command → Prompt workflow activates → Step-by-step guidance
```

| Layer | Location | Activation | What AI Does |
|-------|----------|------------|--------------|
| **Instructions** (7 files) | `.github/instructions/` | Auto-applied when editing matching file types | Provides domain rules, lookup tables, forbidden actions |
| **Prompts** (14 commands) | `.github/prompts/` | User types `/command` in chat | Runs structured multi-step workflows |
| **Skills** (2) | `.github/skills/` | Referenced by prompts and instructions | `metadata-validation` — runs validation script (40+ rules); `table-observability` — SQL templates for 8 query domains + live data queries |

> **Design choice:** This repository uses GitHub Copilot's **native customization framework** (Instructions, Prompts, and Skills) as its agent layer — no external agent infrastructure (AI Foundry, OpenAI endpoints, vector stores, RAG pipelines) is required. This eliminates IT overhead, custom hosting, and manual model upgrades. All new capabilities should be added through Instructions, Prompts, or Skills.

> **Contract-first extension:** The repo now also publishes machine-readable contracts under `.github/contracts/` for the metadata schema, workflow capability registry, and query capability registry. These contracts are generated from the existing validator, workflow, and investigation definitions so higher-level planners can reuse the same governed execution surface without re-parsing markdown by hand.

**Available `/commands`:**

14 slash commands cover the full lifecycle, including the top-level planner (`/fdp-00-execute`) plus onboarding (`/fdp-01-onboarding`), deployment (`/fdp-02-deploy`, `/fdp-02-feature`), authoring (`/fdp-03-author`, `/fdp-03-convert`), CI/CD (`/fdp-04-commit`), execution (`/fdp-05-run`), investigation (`/fdp-06-investigate`, `/fdp-06-troubleshoot`), and maintenance (`/fdp-99-maintain`, `/fdp-99-audit`, `/fdp-99-changelog`, `/fdp-99-prompt`). See [New User Onboarding](New_User_Onboarding.md) for the full command reference with usage examples.

Each instruction file contains:
- **Non-negotiable rules** specific to that domain
- **Documentation lookup tables** — patterns to find specific doc sections efficiently
- **Forbidden actions** — common mistakes to avoid
- **Validation requirements** — what must be checked before delivery

---

## 3. 🔄 AI-Assisted Code Conversion

When migrating from existing ETL platforms (SSIS, Informatica, ADF, custom SQL), Copilot acts as a **conversion engine**:

| Step | AI Role |
|------|---------|
| **Analyze existing code** | Copilot reads SQL/ETL logic and identifies sources, transformations, and targets |
| **Decompose into metadata** | Converts complex logic into the accelerator's metadata configuration format |
| **Generate equivalent configs** | Produces SQL metadata that replicates the original behavior using the framework's standardized helper functions |
| **Validate coverage** | Performs a sanity check to ensure 100% of original logic is captured |

This replaces what would otherwise be months of manual rewriting with a structured, AI-guided conversion workflow.

---

## 4. 📊 AI-Ready Data Governance Features

The accelerator produces structured outputs that are designed for downstream AI/analytics consumption:

### Exploratory Data Analysis (EDA) Profiling
- The `run_exploratory.py_Data_Analysis` notebook automatically profiles Delta tables
- Computes statistics: row counts, distinct values, null percentages, min/max/mean/std dev
- Results feed into the **Exploratory Data Analysis Power BI Report** for visual data profiling
- **Copilot integration:** The `table-observability` skill uses EDA results as a first-pass data source before querying live data endpoints — an EDA-first decision tree checks whether cached profiling data (< 24 hours old) can answer the user's question, avoiding unnecessary live queries

### Data Pipeline Lineage
- The `generate_lineage.py` notebook generates end-to-end lineage with:
  - Medallion layer tracking (Bronze → Silver → Gold)
  - Multi-source detection (joins, unions)
  - Cross-trigger lineage
  - **AI-ready narratives** — structured lineage descriptions consumable by LLMs

### Schema Evolution Detection
- Automatic schema hashing and change detection
- All schema changes logged to `dbo.Schema_Changes` with column name, old/new type, and timestamp

---

## 5. 📋 Copilot Customization Files

The repository includes purpose-built **Copilot customization files** that teach GitHub Copilot about the accelerator's architecture, patterns, and rules. This is what makes Copilot "smart" about this specific solution.

### What it enables:
- ✅ Understands the metadata-driven architecture
- ✅ Generates accurate configurations following established patterns
- ✅ Answers questions by referencing comprehensive documentation
- ✅ Guides deployment, configuration, and troubleshooting
- ✅ Validates configurations against the canonical metadata reference
- ✅ Provides step-by-step assistance tailored to experience level

### How it works:
1. User opens the repository in VS Code with GitHub Copilot enabled
2. Copilot **automatically detects** `.github/copilot-instructions.md` (global rules)
3. **Instructions auto-apply** based on the file being edited (e.g., `metadata-sql.instructions.md` loads when working in the `metadata/` folder under the active workspace git folder)
4. **Prompts** (`/commands`) provide structured multi-step workflows on demand
5. **Skills** (validator scripts) are called by prompts and instructions
6. Validates outputs before delivery

> **No additional configuration required** — Copilot automatically reads the configuration when the repo is opened.

---

## 6. 🔍 Intelligent Documentation Search

Rather than relying on ad-hoc grep over markdown alone, the repo now generates a **compact machine-readable knowledge index** at `.github/contracts/knowledge-index.v1.json`. It is intentionally a small doc-routing directory, not a giant section-by-section mirror of the docs.

| Layer | Artifact | Purpose |
|-------|----------|---------|
| Routing | `.github/contracts/knowledge-index.v1.json` | Maps fuzzy questions to the right document using route cards, keywords, aliases, and search hints |
| Machine authority | `.github/contracts/metadata-schema.v1.json` | Answers exact metadata-rule questions such as accepted values, defaults, and validator behavior |
| Explanation | `docs/*.md` | Holds the human-readable explanations and examples that are actually read and summarized |

The indexed doc-routing flow is:

```text
Question → grep compact knowledge index → pick routed doc → grep inside that doc → summarize → fallback to direct FAQ/doc grep only if needed
```

For targeted markdown lookups, the instruction files still rely on section-level patterns instead of loading entire docs:

| Topic | File | Search Pattern |
|-------|------|----------------|
| Golden Rules | `docs/METADATA_GENERATION_GUIDE.md` | `## Golden Rules for LLM Metadata Generation` |
| SQL File Format | `docs/METADATA_SQL_FILE_FORMAT.md` | `## SQL File Format and Structure` |
| Error Prevention | `docs/METADATA_SQL_FILE_FORMAT.md` | `## Error Prevention` |

This approach:
- Keeps the routing artifact small enough to stay useful
- Uses contracts for exact metadata rules when precision matters
- Preserves markdown as the explanation layer
- Handles synonyms and cross-cutting questions more reliably
- Loads only relevant sections (faster responses)
- Reduces token usage
- Ensures up-to-date guidance from the source

---

## 7. ✅ Automated Validation Loop

Every metadata SQL file generated by Copilot goes through a **mandatory validation cycle**:

```
Generate SQL → Run Validator → Fix Errors → Re-validate → Deliver
```

The validator checks:
- All attributes against `docs/METADATA_REFERENCE.md`
- SQL syntax (proper commas, semicolon on last VALUES row)
- Required configurations are present
- Typos and invalid values
- Returns clear error messages with line numbers

**No metadata SQL is delivered until validation passes.**

---

## Summary

| AI Capability | Where It Applies |
|---------------|------------------|
| **Natural language → metadata SQL** | Metadata generation for Bronze, Silver, Gold layers |
| **Skill-based routing** | Auto-applied instructions load based on file type; `/commands` for workflows |
| **Existing code conversion** | Converting SSIS, Informatica, ADF, SQL to metadata configs |
| **Automated validation** | Every generated file is validated before delivery |
| **EDA profiling** | Automatic data profiling with statistics and reports |
| **Lineage generation** | AI-ready narratives for end-to-end data lineage |
| **Schema evolution** | Automatic detection and logging of schema changes |
| **Context-aware assistance** | Copilot customization files (instructions, prompts, skills) teach AI about the entire solution |
| **Intelligent doc search** | Compact knowledge index for routing plus targeted markdown section loading |

> **Bottom line:** AI isn't a bolt-on feature — it's a core part of how users interact with, configure, and operate the accelerator.
