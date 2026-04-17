---
name: conversion-verification
description: "Verify that a legacy-to-metadata conversion captured all source tables, complexity signals, and operation types. Run after generating metadata SQL from legacy code."
---

# Conversion Verification Skill

Deterministic post-conversion checks that compare **legacy source code** against **generated metadata SQL** to catch missed tables, dropped complexity, and unmapped operation types.

## Quick Start

Use this immediately after generating or editing converted metadata:

```powershell
python .github/skills/conversion-verification/verify_conversion.py \
  --source <legacy_file.sql> \
  --metadata <generated_metadata.sql>
```

If the source spans multiple files, pass them all in one invocation so table and complexity coverage are evaluated together.

## Usage

```powershell
# Basic — one source file, one metadata file
python .github/skills/conversion-verification/verify_conversion.py \
  --source <legacy_file.sql> \
  --metadata <generated_metadata.sql>

# Multiple source files
python .github/skills/conversion-verification/verify_conversion.py \
  --source proc1.sql proc2.sql helpers.py \
  --metadata <generated_metadata.sql>

# JSON-only output (for programmatic parsing)
python .github/skills/conversion-verification/verify_conversion.py \
  --source <legacy_file.sql> \
  --metadata <generated_metadata.sql> \
  --format json
```

## When to Run

- **During `/fdp-03-convert`** — invoke at Step 5 (VERIFY COMPLETENESS) after generating the metadata SQL file
- **After ANY edit** to a converted metadata SQL file
- Before delivering conversion output to the user

## Input Hints

- `--source`: one or more legacy SQL or Python files that define the original logic
- `--metadata`: the generated metadata SQL notebook content file
- `--format json`: use when the result will be post-processed instead of summarized directly
- `--stdin`: use only when the source text is available inline rather than as a file

## What It Checks

### CHECK 1: Table Coverage
Every source table referenced in the legacy code (`FROM`, `JOIN`, `INTO`, `UPDATE`, `MERGE`, `spark.read.table()`, `DeltaTable.forName()`, etc.) must appear somewhere in the generated metadata SQL — as a `source_details.table_name`, `target_entity`, `join_data.right_table_name`, or inside a `source_details.query`.

**Exclusions** (not checked):
- CTEs — expected to decompose into intermediate Table_IDs or be internalized in custom functions
- Temp tables / temp views — same as CTEs
- System tables (`sys.*`, `INFORMATION_SCHEMA.*`, `pg_catalog.*`)

### CHECK 2: Complexity Coverage
If the source contains patterns that **cannot** be expressed in built-in accelerator transformations — cursors, loops, recursive CTEs, UDFs, RDD operations, API calls, ML inference — the metadata **must** contain at least one `custom_transformation_function`, `custom_table_ingestion_function`, or `custom_staging_function`.

This is a **global check**: any complexity signal → any custom function must exist. It does not try to match specific signals to specific functions.

**Status meanings:**
- `PASS` — Complexity signals found AND custom functions exist
- `FAIL` — Complexity signals found BUT no custom functions → **must fix before delivery**
- `SKIP` — No complexity signals found (all logic is expressible in built-ins)

### CHECK 3: Operation Type Coverage
For each **category** of operation found in the source (JOIN, GROUP BY, CASE WHEN, window functions, UNION, PIVOT, etc.), the corresponding accelerator transformation type must exist somewhere in the metadata's Advanced Configuration.

This is a **presence check, not count matching**. If the source has 5 JOINs, we only check that `join_data` appears at least once — the LLM may legitimately restructure joins across medallion layers.

## Interpreting Results

### Status codes
| Status | Meaning | Action |
|--------|---------|--------|
| `PASS` | All checked items have coverage | No action needed |
| `WARN` | Some items unmatched but may be false positives | Review each; dismiss or fix |
| `FAIL` | Complexity signals with no custom function | **Must fix** — create custom function notebooks |
| `SKIP` | Check not applicable (e.g., no complexity signals) | No action needed |

### Exit codes
| Code | Meaning |
|------|---------|
| `0` | All checks PASS, WARN, or SKIP |
| `1` | At least one check FAILed |
| `2` | Input error (file not found, bad arguments) |

## False Positive Dismissal Guide

When reviewing results, dismiss these known false positives:

| Finding | Why It's a False Positive | Action |
|---------|---------------------------|--------|
| CTE or temp table reported as "unmatched" | CTEs decompose into Table_IDs or are internal to custom functions | Dismiss — script already excludes most, but edge cases may appear |
| System table (`sys.objects`, `INFORMATION_SCHEMA.COLUMNS`) | Used for introspection, not data flow | Dismiss |
| Operation type `filter` unmatched | Filters may be implicit in Bronze `WHERE` clauses or watermark conditions | Check if filtering is handled by watermark or hardcoded query |
| Operation type `derived_column` unmatched | SQL column aliasing is too broad; actual derived logic may use `conditional_column` or `aggregate_data` | Check if the derived logic is covered by another transformation |
| Operation type `type_cast` unmatched | The accelerator handles type casting implicitly in many transformations | Dismiss if source casts are simple format changes |
| Operation type `string_functions` unmatched | Column cleansing (TRIM, UPPER, LOWER) is handled by `column_cleansing` in Primary Config, not Advanced Config | Dismiss if only basic string operations |

## Source-Specific Refinement

After reviewing the generic verification output, you may optionally generate a **source-specific validation checklist** tailored to the exact conversion. This is especially useful for complex conversions with many tables/operations.

Example approach:
1. Run `verify_conversion.py` and review output
2. Dismiss false positives
3. For remaining findings, generate a targeted checklist:
   > "Verify that `dbo.Orders.OrderDate` is mapped as watermark column for Bronze Table_ID 1001"
   > "Verify that the recursive hierarchy flattening in CTE `employee_tree` is handled by `custom_transformation_function` in Table_ID 1201"

## Supported Languages

| Language | Detection Method | Coverage |
|----------|-----------------|----------|
| SQL (all dialects) | File extension `.sql` or keyword detection | FROM, JOIN, INTO, UPDATE, MERGE, GROUP BY, CASE WHEN, window functions, CURSOR, WHILE, etc. |
| PySpark | File extension `.py` or `spark.`/`from pyspark` detection | `.join()`, `.groupBy()`, `.withColumn()`, `spark.read.table()`, UDFs, RDD ops, etc. |
| Pandas | `pd.`/`from pandas` detection | `.merge()`, `.groupby()`, `pd.read_csv()`, etc. |
| Mixed | Auto-detected when both SQL and Python signals present | Combines all extractors |

### Deferred (not yet supported)
- **Scala Spark** — add when field teams need it
- **Deep embedded SQL parsing** (f-strings, concatenation inside `spark.sql()`) — simple string extraction works; deep parsing deferred

## Files

| File | Purpose |
|------|---------|
| `verify_conversion.py` | Self-contained verification script (pure stdlib Python) |
| `test_verify_conversion.py` | Unit + integration tests |
