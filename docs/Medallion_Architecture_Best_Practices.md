# Medallion Architecture Best Practices

> Last updated: 2026-01-15

This guide provides essential best practices for implementing a medallion architecture. **Read this before generating metadata.**

## Table of Contents

1. [What is Medallion Architecture?](#what-is-medallion-architecture)
2. [Layer-by-Layer Deep Dive](#layer-by-layer-deep-dive)
   * [Bronze Layer (Raw)](#bronze-layer-raw)
   * [Silver Layer (Validated)](#silver-layer-validated)
   * [Gold Layer (Enriched)](#gold-layer-enriched)
3. [Data Quality at Each Layer](#data-quality-at-each-layer)
4. [Common Anti-Patterns to Avoid](#common-anti-patterns-to-avoid)
5. [Implementation in This Accelerator](#implementation-in-this-accelerator)

---

## What is Medallion Architecture?

A **medallion architecture** is a data design pattern used to logically organize data in a catalog. Data flows through each layer (Bronze ⇒ Silver ⇒ Gold), progressively improving in structure and quality.

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **Bronze** | Store raw data in original format | Unvalidated |
| **Silver** | Clean, conform, and enrich data | Validated |
| **Gold** | Business-ready aggregates and models | Highly refined |

---

## Layer-by-Layer Deep Dive

### Bronze Layer (Raw)

The Bronze layer is your **landing zone** and **single source of truth** for raw data.

#### 🔴 Bronze Rules

| Rule | Description |
|------|-------------|
| **Minimal processing** | Store data exactly as received - no transformations |
| **Preserve source fidelity** | Keep original column names, types, and all records (even duplicates) |
| **Add metadata only** | `_source_file_name`, `_ingestion_timestamp`, `_batch_id` |
| **Enable reprocessing** | Retain all historical data for re-ingestion if needed |

```
✅ DO in Bronze:
   - Ingest raw data
   - Add audit columns
   - Basic partitioning by date/source

❌ DON'T in Bronze:
   - Apply transformations
   - Filter or drop records
   - Change data types
   - Apply data quality checks
```

### Silver Layer (Validated)

The Silver layer is where **cleanup and validation** occur.

#### 🔴 Silver Rules

| Rule | Description |
|------|-------------|
| **Build from Bronze** | NEVER write to Silver directly from source systems |
| **Enforce data quality** | Schema validation, null handling, deduplication |
| **Perform joins** | Combine related Bronze datasets here |
| **Quarantine invalid records** | Send failures to quarantine tables, not dropped |

```
✅ DO in Silver:
   - Data quality checks (validate_condition, etc.)
   - Type casting and validation
   - Deduplication
   - Joins between tables
   - Null handling

❌ DON'T in Silver:
   - Write directly from external sources
   - Create business aggregates
   - Apply heavy business logic
```

### Gold Layer (Enriched)

The Gold layer provides **business-ready** data optimized for analytics.

#### 🔴 Gold Rules

| Rule | Description |
|------|-------------|
| **Build from Silver** | Source from validated Silver tables only |
| **Organize by domain** | Finance, Sales, HR, etc. |
| **Create dimensional models** | Fact tables, dimension tables, aggregates |
| **Optimize for consumers** | Pre-aggregate for dashboards and reports |

```
✅ DO in Gold:
   - Aggregations (SUM, COUNT, AVG)
   - Dimensional modeling (star schema)
   - Surrogate keys for dimensions
   - SCD Type 2 for slowly changing dimensions
   - Business-specific filtering

❌ DON'T in Gold:
   - Store raw/unvalidated data
   - Keep large historical datasets (leave in Silver)
   - Create monolithic tables for all use cases
```

---

## Data Quality at Each Layer

| Layer | What to Check | Action |
|-------|---------------|--------|
| **Bronze** | Row counts, file metadata | **Observe only** - don't fix |
| **Silver** | Schema, nulls, duplicates, business rules | Validate, quarantine failures |
| **Gold** | Aggregates, freshness, completeness | Alert on anomalies |

> **Key Principle:** Data quality checks belong in **Silver**, not Bronze.

---

## Common Anti-Patterns to Avoid

| ❌ Anti-Pattern | Why It's Bad | ✅ Do This Instead |
|-----------------|--------------|-------------------|
| **Skipping Bronze** | No raw data for debugging/reprocessing | Always land in Bronze first |
| **Over-processing in Bronze** | Loss of data fidelity | Keep Bronze minimal |
| **Writing to Silver from source** | Schema changes cause failures | Always source from Bronze |
| **No quality gates** | Garbage in, garbage out | Add DQ checks in Silver |
| **Monolithic Gold tables** | Poor performance | Create purpose-built tables per domain |
| **DQ checks in Bronze** | Wrong layer for validation | Move DQ to Silver |
