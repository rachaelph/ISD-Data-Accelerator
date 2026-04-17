# Legacy Code Conversion Guide

> Last updated: 2026-03-29

> **How to systematically convert legacy SQL scripts, Python/Pandas code, SSIS packages, Informatica mappings, ADF pipelines, and other ETL tools into the accelerator's metadata format.** Covers decomposition, medallion layer assignment, operation-to-transformation mapping, and verification.
>
> **Related docs:**
> - [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) — core generation rules, golden rules, end-to-end examples
> - [TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md) — SQL INSERT examples for all 34 transformation types
> - [METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md) — SQL file structure and error prevention
> - [METADATA_REFERENCE.md](METADATA_REFERENCE.md) — canonical attribute spec


> **Purpose:** When converting legacy SQL scripts, Python/Pandas code, or other ETL tools to this accelerator's metadata format, use this section to systematically decompose procedural code into declarative metadata components across the medallion architecture.

### 🔴 CRITICAL: Conversion vs Generation

| Scenario | Approach | Key Difference |
|----------|----------|----------------|
| **Generation** ("I need a Customers table") | Start from requirements, design metadata | Ambiguous intent → You design the solution |
| **Conversion** ("Here's my 500-line stored procedure") | Decompose existing logic, map to patterns | Specific logic → Must preserve ALL behavior |

**Conversion is NOT Generation!** When converting legacy code:
- You MUST decompose the procedural code first (see Step 4: Conversion Workflow below)
- You MUST identify ALL operations in the source code
- You MUST verify EVERY operation is covered in output metadata
- Missing even ONE operation = incomplete conversion = broken pipeline

### Step 1: Decompose Legacy Code (MANDATORY)

Before mapping anything, break down the legacy code into atomic operations:

```
┌─────────────────────────────────────────────────────────────────┐
│ DECOMPOSITION CHECKLIST                                         │
├─────────────────────────────────────────────────────────────────┤
│ □ List ALL source tables/files read                             │
│ □ List ALL columns selected/created                             │
│ □ List ALL filtering conditions (WHERE, HAVING)                 │
│ □ List ALL joins (type, tables, keys)                           │
│ □ List ALL aggregations (GROUP BY + functions)                  │
│ □ List ALL window functions                                     │
│ □ List ALL CASE/IF logic                                        │
│ □ List ALL type conversions                                     │
│ □ List ALL string manipulations                                 │
│ □ List ALL date/time operations                                 │
│ □ List ALL CTEs and their dependencies                          │
│ □ List ALL temp tables and their usage                          │
│ □ List ALL loops/cursors (may need custom_transformation_function)             │
│ □ List ALL recursive patterns (DEFINITELY needs custom_transformation_function)│
│ □ Identify the FINAL output structure                           │
└─────────────────────────────────────────────────────────────────┘
```

**Example Decomposition:**
```sql
-- Legacy Stored Procedure:
CREATE PROCEDURE sp_CustomerSummary AS
BEGIN
    SELECT 
        c.customer_id,
        UPPER(c.name) as customer_name,           -- ① string_functions
        COALESCE(c.email, 'N/A') as email,        -- ② apply_null_handling  
        COUNT(o.order_id) as order_count,         -- ③ aggregate_data
        SUM(o.total) as lifetime_value,           -- ③ aggregate_data
        CASE WHEN SUM(o.total) > 10000 THEN 'VIP' -- ④ conditional_column
             ELSE 'Regular' END as tier
    FROM customers c                              -- Source table 1
    LEFT JOIN orders o ON c.customer_id = o.customer_id  -- ⑤ join_data
    WHERE c.status = 'active'                     -- ⑥ filter_data
    GROUP BY c.customer_id, c.name, c.email       -- ③ (part of aggregation)
END

-- Decomposition Result:
-- Sources: customers, orders
-- Operations: ①string ②null ③aggregate ④conditional ⑤join ⑥filter
-- Sequence: filter → join → aggregate → string → null → conditional
```

### Step 2: Assign to Medallion Layers (MANDATORY)

Legacy scripts often do everything in one pass. The accelerator uses **Bronze → Silver → Gold**. You MUST split the logic appropriately:

```
┌─────────────────────────────────────────────────────────────────┐
│ MEDALLION LAYER ASSIGNMENT RULES                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  BRONZE (Raw Ingestion)                                         │
│  ├── Extract from source systems (DB, files, APIs)              │
│  ├── Minimal transformation (column name cleansing only)        │
│  ├── NO business logic, NO joins, NO aggregations               │
│  └── One Bronze table per source table/file                     │
│                                                                 │
│  SILVER (Validated & Cleaned)                                   │
│  ├── Read from Bronze tables                                    │
│  ├── Apply cleansing: nulls, types, standardization             │
│  ├── Apply filtering (remove invalid records)                   │
│  ├── Apply joins (combine related entities)                     │
│  ├── Apply deduplication                                        │
│  ├── Data Quality checks (validate_condition)                   │
│  └── One Silver table per logical entity                        │
│                                                                 │
│  GOLD (Business-Ready)                                          │
│  ├── Read from Silver tables                                    │
│  ├── Apply aggregations                                         │
│  ├── Apply business calculations (derived_column)               │
│  ├── Apply dimensional modeling (surrogate keys)                │
│  ├── Apply business rules (conditional_column)                  │
│  └── Organized by business domain or consumption pattern        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Example Layer Assignment:**
```
Legacy sp_CustomerSummary → Accelerator Metadata:

┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   BRONZE    │     │   SILVER    │     │        GOLD         │
├─────────────┤     ├─────────────┤     ├─────────────────────┤
│ customers   │────▶│ customers   │────▶│ customer_summary    │
│ (raw)       │     │ (cleaned)   │     │                     │
│             │     │ • filter    │     │ • join (from Silver)│
│ orders      │────▶│ • null fix  │     │ • aggregate         │
│ (raw)       │     │ • type cast │     │ • conditional_column│
│             │     │             │     │ • string_functions  │
└─────────────┘     │ orders      │     └─────────────────────┘
                    │ (cleaned)   │
                    │ • filter    │
                    │ • dedup     │
                    └─────────────┘

Table_IDs:
- Bronze: 1001 (customers), 1002 (orders)
- Silver: 1101 (customers), 1102 (orders)  
- Gold:   1201 (customer_summary)
```

### Step 3: Map Operations to Transformations (Quick Mapping Table)
| Legacy Code Pattern | Accelerator Transformation | Notes |
|---------------------|---------------------------|-------|
| **Column Operations** |||
| `AS new_name`, `RENAME COLUMN` | `columns_to_rename` | SQL column aliasing |
| `.rename(columns={...})` | `columns_to_rename` | Pandas column rename |
| `SELECT col1, col2` (subset) | `select_columns` | Explicit column selection |
| `DROP COLUMN`, `.drop(columns=[...])` | `remove_columns` | Remove unwanted columns |
| **Calculated Fields** |||
| `col1 + col2`, arithmetic expressions | `derived_column` | Simple Spark SQL expressions |
| `.assign()`, `df['new'] = ...` | `derived_column` | Pandas calculated columns |
| `CONCAT()`, `+`, `\|\|` | `concat_columns` | String concatenation |
| `CASE WHEN...THEN...ELSE...END`, `IIF()` | `conditional_column` | Conditional logic |
| **Filtering & Deduplication** |||
| `WHERE condition` | `filter_data` | Row filtering |
| `.query()`, `.loc[condition]`, `df[df.col > x]` | `filter_data` | Pandas filtering |
| `DISTINCT`, `GROUP BY` (for dedup) | `drop_duplicates` | Remove duplicate rows |
| `.drop_duplicates()` | `drop_duplicates` | Pandas deduplication |
| **Joins & Unions** |||
| `JOIN`, `LEFT JOIN`, `INNER JOIN` | `join_data` | All SQL join types |
| `.merge()`, `pd.merge()` | `join_data` | Pandas merge operations |
| `UNION`, `UNION ALL` | `union_data` | Combine row sets |
| `pd.concat()`, `.append()` | `union_data` | Pandas concatenation |
| **Aggregations** |||
| `GROUP BY` with `SUM`, `AVG`, `COUNT`, etc. | `aggregate_data` | SQL aggregations |
| `.groupby().agg()`, `.groupby().sum()` | `aggregate_data` | Pandas groupby |
| **Window Functions** |||
| `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` | `add_window_function` | Ranking functions |
| `SUM() OVER(...)`, `AVG() OVER(...)`, `COUNT() OVER(...)` | `add_window_function` | Analytic window functions |
| `.groupby().shift()`, `.groupby().rank()` | `add_window_function` / `derived_column` | Pandas window operations |
| `SUM() OVER(...)`, `AVG() OVER(...)`, `COUNT() OVER(...)` | `add_window_function` | Analytic window functions |
| `.groupby().shift()`, `.groupby().rank()` | `add_window_function` / `derived_column` | Pandas window operations |
| **Data Type Conversion** |||
| `CAST()`, `CONVERT()`, `::type` | `change_data_types` | SQL type conversion |
| `.astype()` | `change_data_types` | Pandas type conversion |
| **Null Handling** |||
| `COALESCE()`, `ISNULL()`, `NVL()` | `apply_null_handling` | Replace nulls with values |
| `.fillna()`, `.replace({np.nan: ...})` | `apply_null_handling` | Pandas null handling |
| **Value Replacement** |||
| `REPLACE()`, `CASE WHEN col='old' THEN 'new'` | `replace_values` | Value substitution |
| `.replace()`, `.str.replace()` | `replace_values` | Pandas replacement |
| **String Functions** |||
| `UPPER()`, `LOWER()`, `TRIM()`, `LTRIM()`, `RTRIM()` | `string_functions` | Basic string ops |
| `.str.upper()`, `.str.lower()`, `.str.strip()` | `string_functions` | Pandas string methods |
| `SUBSTRING()`, `CHARINDEX()`, `PARSENAME()` | `split_column` | String parsing/splitting |
| `.str.split()` | `split_column` | Pandas string split |
| **Date/Time Operations** |||
| `DATEPART()`, `YEAR()`, `MONTH()`, `DAY()` | `transform_datetime` | Date extraction |
| `DATEADD()`, `DATEDIFF()` | `transform_datetime` | Date arithmetic |
| `.dt.year`, `.dt.month`, `.dt.strftime()` | `transform_datetime` | Pandas datetime accessor |
| **Reshaping** |||
| `PIVOT` | `pivot_data` | Rows to columns |
| `.pivot()`, `.pivot_table()` | `pivot_data` | Pandas pivot |
| `UNPIVOT`, `CROSS APPLY VALUES` | `unpivot_data` | Columns to rows |
| `.melt()`, `.stack()` | `unpivot_data` | Pandas unpivot |
| **Array/Struct Operations** |||
| `CROSS APPLY STRING_SPLIT()`, `OPENJSON()` | `explode_array` | Flatten arrays |
| `.explode()` | `explode_array` | Pandas/Spark explode |
| `OPENJSON() WITH (...)` (nested) | `flatten_struct` | Flatten nested structures |
| `pd.json_normalize()` | `flatten_struct` | Pandas JSON flattening |
| **Data Masking** |||
| `SUBSTRING()` + `REPLICATE('X')` patterns | `mask_sensitive_data` | PII masking |
| Custom masking functions | `mask_sensitive_data` | Data obfuscation |
| **Sorting** |||
| `ORDER BY` | `sort_data` | Row ordering |
| `.sort_values()` | `sort_data` | Pandas sorting |
| **Dimensional Modeling** |||
| Identity columns, sequences | `create_surrogate_key` | Generate SK |
| Lookup table joins for FK→SK | `attach_dimension_surrogate_key` | Fact table SK lookup |
| **Hash Generation** |||
| `HASHBYTES()`, `MD5()`, `SHA2()` | `add_row_hash` | Row hashing |
| **Entity Matching** |||
| `fuzzywuzzy`, `dedupe`, Soundex matching | `entity_resolution` | Fuzzy matching |
| **Text Normalization** |||
| Multi-step text cleaning (trim, lower, strip accents) | `normalize_text` | Standardize text |
| **Custom Logic** |||
| Complex stored procedures, Python functions | `custom_transformation_function` or `custom_table_ingestion_function` | **LAST RESORT** |
| API calls, ML models, proprietary formats | `custom_transformation_function` or `custom_staging_function` | **LAST RESORT** |
| Custom staging with full write control | `custom_staging_function` with `Processing_Method='batch'` | **LAST RESORT** |
| Existing notebooks/dataflows | `execute_databricks_notebook` / `execute_databricks_job` | Execute existing items |

### Step 4: Conversion Workflow

```
┌────────────────────────────────────────────────────────────────────────┐
│ LEGACY CODE CONVERSION WORKFLOW                                        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. DECOMPOSE (Step 1 above)                                           │
│     └── List every operation in the legacy code                        │
│                                                                        │
│  2. LAYER (Step 2 above)                                               │
│     └── Assign each operation to Bronze, Silver, or Gold               │
│                                                                        │
│  3. MAP (Step 3 - Quick Mapping Table)                                 │
│     └── Find the accelerator transformation for each operation         │
│                                                                        │
│  4. SEQUENCE                                                           │
│     └── Order transformations within each layer by instance_number     │
│         Filter → Join → Transform → Aggregate → Derive                 │
│                                                                        │
│  5. GENERATE                                                           │
│     └── Create metadata SQL for each Table_ID                          │
│                                                                        │
│  6. VERIFY (🔴 CRITICAL)                                               │
│     └── Check: Does EVERY decomposed operation appear in metadata?     │
│     └── If NO → You missed something → Go back to step 3               │
│                                                                        │
│  7. VALIDATE                                                           │
│     └── Run validator before delivery                                  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Step 5: Verify Conversion Completeness

**🔴 MANDATORY VERIFICATION:** Before delivering converted metadata, create this checklist:

```
📋 CONVERSION VERIFICATION:
┌──────────────────────────────┬──────────────────────────┬─────────┐
│ Legacy Operation             │ Metadata Coverage                  │ Status  │
├──────────────────────────────┼────────────────────────────────────┼─────────┤
│ SELECT UPPER(name)           │ string_functions #3                │ ✅      │
│ LEFT JOIN orders             │ join_data #5                       │ ✅      │
│ WHERE status = 'active'      │ filter_data #1                     │ ✅      │
│ GROUP BY customer_id         │ aggregate_data #7                  │ ✅      │
│ Recursive CTE for hierarchy  │ custom_transformation_function #10 │ ✅      │
│ [operation from source]      │ [where in metadata]      │ ✅/❌   │
└──────────────────────────────┴──────────────────────────┴─────────┘
Coverage: 5/5 = 100% ✅
```

**If any operation shows ❌, you have an incomplete conversion!**

### Example: Full Conversion

**Legacy SQL (single-pass script):**
```sql
SELECT 
    UPPER(c.customer_name) as customer_name,
    COALESCE(c.email, 'unknown@example.com') as email,
    SUM(o.order_total * 1.1) as total_with_tax,
    COUNT(o.order_id) as order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.status = 'active' AND o.order_date >= '2024-01-01'
GROUP BY c.customer_id, c.customer_name, c.email
ORDER BY total_with_tax DESC
```

**Decomposition:**
| # | Operation | Type | Layer |
|---|-----------|------|-------|
| 1 | Read customers | Source | Bronze |
| 2 | Read orders | Source | Bronze |
| 3 | Filter customers (status='active') | filter_data | Silver |
| 4 | Filter orders (date >= 2024) | filter_data | Silver |
| 5 | Join customers + orders | join_data | Gold |
| 6 | Calculate order_total * 1.1 | derived_column | Gold |
| 7 | Aggregate SUM, COUNT | aggregate_data | Gold |
| 8 | UPPER(customer_name) | string_functions | Gold |
| 9 | COALESCE(email) | apply_null_handling | Gold |
| 10 | Sort by total | sort_data | Gold |

**Resulting Metadata Structure:**
```
Bronze (Table_IDs 1001-1002):
  1001: customers → bronze.dbo.customers (raw extract)
  1002: orders → bronze.dbo.orders (raw extract)

Silver (Table_IDs 1101-1102):
  1101: bronze.dbo.customers → silver.dbo.customers
        Transforms: filter_data (status='active')
  1102: bronze.dbo.orders → silver.dbo.orders  
        Transforms: filter_data (order_date >= '2024-01-01')

Gold (Table_ID 1201):
  1201: silver tables → gold.dbo.customer_order_summary
        Transforms (in order):
        #1: join_data (customers + orders)
        #2: derived_column (order_total * 1.1)
        #3: aggregate_data (SUM, COUNT, GROUP BY)
        #4: string_functions (UPPER)
        #5: apply_null_handling (COALESCE)
        #6: sort_data (ORDER BY)
```

