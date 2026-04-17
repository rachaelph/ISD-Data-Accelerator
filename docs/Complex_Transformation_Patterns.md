# Complex Transformation Patterns

> Last updated: 2026-03-29

Use this file after the canonical decision docs have already established that the requirement is too complex for Tier 1 built-ins.

Canonical decision references:
- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for built-in vs custom vs standalone escalation
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for choosing the correct custom function type
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for metadata authoring rules and legacy code conversion
- [TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md) for all 34 built-in transformation SQL examples

This file should stay focused on recognizable complex patterns and why they usually need Tier 2 custom logic. It should not be the primary decision authority.

---

## Patterns That Usually Need Tier 2 Custom Logic

### 1. Recursive Hierarchies (Parent-Child Trees)

**Recognize:** `WITH RECURSIVE`, self-joins with depth tracking, `CONNECT BY PRIOR`

**Why built-ins fail:** No recursion support. `join_data` can't iterate until condition met.

**Solution:**
```python
def flatten_hierarchy(new_data: DataFrame, metadata: dict, spark) -> DataFrame:
    hierarchy = spark.read.table("silver.dbo.dim_hierarchy")
    result = base_level
    for i in range(max_depth):
        next_level = result.join(hierarchy, ...)
        if next_level.count() == 0: break
        result = result.union(next_level)
    return result
```

---

### 2. Running Totals / Cumulative with Reset

**Recognize:** `SUM() OVER (PARTITION BY ... ORDER BY ... ROWS UNBOUNDED PRECEDING)` with reset conditions

**Why built-ins fail:** `add_window_function` can't handle conditional resets mid-partition.

**Solution:** Use custom_transformation_function with explicit window logic and `when()` conditions.

---

### 3. Multi-Pass Aggregation (Rollups with Dummy Records)

**Recognize:** INSERT base records, then INSERT adjustment/dummy records based on first insert

**Why built-ins fail:** Built-ins are single-pass. Can't reference output of previous step.

**Solution:**
```python
def calculate_with_dummies(new_data: DataFrame, metadata: dict, spark) -> DataFrame:
    base_records = new_data  # First pass result
    dummy_records = calculate_adjustments(base_records, hierarchy)
    return base_records.union(dummy_records)
```

---

### 4. Conditional Row Generation (Explode with Logic)

**Recognize:** Generate N rows per input based on date ranges, effective periods, or business rules

**Why built-ins fail:** `explode_array` needs pre-existing array. Can't generate rows from scalar logic.

**Solution:** Use `flatMap` or `explode(sequence(...))` in custom_transformation_function.

---

### 5. Lookup with Fallback Chain

**Recognize:** Try Table A, if no match try Table B, if no match try Table C, else default

**Why built-ins fail:** `join_data` is single join. `coalesce` works for columns, not table fallbacks.

**Solution:**
```python
def lookup_with_fallback(new_data: DataFrame, metadata: dict, spark) -> DataFrame:
    result = new_data.join(table_a, ..., "left")
    unmatched = result.filter(col("key_a").isNull())
    matched_b = unmatched.join(table_b, ..., "left")
    # Continue chain...
    return matched.union(matched_b).union(...)
```

---

### 6. Pivoting with Dynamic Columns

**Recognize:** Pivot where column names come from data, not known at design time

**Why built-ins fail:** `pivot_data` requires explicit `pivot_column` values.

**Solution:** Query distinct values first, then pivot dynamically in custom_transformation_function.

---

### 7. Data Quality with Row-Level Scoring

**Recognize:** Calculate DQ score per row, route to different targets based on score

**Why built-ins fail:** DQ checks are pass/fail per condition, not cumulative scoring.

**Solution:** Calculate score column, then use `filter_data` or custom routing logic.

---

### 8. Late-Arriving Dimension Handling

**Recognize:** Fact arrives before dimension. Need placeholder, then backfill.

**Why built-ins fail:** Requires reading existing target + conditional insert/update logic.

**Solution:** custom_transformation_function that checks target table state before deciding action.

---

## Mixing Built-ins and custom_transformation_function

**Pattern:** Use built-ins for standard prep, custom_transformation_function for complex middle step, built-ins for final cleanup.

```sql
-- Instance 1-3: Built-in prep
(101, 'data_transformation_steps', 'derived_column', 1, ...),
(101, 'data_transformation_steps', 'join_data', 2, ...),
(101, 'data_transformation_steps', 'filter_data', 3, ...),

-- Instance 4: Complex logic
(101, 'data_transformation_steps', 'custom_transformation_function', 4, 'functions_to_execute', 'handle_hierarchy'),
(101, 'data_transformation_steps', 'custom_transformation_function', 4, 'files_to_run', 'NB_MyCustomLogic'),

-- Instance 5-6: Built-in cleanup
(101, 'data_transformation_steps', 'select_columns', 5, ...),
(101, 'data_transformation_steps', 'drop_duplicates', 6, ...)
```

---

### 9. Split-Transform-Union (Different Logic per Subset)

**Recognize:** IF type='A' do X, IF type='B' do Y, UNION ALL results

**Why built-ins fail:** Would require separate Table_IDs per branch = multiple reads/writes. Inefficient.

**Solution:** Single custom_transformation_function that filters, transforms each subset differently, unions result.

---

### 10. Iterative Convergence / Graph Algorithms

**Recognize:** PageRank, shortest path, clustering until stable, Newton-Raphson

**Why built-ins fail:** Single-pass only. Can't loop until convergence condition met.

**Solution:** custom_transformation_function with while loop checking delta threshold.

---

### 11. External API Calls

**Recognize:** Geocoding, address validation, enrichment from REST services

**Why built-ins fail:** No HTTP/API support.

**Solution:** custom_transformation_function using `requests` or Spark UDF with API calls.

---

### 12. ML Model Inference

**Recognize:** Score rows with trained model, sentiment analysis, predictions

**Why built-ins fail:** No ML support in built-ins.

**Solution:** custom_transformation_function loading model via MLflow, sklearn, or Spark ML.

---

## When NOT to Use custom_transformation_function

| Scenario | Use Instead |
|----------|-------------|
| Simple joins | `join_data` |
| Column calculations | `derived_column` |
| Type conversions | `change_data_types` |
| Filtering rows | `filter_data` |
| Standard aggregations | `aggregate_data` |
| Window functions (non-conditional) | `add_window_function` |
| String manipulation | `string_functions`, `normalize_text` |

If a built-in transformation already covers the requirement, do not use this file as justification to escalate. Follow [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) and keep the work in Tier 1.
