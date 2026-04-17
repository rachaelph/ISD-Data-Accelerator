# Data Transformation Guide

> Last updated: 2026-03-29

Use this file when the question is about how to apply transformations, custom functions, data quality checks, filters, CDF, entity resolution, or when choosing a transformation approach for a specific scenario.

This guide covers all transformation scenarios - custom functions, filters, data quality checks, CDF, entity resolution, pre-built transformations, and the transformation reference.

Use the thinner canonical docs when the question is really about contract or decision authority:
- [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for accepted metadata values and configuration rules
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for metadata authoring patterns and SQL structure
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for custom-function choice
- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for escalation from built-ins to custom code

---

## Table of Contents

- [Using a custom function as a data transformation step](#using-a-custom-function-as-a-data-transformation-step)
- [Transform data with filters and data quality checks](#transform-data-with-filters-and-data-quality-checks)
- [Transform delta tables with custom ingestion and data cleansing](#transform-delta-tables-with-custom-ingestion-and-data-cleansing)
- [Transform delta tables using Change Data Feed (CDF)](#transform-delta-tables-using-change-data-feed-cdf)
- [Execute a Notebook, external job, or Pipeline as a standalone activity](#execute-a-notebook-dataflow-gen2-or-pipeline-as-a-standalone-activity)
- [Entity Resolution: Fuzzy Matching, Phonetic Matching, and Percent Difference](#entity-resolution-fuzzy-matching-phonetic-matching-and-percent-difference)
- [Execute Pre-Built Transformations](#execute-pre-built-transformations)
- [Transformation Reference Guide](#transformation-reference-guide)
- [Complete Transformation Example](#complete-transformation-example)

---

## Data Transformation

### Using a custom function as a data transformation step

**Keywords:** custom transformation function, custom_transformation_function, custom code, PySpark function, business logic, Tier 2, custom notebook, transformation notebook, NB_Custom

> 📌 **Not sure this is the right custom function?** See the [Custom Function Decision Guide](#custom-function-decision-guide) selection rules.

#### Overview
Custom transformation functions allow you to apply complex business logic that goes beyond pre-built transformations. This is ideal for proprietary calculations, complex data reshaping, or integration with external systems.

#### Use Cases
- **Complex business calculations** requiring multiple steps or custom algorithms
- **External API integration** to enrich data with third-party information
- **Custom data validation** with business-specific rules
- **Multi-table aggregations** with complex logic
- **Machine learning model inference** as part of the pipeline

#### Prerequisites
1.  Create a new PySpark notebook in either your feature workspace or your development workspace
2.  Create a function in the notebook that has the same parameters and return value as the below example function 
    *   Reference custom function example [here](resources/Custom_Transformation_Function.py)

#### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Example Configuration
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('custom_transformation_function', 1, 40, 'gold', 'dbo.oracle_Sales3', 'Prod_Id, Cust_Id, Time_Id, Channel_Id, Promo_Id', 'batch', 1);

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (40, 'source_details', 'table_name', 'Silver.dbo.oracle_Sales')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Multiple custom functions can be chained (executed in order listed)
    -- Comma-separated list of function names
    (40, 'data_transformation_steps', 'custom_transformation_function', 1, 'functions_to_execute', 'calculate_revenue_metrics,apply_business_rules'),
    -- Corresponding notebooks containing the functions (executed in order to import)
    -- Comma-separated list of file names (without .py extension)
    (40, 'data_transformation_steps', 'custom_transformation_function', 1, 'files_to_run', 'custom_revenue_calculations,custom_business_rules')
```

#### Custom Function Requirements

Your custom transformation function must follow specific parameter and return value signatures.

**For detailed examples and full code implementations, see:**
- [Custom Transformation Function Example](resources/Custom_Transformation_Function.py)

The example notebook includes:
- Complete function signature with all required parameters
- Revenue calculation implementations
- Business rules application patterns
- Error handling and logging
- DataFrame transformation best practices

#### Multiple Configuration Instances

You can apply multiple custom function steps by increasing the `Configuration_Name_Instance_Number`:

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- First custom function step
    (40, 'data_transformation_steps', 'custom_transformation_function', 1, 'functions_to_execute', 'calculate_revenue_metrics'),
    (40, 'data_transformation_steps', 'custom_transformation_function', 1, 'files_to_run', 'custom_revenue_calculations'),
    -- Second custom function step (runs after first)
    (40, 'data_transformation_steps', 'custom_transformation_function', 2, 'functions_to_execute', 'apply_ml_predictions'),
    (40, 'data_transformation_steps', 'custom_transformation_function', 2, 'files_to_run', 'custom_ml_inference')
```

#### Best Practices
1. **Imports inside functions**: All `import` statements must be placed **inside your function body**, not at the top of the notebook. Custom notebooks execute via `run_cell()` in the shared IPython namespace — top-level imports pollute `globals()` and can shadow critical symbols like the `f` alias for `pyspark.sql.functions`. The CI/CD validation script enforces this rule.
2. **No wildcard imports**: Never use `from X import *` — use explicit imports (e.g., `from pyspark.sql.types import StructType, StructField, StringType`).
3. **Keep functions focused**: Each function should do one thing well
4. **Use configuration dictionaries**: Access configuration values instead of hardcoding
5. **Add error handling**: Use try-except blocks for robust execution
6. **Log progress**: Use print statements for debugging (appears in pipeline logs)
7. **Validate schema**: Check DataFrame schema before and after transformation
8. **Test independently**: Test functions outside the pipeline first

### Transform data with filters and data quality checks

**Keywords:** filter, data quality, DQ, quarantine, warn, fail, null check, not null, range check, threshold, filter_data, reference table, minimum records, data validation, quality rules

#### Overview
Apply filtering logic to remove unwanted data and implement data quality checks to ensure data meets business requirements before loading. This pattern supports multiple filter conditions, reference table lookups, and minimum record validation.

#### Use Cases
- **Filter by business unit** to create department-specific datasets
- **Validate foreign keys** against reference tables
- **Validate data formats** using predefined patterns (phone numbers, emails, dates, etc.)
- **Enforce data quality thresholds** with minimum record counts
- **Quarantine invalid data** for review instead of failing the pipeline
- **Detect statistical anomalies** in numeric columns using z-score or IQR methods
- **Enforce null constraints** on critical columns
- **Validate value ranges** for numeric and date fields
- **Monitor data freshness** to catch stale data before downstream processing
- **Validate schema contracts** to catch missing columns or type mismatches before data is written
- **Detect schema drift** automatically to track and optionally block unexpected schema changes

#### Available Data Quality Check Types

The framework provides **10 built-in data quality check types**, all configured via `Data_Pipeline_Metadata_Advanced_Configuration` with `Configuration_Category = 'data_quality'`.

> **Canonical reference:** See the `data_quality` section in [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for the complete list of check types, required attributes, and accepted values.

The 10 types are: `validate_condition`, `validate_referential_integrity`, `validate_pattern`, `validate_batch_size`, `validate_not_null`, `validate_unique`, `validate_range`, `validate_freshness`, `validate_completeness`, and `validate_anomaly`.

> **All check types** support `if_not_compliant` (`warn`, `mark`, `quarantine`, `fail`) and require a `message` attribute.
> See [detailed documentation for each check type](#validate_not_null---null-value-validation) below.

#### Built-in Schema Validation (runs automatically)

> 🛡️ **In addition to the 10 configurable DQ check types above, the accelerator provides automatic schema validation that runs as part of every pipeline — no extra configuration required.**

| Capability | Function | When It Runs | What It Does |
|-----------|----------|-------------|-------------|
| **Schema Contract Validation** | `validate_table_schema_contract` | Automatically during Delta-to-Delta and Parquet ingestion when a `schema` is defined in `source_details` | Fails the pipeline if the source DataFrame is missing expected columns or has wrong data types. Extra columns are allowed (forward-compatible). |
| **Schema Change Detection** | `_handle_schema_change_validation` | Automatically after every write | Detects added/removed/changed columns vs. the previously logged schema. Logs changes to `Schema_Logs` and `Schema_Changes`. |
| **Schema Drift Enforcement** | `fail_on_new_schema` config | When configured in `source_details` | Fails the pipeline if the source schema has changed since last run. Prevents unexpected schema drift from propagating downstream. |

> **How to configure:** Set the `schema` attribute in `source_details` to define your expected columns/types. Optionally set `fail_on_new_schema = 'true'` to enforce strict schema stability. See [Schema Enforcement Behavior by File Format](#schema-enforcement-behavior-by-file-format) for format-specific details.

#### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Example Configuration with Multiple Quality Checks
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('query', 1, 41, 'gold', 'dbo.oracle_Sales3', 'Prod_Id, Cust_Id, Time_Id, Channel_Id, Promo_Id', 'batch', 1);

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (41, 'source_details', 'table_name', 'Silver.dbo.oracle_Sales'),
    -- Overwrite target table on each run (see Data Modeling section for all merge_type options)
    (41, 'target_details', 'merge_type', 'overwrite')
    
    -- OPTIONAL: Enable incremental processing with watermark
    -- Use comma-separated values for multiple watermark columns (framework uses OR logic)
    -- NOTE: Multiple watermark columns only supported for Delta tables (Bronze/Silver/Gold), not external databases
    -- (41, 'watermark_details', 'column_name', 'ModifiedDate, CreatedDate'),
    -- (41, 'watermark_details', 'data_type', 'datetime')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- 1. FILTER DATA: Uses Spark SQL syntax, supports multiple columns and functions
    -- Can have multiple filter steps by increasing Configuration_Name_Instance_Number
    (41, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'BusinessUnit = ''East'' '),
    
    -- 2. DATA QUALITY: Validate condition with action on non-compliance
    -- Options: warn (log warning, default), mark (add row-level DQ detail marker), quarantine (move to separate table), fail (stop pipeline)
    -- Every DQ check MUST include a 'message' attribute
    (41, 'data_quality', 'validate_condition', 2, 'condition', 'left(CUST_ID, 1) = ''6'' '),
    (41, 'data_quality', 'validate_condition', 2, 'if_not_compliant', 'warn'),
    (41, 'data_quality', 'validate_condition', 2, 'message', 'Customer ID must start with 6'),
    
    -- Another validation condition with different action
    (41, 'data_quality', 'validate_condition', 3, 'condition', 'right(CUST_ID, 1) = ''1'' '),
    (41, 'data_quality', 'validate_condition', 3, 'if_not_compliant', 'quarantine'),
    (41, 'data_quality', 'validate_condition', 3, 'message', 'Customer ID must end with 1'),
    
    -- 3. REFERENTIAL INTEGRITY: Validate foreign keys exist in reference tables
    -- Ensures data integrity across related tables
    (41, 'data_quality', 'validate_referential_integrity', 4, 'current_table_column_name', 'CUST_ID'),
    (41, 'data_quality', 'validate_referential_integrity', 4, 'reference_table_column_name', 'CUST_ID'),
    (41, 'data_quality', 'validate_referential_integrity', 4, 'reference_table_name', 'bronze.dbo.oracle_Customers'),
    (41, 'data_quality', 'validate_referential_integrity', 4, 'if_not_compliant', 'warn'),
    (41, 'data_quality', 'validate_referential_integrity', 4, 'message', 'Customer must exist in oracle_Customers'),
    
    -- 4. PATTERN VALIDATION: Validate data against common patterns (phone, email, etc.)
    -- Uses predefined regex patterns for common data formats
    (41, 'data_quality', 'validate_pattern', 5, 'column_name', 'PhoneNumber'),
    (41, 'data_quality', 'validate_pattern', 5, 'pattern_type', 'us_phone'),
    (41, 'data_quality', 'validate_pattern', 5, 'if_not_compliant', 'quarantine'),
    (41, 'data_quality', 'validate_pattern', 5, 'allow_null', 'true'),
    (41, 'data_quality', 'validate_pattern', 5, 'message', 'Phone number must be valid US format'),
    
    -- 5. BATCH SIZE VALIDATION: Ensure each batch has sufficient data
    -- Protects against incomplete extractions
    (41, 'data_quality', 'validate_batch_size', 6, 'min_rows', '100'),
    (41, 'data_quality', 'validate_batch_size', 6, 'if_not_compliant', 'fail'),
    (41, 'data_quality', 'validate_batch_size', 6, 'message', 'Batch must contain at least 100 rows')
```

#### Filter Logic Examples

**Simple Equality Filter:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'Status = ''Active'' ')
```

**Multiple Conditions with AND/OR:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'Status = ''Active'' AND Region IN (''East'', ''West'')')
```

**Date Range Filter:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'OrderDate >= ''2024-01-01'' AND OrderDate < ''2025-01-01'' ')
```

**Using Spark SQL Functions:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'year(OrderDate) = 2024 AND month(OrderDate) >= 6')
```

**String Pattern Matching:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'ProductName LIKE ''%Widget%'' ')
```

**Null/Not Null Checks:**
```sql
('41', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'Email IS NOT NULL AND Email != '''' ')
```

#### Data Quality Actions

This guide keeps only the workflow-level behavior here. The canonical rule surface now lives in:

- [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for valid `data_quality` configuration names, attributes, and accepted values
- [FAQ.md](FAQ.md) for operational troubleshooting and common validation questions
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for larger authoring examples

Quick operating rules:

- Every data-quality check needs a `message` attribute.
- `warn`, `mark`, `quarantine`, and `fail` are the only response modes.
- `quarantine` removes rows from the main output.
- `mark` keeps rows in the main output and annotates them.
- Multi-column checks generate per-column detail even when configured in one instance.

Recommended pattern:

- Use `filter_data` for business-scope filtering.
- Use `data_quality` checks for validation, quarantine, and observability.
- Prefer separate `validate_condition` instances when you need root-cause clarity.

#### Understanding AND Logic in validate_condition

If you put multiple predicates in one `validate_condition` with `AND`, the row fails if any predicate fails. Use one combined check only when you intentionally want one all-or-nothing rule.

If you need separate auditability, use separate instances instead of one combined expression.

#### Multiple Table Lookups

Multiple referential-integrity checks in the same table flow are supported. Keep each lookup in its own instance number so failures stay attributable to the correct relationship.

#### Pattern Validation

Pattern validation is still appropriate for emails, phones, GUIDs, ZIP codes, and similar standardized formats, but the authoritative pattern list now belongs in [METADATA_REFERENCE.md](METADATA_REFERENCE.md).

Use pattern validation when the requirement is "does this field match a known format?" Use `filter_data` when the requirement is broader business logic.

##### validate_not_null - NULL Value Validation

Use this for required fields and business keys. Prefer one grouped instance when the same action applies to several columns.

##### validate_unique - Uniqueness Validation

Use this for business-key uniqueness, especially when the target primary key does not fully capture the business rule.

##### validate_range - Value Range Validation

Use this for numeric limits, bounded percentages, and valid date windows.

##### validate_freshness - Data Freshness Validation

Use this for SLA-style recency checks. This is batch-level validation, so it is best paired with `warn` or `fail`, not row isolation logic.

##### validate_completeness - Column Completeness Validation

Use this to track non-null percentages over time and to guard critical columns with minimum fill thresholds.

##### validate_anomaly - Statistical Anomaly Detection

Use this when the incoming data should be evaluated against a historical baseline in the target table. Reserve it for columns where the extra cost and interpretability tradeoff are worth it.

#### Best Practices
1. **Order filters strategically**: Apply most restrictive filters first to reduce data volume early
2. **Use quarantine for investigation**: When unsure if data should fail pipeline
3. **Set realistic minimum thresholds**: Base on historical batch sizes
4. **Combine with data cleansing**: Apply filters after cleansing steps for better accuracy
5. **Monitor quarantine tables**: Regularly review quarantined data to identify systemic issues

### Transform delta tables with custom ingestion and data cleansing

**Keywords:** custom table ingestion, custom_table_ingestion_function, data cleansing, column cleansing, whitespace, null normalization, standardize, clean data, internal table read

> 📌 **Not sure this is the right custom function?** See the [Custom Function Decision Guide](#custom-function-decision-guide) selection rules.

#### Overview
Combine custom ingestion logic with automated data cleansing steps to standardize data quality across the platform. This pattern supports custom data extraction with built-in cleansing for column naming, whitespace trimming, and null value normalization.

#### Use Cases
- **Custom data extraction** from delta tables with complex query logic
- **Automated standardization** of column names across datasets
- **Consistent null handling** replacing blank strings with NULL
- **Whitespace normalization** for string data
- **Watermarking custom extractions** for incremental processing

#### Prerequisites
1.  Create a new PySpark notebook in either your feature workspace or your development workspace
2.  Create a custom ingestion function that has the same parameters and return value as the example function
    *   Reference custom function example [here](resources/Custom_Table_Ingestion_Function.py)

#### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Example Configuration
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('query', 1, 50, 'gold', 'dbo.oracle_Sales3', 'Prod_Id, Cust_Id, Time_Id, Channel_Id, Promo_Id', 'batch', 1)

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Custom ingestion function details (for table/SQL-based extraction)
    (50, 'source_details', 'custom_table_ingestion_function', 'extract_sales_with_aggregations'),
    (50, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_sales_ingestion'),
    
    -- OPTIONAL: Enable watermarking with custom ingestion function
    -- When specified, the watermark value is passed to your custom function
    (50, 'watermark_details', 'table_name', 'Silver.dbo.oracle_Sales'),
    (50, 'watermark_details', 'column_name', 'ModifiedDate'),
    (50, 'watermark_details', 'data_type', 'datetime'),
    
    -- OPTIONAL: Use multiple watermark columns (comma-separated)
    -- Framework uses OR logic: extracts rows where ANY column > last watermark value
    -- Useful when data can be updated via multiple timestamp fields
    -- NOTE: Multiple watermark columns only supported for Delta tables (Bronze/Silver/Gold), not external databases
    -- (50, 'watermark_details', 'column_name', 'ModifiedDate, CreatedDate'),
    
    -- Column Name Cleansing Configuration
    -- Column name transformations (applied in order: trim → exact → regex → non-alphanumeric → case)
    (50, 'column_cleansing', 'trim', 'true'),
    (50, 'column_cleansing', 'apply_case', 'lower'),
    (50, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
    
    -- Data Value Cleansing Configuration
    -- Replace blank strings with NULL values in all string columns (default is '*' for Silver tables)
    -- Use '*' for all columns or comma-separated list for specific columns
    (50, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
    
    -- Trim leading/trailing whitespace from specified string columns (default is '*' for Silver)
    -- Use '*' for all columns or comma-separated list for specific columns
    (50, 'data_cleansing', 'trim_data_in_string_columns', '*')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Remove duplicate rows
    -- Mode is determined by presence of 'order_by':
    -- - If order_by is NOT specified: Exact duplicate removal (optionally on subset columns via 'column_name')
    -- - If order_by IS specified: Ordered deduplication - keeps one row per column_name based on order_by
    
    -- Example 1: Exact deduplication on all columns (column_name = '*')
    -- (50, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', '*')
    
    -- Example 2: Ordered deduplication: keep most recent per primary key
    -- (50, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'customer_id'),
    -- (50, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'ModifiedDate'),
    -- (50, 'data_transformation_steps', 'drop_duplicates', 1, 'order_direction', 'desc')
```

#### Custom Ingestion Function Requirements

Your custom ingestion function must follow specific parameter and return value signatures.

**For detailed examples and full code implementations, see:**
- [Custom Table Ingestion Function Example](resources/Custom_Table_Ingestion_Function.py)

The example notebook includes:
- Complete function signature with all required parameters
- Watermark-based incremental extraction
- Complex SQL query building patterns
- Business logic application
- Error handling and best practices

#### Data Cleansing Options Explained

**Column Name Transformations (`column_cleansing` category):**
Column names are transformed using a combination of independent boolean flags and pattern-matching options:
- **`trim`**: Removes leading/trailing whitespace from column names
- **`apply_case`**: Converts column names to specified case - `lower` (lowercase), `upper` (uppercase), or `title` (title case). Examples: `CustomerName` → `customername` (lower), `CUSTOMERNAME` (upper), or `Customer_Name` (title)
- **`replace_non_alphanumeric_with_underscore`**: Replaces special characters with underscores and collapses consecutive underscores (e.g., `Customer#ID$$Name` → `Customer_ID_Name`)
- **`exact_find`** + **`exact_replace`**: Replace exact string matches. `exact_find` accepts comma-separated values. `exact_replace` can be either a **single value** (replaces all finds with same value) or **comma-separated values with counts that exactly match** `exact_find` (positional 1:1 replacement). Mismatched counts will raise an error. Examples: find `ID,PK,FK` replace with `Key` (all become `Key`) OR find `ID,PK,FK` replace with `Identifier,PrimaryKey,ForeignKey` (exact positional match)
- **`regex_find`** + **`regex_replace`**: Pattern-based replacement (e.g., remove trailing digits)

**Execution order**: trim → exact → regex → non-alphanumeric → case

**Example transformation** (all flags enabled):
```
Input:    "  Customer#ID123  "
After:    "customer_key"    (trimmed, ID→Key, special chars removed, lowercase)
```

**replace_blank_with_null_in_string_columns:**
- Comma-separated list of columns to replace empty strings (`""`) with NULL, or `'*'` for all string columns
- Default for Silver layer: `'*'` (all string columns)
- Default for Bronze/Gold layers: `''` (no columns)
- Ensures consistent null representation
- Helps with downstream joins and aggregations
- Examples:
  - `'*'` - Replace blanks in all string columns
  - `'email,phone,address'` - Replace blanks only in specified columns
  - `''` - No replacement (keep empty strings as-is)

**trim_data_in_string_columns:**
- Comma-separated list of columns to trim leading/trailing whitespace, or `'*'` for all string columns
- Default for Silver layer: `'*'` (all string columns)
- Default for Bronze/Gold layers: `''` (no columns)
- Prevents matching issues due to whitespace
- Examples:
  - `'*'` - Trim all string columns
  - `'name,description,city'` - Trim only specified columns
  - `''` - No trimming (preserve whitespace)

#### Duplicate Removal Options

**Mode is determined by presence of `order_by`:**
- **If `order_by` is NOT specified**: Exact duplicate removal (optionally on subset columns via `column_name`)
- **If `order_by` IS specified**: PK-based deduplication with ordering

**Exact Mode (no `order_by`)**
- Removes rows where specified columns have identical values
- `column_name` is required: use `*` for all columns, or specify column names
- Keeps first occurrence of each unique row

```sql
-- Exact deduplication across all columns
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'column_name', '*')

-- Exact deduplication on specific columns only
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'column_name', 'customer_id,product_id')
```

**Ordered Mode (with `order_by`)**
- Removes duplicates based on `column_name` columns, keeping one row based on ordering
- `column_name` specifies which columns to deduplicate on
- `order_by` specifies columns for ordering (determines which row to keep)
- Optional `order_direction` to specify sort direction (`asc` or `desc`, default: `desc`)

```sql
-- Keep most recent per customer_id based on ModifiedDate (descending is default)
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'column_name', 'customer_id'),
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'order_by', 'ModifiedDate')

-- Keep record with lowest sequence number (ascending order)
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'column_name', 'order_id'),
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'order_by', 'SequenceNumber'),
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'order_direction', 'asc')

-- Multiple order columns with individual directions
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'column_name', 'customer_id'),
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'order_by', 'priority,modified_date'),
('50', 'data_transformation_steps', 'drop_duplicates', '1', 'order_direction', 'desc,desc')
```

#### Watermarking with Custom Ingestion

When you specify `watermark_details.table_name`, the framework:
1. Retrieves the last successful watermark value from the logging table
2. Passes it to your custom function as the `watermark_value` parameter
3. Your function is responsible for applying the watermark logic

This enables incremental processing with custom extraction logic.

#### Best Practices
1. **Apply cleansing at Silver layer**: Use default cleansing settings for Silver, minimal for Bronze/Gold
2. **Handle watermark nulls**: First run will have `watermark_value = None`
3. **Use exact deduplication cautiously**: Can be memory-intensive on large datasets
4. **Test custom functions independently**: Validate logic before integrating with pipeline
5. **Log watermark usage**: Print watermark value in custom function for debugging

### Transform delta tables using Change Data Feed (CDF)

**Keywords:** change data feed, CDF, CDC, change tracking, incremental, delta changes, _change_type, insert update delete, mirrored database, catalog table, cross-workspace

#### Overview
Use Delta Lake's Change Data Feed for incremental loading from Delta tables. CDF provides transaction-level change tracking without requiring business watermark columns, capturing all inserts, updates, and deletes at the storage layer.

#### Use Cases
- **Reliable incremental loading**: Capture all changes without business logic dependencies
- **Delete tracking**: Automatically capture deleted records (not possible with traditional watermarks)
- **Simplified configuration**: No need to identify or maintain watermark columns
- **Audit trail**: Complete change history with commit timestamps
- **Late-arriving data**: No concerns about out-of-order updates

#### Benefits Over Traditional Watermarks
| Feature | Change Data Feed | Traditional Watermarks |
|---------|------------------|------------------------|
| **Change tracking** | Transaction-level (all changes) | Column-level (requires updated timestamp) |
| **Delete capture** | ✅ Automatic | ❌ Requires soft-delete columns |
| **Configuration** | Minimal (just enable CDF) | Must identify watermark columns |
| **Reliability** | ✅ Storage-level guarantees | ⚠️ Depends on business logic |
| **Observability** | `_commit_version` tracked for watermarking | Business column values |
| **Late data handling** | ✅ Automatic | ⚠️ Requires careful design |

#### Prerequisites
1. **Enable Change Data Feed on source table**:
   ```sql
   -- For new tables
   CREATE TABLE silver.dbo.sales (
       sale_id INT,
       amount DECIMAL(10,2),
       sale_date DATE
   )
   TBLPROPERTIES (delta.enableChangeDataFeed = true);
   
   -- For existing tables
   ALTER TABLE silver.dbo.sales 
   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
   ```

2. **Verify CDF is enabled**:
   ```sql
   DESCRIBE DETAIL silver.dbo.sales;
   -- Look for: enableChangeDataFeed = true in properties
   ```

#### Configuration Steps
1. Update/Enter the appropriate values in the SQL statements below
2. Execute the SQL in the Metadata Warehouse
3. Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Example Configuration
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('SalesWithCDF', 1, 60, 'gold', 'dbo.sales_enriched', 'sale_id', 'batch', 1)

-- Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Source table (must have CDF enabled)
    (60, 'source_details', 'table_name', 'silver.dbo.sales'),
    
    -- Enable Change Data Feed
    (60, 'watermark_details', 'use_change_data_feed', 'true'),
    
    -- Target merge strategy
    (60, 'target_details', 'merge_type', 'merge'),
    
    -- Optional: Apply data cleansing
    (60, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
    (60, 'data_cleansing', 'trim_data_in_string_columns', '*')
```

#### How It Works

**Initial Load (First Run)**:
1. Framework reads all data from the source table using CDF
2. Extracts `_commit_version` from the last change
3. Stores version number in logging table as the new watermark

**Incremental Loads (Subsequent Runs)**:
1. Framework reads only changes since last `_commit_version`
2. Receives DataFrame with CDF metadata columns:
   - `_change_type`: `insert`, `update_preimage`, `update_postimage`, `delete`
   - `_commit_version`: Delta table version number (used for watermarking)
   - `_commit_timestamp`: Transaction timestamp (available for logging/observability)
3. Applies transformations and data quality checks
4. Updates watermark with latest `_commit_version`

#### CDF Metadata Columns

When using CDF, the DataFrame includes these additional columns:
- **`_change_type`**: Type of change
  - `insert` - New record
  - `update_preimage` - Record before update
  - `update_postimage` - Record after update
  - `delete` - Deleted record
- **`_commit_version`**: Delta table version number (used for watermarking)
- **`_commit_timestamp`**: Transaction timestamp (available for logging/observability)

**Handling CDF Metadata Columns**:
- By default, the framework **keeps these columns** so you can filter by change type if needed
- To remove them, use `remove_columns` transformation:
  ```sql
  (60, 'data_transformation_steps', 'remove_columns', 1, 'column_name', '_change_type,_commit_version')
  ```

#### Advanced: Filtering by Change Type

Filter specific change types if you only want certain operations:
```sql
-- Only process inserts and updates (ignore deletes)
(60, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', '_change_type IN (''insert'', ''update_postimage'')'),

-- Only process deletes (for delete tracking)
(60, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', '_change_type = ''delete''')
```

#### Best Practices
1. **Enable CDF before first load**: Retroactively enabling CDF only tracks future changes
2. **Monitor CDF retention**: Delta retains CDF data based on `delta.deletedFileRetentionDuration` (default 7 days)
3. **Use for critical tables**: CDF adds storage overhead - use for tables requiring reliable change tracking
4. **Test delete handling**: Ensure downstream processes handle `_change_type = 'delete'` appropriately
5. **Combine with merge strategies**: Use `merge_type = 'merge_and_delete'` to physically delete records
6. **Monitor `_commit_timestamp` in logs**: Easier to troubleshoot than business column values

#### When NOT to Use CDF
- **External database sources**: CDF only works with Delta tables
- **File-based ingestion**: Use file modification timestamps instead
- **Simple append-only**: Traditional watermarks are simpler for append-only patterns
- **Storage-constrained**: CDF adds storage overhead for change tracking

> 📖 **Standalone item execution** (`execute_databricks_notebook`, `execute_databricks_job`, `execute_databricks_job`) is documented in [CORE_COMPONENTS_REFERENCE.md — Execute External workspace items](CORE_COMPONENTS_REFERENCE.md#execute-a-notebook-dataflow-gen2-or-pipeline-as-a-standalone-activity).

### Entity Resolution: Fuzzy Matching, Phonetic Matching, and Percent Difference

**Keywords:** entity resolution, fuzzy matching, phonetic, Soundex, Levenshtein, deduplication, duplicate detection, percent difference, record linkage, MinHash, LSH, entity matching, n-gram

#### Overview
Perform sophisticated entity matching across datasets using fuzzy matching (Levenshtein distance), phonetic matching (Soundex), exact matching, and numeric percent difference. The framework uses MinHash LSH (Locality-Sensitive Hashing) for scalability with large datasets, enabling efficient duplicate detection and cross-dataset entity resolution.

#### Key Capabilities
- **N-Gram Filtering**: Pre-filters potential matches using MinHash LSH for scalability with millions of records
- **Multiple Comparison Types**: 
  - **Fuzzy**: Levenshtein distance for typo-tolerant string matching
  - **Phonetic**: Soundex algorithm for sound-alike matching
  - **Exact**: Exact string matching
  - **Percent Difference**: Numeric comparison with configurable tolerance
- **Configurable Matching Logic**: Define auto-match and manual review thresholds with SQL-like expressions
- **Self-Join Support**: Compare records within the same dataset for deduplication
- **Cross-Dataset Matching**: Match entities across different source systems

#### Use Cases
- **Customer deduplication** across multiple source systems
- **Product matching** with slight name variations
- **Address standardization** with fuzzy matching
- **Vendor consolidation** matching company names phonetically
- **Data quality improvement** identifying duplicate records

#### Prerequisites
Understanding of:
- Levenshtein distance (higher score = more similar, 1.0 = exact match)
- Soundex phonetic encoding (matches words that sound similar)
- LSH for approximate nearest neighbor search

#### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Example Configuration
```sql
-- Orchestration Table
-- Primary keys are ALWAYS: {Primary_Dataset_Alias}_match_id, {Secondary_Dataset_Alias}_match_id
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('EntityMatching', 1, 47, 'gold', 'dbo.Customer_Match_Results', 'table1_match_id, table2_match_id', 'batch', 1);

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Primary dataset (source of records to match)
    (47, 'source_details', 'table_name', 'Silver.dbo.Customers_Dataset_A')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Secondary dataset (can be same table for deduplication or different table for cross-system matching)
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_query', 'SELECT * FROM Silver.dbo.Customers_Dataset_B'),
    
    -- Dataset aliases used in column naming and matching logic
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_alias', 'system_a'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_alias', 'system_b'),
    
    -- Fields to compare (positional matching: 1st primary field compared to 1st secondary field, etc.)
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_comparison_fields', 'first_name, last_name, company, state'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_comparison_fields', 'first_name, last_name, company, state'),
    
    -- Comparison types: fuzzy, soundex, exact, percent_difference
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'comparison_types', 'fuzzy, fuzzy, fuzzy, exact'),
    
    -- Optional: Only perform fuzzy matching if at least one exact match exists (reduces false positives)
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'only_fuzzy_match_if_one_exact_match', 'Yes'),
    
    -- Auto-match logic: Records matching this SQL expression are marked as "Auto_Match"
    -- Column naming: {primary_alias}_{field_name}_{secondary_alias}_{field_name}_{comparison_type}
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'auto_match_logic', 
     '(system_a_first_name_system_b_first_name_fuzzy > 0.9 AND system_a_last_name_system_b_last_name_fuzzy > 0.9 AND system_a_company_system_b_company_fuzzy > 0.5)'),
    
    -- Manual review logic: Records matching this are marked as "Match_For_Manual_Review"
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'match_with_manual_review_logic', 
     'system_a_first_name_system_b_first_name_fuzzy > 0.8 AND system_a_last_name_system_b_last_name_fuzzy > 0.8')
```

#### Output Schema

The entity resolution output includes:
- **Original columns from both datasets** (prefixed with dataset aliases)
- **Match score columns**: `{primary_alias}_{field}_{secondary_alias}_{field}_{comparison_type}`
  - Example: `system_a_first_name_system_b_first_name_fuzzy` (value: 0.0 to 1.0)
- **Match identifiers**:
  - `{primary_alias}_match_id`: Unique ID from primary dataset
  - `{secondary_alias}_match_id`: Unique ID from secondary dataset
- **match_outcome**: 
  - `Auto_Match`: Meets auto-match criteria
  - `Match_For_Manual_Review`: Meets manual review criteria
  - `No_Match`: Below all thresholds

#### Comparison Type Details

**Fuzzy (Levenshtein Distance):**
- Measures edit distance between strings (insertions, deletions, substitutions)
- Returns similarity score: 0.0 (completely different) to 1.0 (identical)
- Best for: Typos, spelling variations, OCR errors
- Example: "Smith" vs "Smyth" → 0.8

**Soundex (Phonetic):**
- Encodes words by pronunciation
- Returns 1.0 for same phonetic encoding, 0.0 otherwise
- Best for: Different spellings of same pronunciation
- Example: "Smith" vs "Smythe" → 1.0

**Exact:**
- Case-sensitive exact match
- Returns 1.0 for match, 0.0 for no match
- Best for: IDs, codes, standardized fields

**Percent Difference:**
- For numeric fields, calculates: `1 - (abs(val1 - val2) / max(val1, val2))`
- Returns 1.0 for identical values, lower for larger differences
- Best for: Revenue, quantities, measurements
- Example: 100 vs 105 → 0.95

#### Advanced Examples

**Self-Join for Deduplication:**
```sql
-- Find duplicates within same dataset
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Use same table for both primary and secondary
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_query', 
     'SELECT * FROM Silver.dbo.Customers_Dataset_A'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_alias', 'record1'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_alias', 'record2'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_comparison_fields', 'first_name, last_name, email'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_comparison_fields', 'first_name, last_name, email'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'comparison_types', 'fuzzy, fuzzy, exact'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'auto_match_logic', 
     '(record1_email_record2_email_exact = 1.0) OR (record1_first_name_record2_first_name_fuzzy > 0.95 AND record1_last_name_record2_last_name_fuzzy > 0.95)')
```

**Numeric Percent Difference:**
```sql
-- Match products with similar prices
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_comparison_fields', 'product_name, price, weight'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_comparison_fields', 'product_name, price, weight'),
    -- Use percent_difference for numeric fields
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'comparison_types', 'fuzzy, percent_difference, percent_difference'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'auto_match_logic', 
     'catalog_product_name_vendor_product_name_fuzzy > 0.9 AND catalog_price_vendor_price_percent_difference > 0.95')
```

**Phonetic Matching for Names:**
```sql
-- Match companies with different spellings but same pronunciation
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_comparison_fields', 'company_name, city'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_comparison_fields', 'company_name, city'),
    -- Soundex for company name, exact for city
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'comparison_types', 'soundex, exact'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'auto_match_logic', 
     'crm_company_name_erp_company_name_soundex = 1.0 AND crm_city_erp_city_exact = 1.0')
```

**Complex Multi-Criteria Matching:**
```sql
-- Sophisticated matching with multiple confidence levels
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'primary_dataset_comparison_fields', 
     'first_name, last_name, email, phone, address, city, state, zip'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'secondary_dataset_comparison_fields', 
     'first_name, last_name, email, phone, address, city, state, zip'),
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'comparison_types', 
     'fuzzy, fuzzy, exact, exact, fuzzy, exact, exact, exact'),
    
    -- High confidence: Exact email OR exact phone with name similarity
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'auto_match_logic', 
     '(a_email_b_email_exact = 1.0) OR (a_phone_b_phone_exact = 1.0 AND a_first_name_b_first_name_fuzzy > 0.85)'),
    
    -- Medium confidence: Strong name match with address similarity
    (47, 'data_transformation_steps', 'entity_resolution', 1, 'match_with_manual_review_logic', 
     '(a_first_name_b_first_name_fuzzy > 0.85 AND a_last_name_b_last_name_fuzzy > 0.85 AND a_address_b_address_fuzzy > 0.8 AND a_zip_b_zip_exact = 1.0)')
```

#### Performance Considerations

**LSH Optimization:**
- Framework automatically uses MinHash LSH for candidate generation
- Only compares records with similar n-grams, dramatically reducing comparisons
- For 1M x 1M comparison, LSH reduces to ~100K actual comparisons

**Best Practices:**
1. **Use exact matches first**: Leverage `only_fuzzy_match_if_one_exact_match = 'Yes'` to reduce false positives
2. **Limit comparison fields**: More fields = slower processing; focus on most discriminative
3. **Set realistic thresholds**: Start conservative (0.9+) and adjust based on results
4. **Filter datasets**: Use WHERE clauses in queries to reduce dataset sizes
5. **Review manual matches**: Regularly analyze manual review cases to refine thresholds
6. **Monitor performance**: Track execution time and adjust field count if needed

#### Reviewing Results

```sql
-- Auto-matched records
SELECT * 
FROM gold.dbo.Customer_Match_Results 
WHERE match_outcome = 'Auto_Match'

-- Records needing manual review
SELECT * 
FROM gold.dbo.Customer_Match_Results 
WHERE match_outcome = 'Match_For_Manual_Review'
ORDER BY system_a_first_name_system_b_first_name_fuzzy DESC

-- Analyze match score distribution
SELECT 
    match_outcome,
    AVG(system_a_first_name_system_b_first_name_fuzzy) as avg_first_name_score,
    AVG(system_a_last_name_system_b_last_name_fuzzy) as avg_last_name_score,
    COUNT(*) as record_count
FROM gold.dbo.Customer_Match_Results
GROUP BY match_outcome
```

### Execute Pre-Built Transformations

**Keywords:** built-in transformations, derived_column, filter_data, join_data, aggregate_data, pivot_data, unpivot_data, conditional_column, window function, string_functions, columns_to_rename, drop_columns, Tier 1, declarative, metadata-driven transformation

#### Overview
The framework provides a comprehensive library of pre-built transformations that can be applied declaratively through metadata configuration. These transformations execute in a defined order and can be chained together to create complex data processing logic without writing custom code.

#### Available Transformations

The full attribute-by-attribute contract now lives in [METADATA_REFERENCE.md](METADATA_REFERENCE.md), and authoring patterns live in [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md). This guide keeps the runtime view: what each built-in step is for, the few rules that commonly cause runtime issues, and a minimal working example.

The built-in library covers:
1. Column shaping: `columns_to_rename`, `select_columns`, `remove_columns`
2. Type and value cleanup: `change_data_types`, `apply_null_handling`, `replace_values`, `mask_sensitive_data`
3. Row logic: `derived_column`, `conditional_column`, `drop_duplicates`, `filter_data`
4. Reshaping and enrichment: `pivot_data`, `unpivot_data`, `join_data`, `union_data`
5. Analytics: `add_window_function`, `aggregate_data`, `transform_datetime`, `add_row_hash`
6. Text and nested-data helpers: `sort_data`, `explode_array`, `flatten_struct`, `split_column`, `concat_columns`, `string_functions`, `normalize_text`

#### Execution Order

Execution order is controlled by `Configuration_Name_Instance_Number` within `data_transformation_steps`. Use lower numbers for schema-shaping and cleanup, then joins/aggregations, then final formatting. In practice:
1. Rename/select/remove columns first.
2. Cast and derive next.
3. Apply null handling, replacements, deduplication, and filters before expensive joins.
4. Reserve pivots, unions, joins, window functions, and aggregations for later steps.
5. Add hashes or reporting-only columns near the end.

#### Configuration Steps
1. Add or update the metadata SQL.
2. Execute it in the Metadata Warehouse.
3. Run `Trigger Step Orchestrator` for the trigger.

#### Basic Example Configuration
```sql
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('Transformations', 1, 48, 'gold', 'dbo.Product_Analysis', 'product_id', 'batch', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (48, 'source_details', 'table_name', 'Silver.dbo.Products');

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (48, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'PROD_UNIT_OF_MEASURE'),
    (48, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'unit_of_measure'),
    (48, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'product_category_upper'),
    (48, 'data_transformation_steps', 'derived_column', 2, 'expression', 'UPPER(product_category)'),
    (48, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'product_subcategory = ''Baseballs'''),
    (48, 'data_transformation_steps', 'remove_columns', 4, 'column_name', 'internal_notes, temp_field');
```

### Transformation Reference Guide

**Keywords:** transformation rules, derived_column expressions, conditional_column, join alias, pivot output, drop_duplicates, execution order, Spark SQL, CASE expression, cast, type conversion

#### Core Rules

- `derived_column` uses Spark SQL expressions only. Use it for `CASE`, casts, time-zone conversions, arithmetic, and functions.
- `conditional_column` writes literal branch values only. If a branch must return another column or expression, use `derived_column` instead.
- `join_data` always uses `a` for the current dataset and `b` for the joined dataset.
- `pivot_data` output column names follow Spark defaults such as `Q1_sum(revenue)`.
- `drop_duplicates` changes behavior when `order_by` is present: without it the step is exact deduplication, with it the step keeps one row per key based on the sort.

#### Operator Summary

| Transformation | Use When | Runtime Notes |
|---|---|---|
| `columns_to_rename` | standardizing source names | old and new names can be comma-separated and positionally matched |
| `select_columns` | keep only an approved projection | use before joins to reduce shuffle cost |
| `remove_columns` | drop temporary or sensitive columns | useful late in the flow after derived logic |
| `change_data_types` | convert data types declaratively | use separate instances when target types differ |
| `derived_column` | add expression-based columns | best choice for `CASE`, `to_utc_timestamp`, `from_utc_timestamp`, arithmetic |
| `apply_null_handling` | replace, drop, or nullify values | supports repeated handling across comma-separated columns |
| `replace_values` | normalize known raw values | can replace multiple source values with one target value |
| `mask_sensitive_data` | redact PII-like content | masks uppercase, lowercase, digits, and optionally special chars |
| `drop_duplicates` | remove exact or ordered duplicates | add `order_by` and optional `order_direction` for survivorship logic |
| `filter_data` | enforce row-level eligibility | `filter_logic` uses Spark SQL `WHERE` syntax |
| `pivot_data` | rotate row values into columns | requires `aggregation`; aliases come from Spark |
| `unpivot_data` | normalize repeated column sets into rows | define identifier columns plus value columns |

#### Minimal Patterns

```sql
-- Cast and derive
(48, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'order_date'),
(48, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'date'),
(48, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'order_created_utc'),
(48, 'data_transformation_steps', 'derived_column', 2, 'expression', 'to_utc_timestamp(order_created_ntz, ''America/New_York'')')

-- Null handling and standardization
(48, 'data_transformation_steps', 'apply_null_handling', 3, 'column_name', 'discount_amount, tax_amount'),
(48, 'data_transformation_steps', 'apply_null_handling', 3, 'action', 'replace_with_default'),
(48, 'data_transformation_steps', 'apply_null_handling', 3, 'default_value', '0'),
(48, 'data_transformation_steps', 'replace_values', 4, 'column_name', 'country'),
(48, 'data_transformation_steps', 'replace_values', 4, 'values_to_replace', 'USA,U.S.A.,US'),
(48, 'data_transformation_steps', 'replace_values', 4, 'replacement_value', 'United States')

-- Survivorship and filtering
(48, 'data_transformation_steps', 'drop_duplicates', 5, 'column_name', 'customer_id'),
(48, 'data_transformation_steps', 'drop_duplicates', 5, 'order_by', 'modified_date'),
(48, 'data_transformation_steps', 'filter_data', 6, 'filter_logic', 'status = ''Active'' AND order_date >= ''2024-01-01''')

-- Reshaping or enrichment
(48, 'data_transformation_steps', 'pivot_data', 7, 'group_columns', 'product_id'),
(48, 'data_transformation_steps', 'pivot_data', 7, 'pivot_column', 'quarter'),
(48, 'data_transformation_steps', 'pivot_data', 7, 'value_column', 'revenue'),
(48, 'data_transformation_steps', 'pivot_data', 7, 'aggregation', 'sum'),
(48, 'data_transformation_steps', 'join_data', 8, 'join_type', 'left'),
(48, 'data_transformation_steps', 'join_data', 8, 'right_table_name', 'Gold.dbo.Dim_Customer'),
(48, 'data_transformation_steps', 'join_data', 8, 'join_condition', 'a.customer_id = b.customer_id AND b.scd_active = 1')
```

---

#### Additional Operators

| Transformation | Use When | Runtime Notes |
|---|---|---|
| `union_data` | stack compatible datasets | prefer `by_name` unless column order is controlled; `deduplicate` is optional |
| `add_window_function` | ranking or running totals | use lowercase function names; `output_column_name` is required |
| `aggregate_data` | summarize to a coarser grain | keep `column_name`, `aggregation`, and `output_column_name` positionally aligned |
| `transform_datetime` | extract date parts or shift dates | `output_column_name` is required; use `derived_column` for time-zone semantics |
| `add_row_hash` | detect change across a row or business key | `column_name='*'` hashes all columns |
| `sort_data` | enforce deterministic order | useful before sequence-sensitive downstream logic |
| `explode_array` | turn array values into rows | `preserve_nulls='true'` keeps sparse source rows |
| `flatten_struct` | project nested struct fields into columns | default prefix is `{column_name}_` |
| `split_column` | split delimited text into fields | `output_column_names` is required |
| `concat_columns` | build display or business strings | choose `null_handling` intentionally |
| `conditional_column` | literal-valued branching only | not for column references or non-literal expressions |
| `string_functions` | simple one-step text transforms | output column is always explicit |
| `normalize_text` | multi-step text standardization | useful before matching, deduping, or fuzzy comparison |

#### Practical Patterns

```sql
-- Union and analytics
(48, 'data_transformation_steps', 'union_data', 9, 'union_type', 'by_name'),
(48, 'data_transformation_steps', 'union_data', 9, 'union_tables', 'Silver.dbo.Orders_Archive'),
(48, 'data_transformation_steps', 'add_window_function', 10, 'column_name', 'amount'),
(48, 'data_transformation_steps', 'add_window_function', 10, 'output_column_name', 'running_total'),
(48, 'data_transformation_steps', 'add_window_function', 10, 'window_function', 'sum'),
(48, 'data_transformation_steps', 'add_window_function', 10, 'partition_by', 'customer_id'),
(48, 'data_transformation_steps', 'add_window_function', 10, 'order_by', 'order_date')

-- Aggregation and time derivation
(48, 'data_transformation_steps', 'aggregate_data', 11, 'group_by_columns', 'customer_id, region'),
(48, 'data_transformation_steps', 'aggregate_data', 11, 'column_name', 'amount, amount'),
(48, 'data_transformation_steps', 'aggregate_data', 11, 'aggregation', 'sum, avg'),
(48, 'data_transformation_steps', 'aggregate_data', 11, 'output_column_name', 'total_amount, avg_amount'),
(48, 'data_transformation_steps', 'transform_datetime', 12, 'column_name', 'order_date'),
(48, 'data_transformation_steps', 'transform_datetime', 12, 'operation', 'year'),
(48, 'data_transformation_steps', 'transform_datetime', 12, 'output_column_name', 'order_year')

-- Nested-data and text helpers
(48, 'data_transformation_steps', 'explode_array', 13, 'column_name', 'tags'),
(48, 'data_transformation_steps', 'explode_array', 13, 'output_column_name', 'tag'),
(48, 'data_transformation_steps', 'flatten_struct', 14, 'column_name', 'customer_info'),
(48, 'data_transformation_steps', 'split_column', 15, 'column_name', 'full_name'),
(48, 'data_transformation_steps', 'split_column', 15, 'delimiter', ','),
(48, 'data_transformation_steps', 'split_column', 15, 'output_column_names', 'first_name, last_name, suffix'),
(48, 'data_transformation_steps', 'normalize_text', 16, 'column_name', 'customer_name'),
(48, 'data_transformation_steps', 'normalize_text', 16, 'operations', 'trim,lowercase,collapse_whitespace,remove_punctuation')
```

#### Rules That Commonly Matter

- `union_data`: `by_position` is faster, but `by_name` is safer for evolving schemas.
- `add_window_function`: `row_number`, `rank`, `dense_rank`, `sum`, `avg`, `min`, `max`, `count`, `first_value`, and `last_value` are the common supported patterns documented here.
- `transform_datetime`: use it for extraction and simple arithmetic; keep time-zone interpretation in `derived_column` so the intent is explicit.
- `conditional_column`: values are literals. For `CASE WHEN ... ELSE existing_column END`, switch back to `derived_column`.
- `string_functions` and `normalize_text`: keep them near the end unless downstream joins or matching depend on the normalized output.

For the full parameter list and accepted values for each transformation, use [METADATA_REFERENCE.md](METADATA_REFERENCE.md). For deciding whether to stay in metadata or move to custom code, use [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md).

---

### Complete Transformation Example

**Keywords:** full example, end-to-end transformation, comprehensive example, sample metadata, transformation pipeline, chained transformations

```sql
-- Comprehensive transformation pipeline
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- 1. Rename columns for standardization
    (48, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'CUST_ID'),
    (48, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id'),
    
    -- 2. Select only needed columns
    (48, 'data_transformation_steps', 'select_columns', 2, 'column_name',
     'customer_id, order_date, product_id, quantity, unit_price'),
    
    -- 3. Change data types
    (48, 'data_transformation_steps', 'change_data_types', 3, 'column_name', 'order_date'),
    (48, 'data_transformation_steps', 'change_data_types', 3, 'new_type', 'date'),
    
    -- 4. Add calculated columns
    (48, 'data_transformation_steps', 'derived_column', 4, 'column_name', 'total_amount'),
    (48, 'data_transformation_steps', 'derived_column', 4, 'expression', 'quantity * unit_price'),
    
    -- 5. Handle nulls
    (48, 'data_transformation_steps', 'apply_null_handling', 5, 'column_name', 'quantity'),
    (48, 'data_transformation_steps', 'apply_null_handling', 5, 'action', 'replace_with_default'),
    (48, 'data_transformation_steps', 'apply_null_handling', 5, 'default_value', '1'),
    
    -- 6. Drop duplicates per order_id based on order_date
    (48, 'data_transformation_steps', 'drop_duplicates', 6, 'column_name', 'order_id'),
    (48, 'data_transformation_steps', 'drop_duplicates', 6, 'order_by', 'order_date'),
    
    -- 7. Filter active orders only
    (48, 'data_transformation_steps', 'filter_data', 7, 'filter_logic',
     'order_date >= ''2024-01-01'' AND total_amount > 0'),
    
    -- 8. Join with customer dimension
    (48, 'data_transformation_steps', 'join_data', 8, 'join_type', 'left'),
    (48, 'data_transformation_steps', 'join_data', 8, 'right_table_name', 'Gold.dbo.Dim_Customer'),
    (48, 'data_transformation_steps', 'join_data', 8, 'join_condition', 'a.customer_id = b.customer_id'),
    (48, 'data_transformation_steps', 'join_data', 8, 'right_columns',
     'customer_name, customer_tier, region'),
    
    -- 9. Add window function for running total
    (48, 'data_transformation_steps', 'add_window_function', 9, 'column_name', 'total_amount'),
    (48, 'data_transformation_steps', 'add_window_function', 9, 'output_column_name', 'running_total'),
    (48, 'data_transformation_steps', 'add_window_function', 9, 'window_function', 'sum'),
    (48, 'data_transformation_steps', 'add_window_function', 9, 'partition_by', 'customer_id'),
    (48, 'data_transformation_steps', 'add_window_function', 9, 'order_by', 'order_date'),
    
    -- 10. Extract date parts
    (48, 'data_transformation_steps', 'transform_datetime', 10, 'column_name', 'order_date'),
    (48, 'data_transformation_steps', 'transform_datetime', 10, 'operation', 'year'),
    (48, 'data_transformation_steps', 'transform_datetime', 10, 'output_column_name', 'order_year'),
    
    -- 11. Add row hash for change detection
    (48, 'data_transformation_steps', 'add_row_hash', 11, 'column_name', '*'),
    (48, 'data_transformation_steps', 'add_row_hash', 11, 'output_column_name', 'row_hash')
```

#### Best Practices
1. **Order matters**: Use `Configuration_Name_Instance_Number` to control execution sequence
2. **Test incrementally**: Add transformations one at a time and validate results
3. **Document logic**: Add comments in SQL scripts explaining business rules
4. **Monitor performance**: Some transformations (joins, window functions) can be memory-intensive
5. **Use appropriate types**: Choose the right transformation instead of complex derived_column expressions
6. **Validate schemas**: Ensure column names and types match before joins and unions

