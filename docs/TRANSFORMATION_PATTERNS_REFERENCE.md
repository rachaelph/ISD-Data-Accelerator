# Transformation Patterns Reference

> Last updated: 2026-03-29

> **Canonical lookup catalog for all 34 built-in transformation types.** Each entry shows the exact `INSERT` syntax for `Data_Pipeline_Metadata_Advanced_Configuration` with `Configuration_Category = 'data_transformation_steps'`.
>
> **Related docs:**
> - [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) — core generation rules, golden rules, legacy code conversion
> - [METADATA_REFERENCE.md](METADATA_REFERENCE.md) — canonical attribute spec (allowed values, required attributes)
> - [DATA_TRANSFORMATION_GUIDE.md](DATA_TRANSFORMATION_GUIDE.md) — transformation scenarios and best practices
> - [Complex_Transformation_Patterns.md](Complex_Transformation_Patterns.md) — advanced multi-step patterns

Use this file when you need the exact SQL INSERT syntax for a specific built-in transformation type.

## Transformation SQL Quick Reference

**Keywords:** derived_column, filter_data, join_data, aggregate_data, pivot_data, unpivot_data, conditional_column, window function, string_functions, columns_to_rename, drop_columns, cast_columns, drop_duplicates, scd2, surrogate key, union_data, add_row_number, explode_column, replace_values, fill_null, split_column, flatten_json, data_transformation_steps, INSERT syntax, SQL example, advanced configuration


#### B. `data_transformation_steps`

**CRITICAL: Execution Order**: The `Configuration_Name_Instance_Number` dictates the **sequential execution order** for all transformation steps within a Table_ID. Steps execute in ascending order (1, then 2, then 3, etc.). You can have multiple instances of the same transformation type by using different instance numbers. For example:
- Instance 1: `filter_data` to remove nulls
- Instance 2: `join_data` to enrich with customer data
- Instance 3: `filter_data` to remove invalid products

Each instance number must be **unique** for a given `(Table_ID, Configuration_Category, Configuration_Name)` combination.

**Common Transformations:**

##### 1. columns_to_rename - Rename columns
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_id,prod_id'),
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id,product_id');
```

##### 2. derived_column - Create calculated columns
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'derived_column', 1, 'column_name', 'total_amount'),
    (103, 'data_transformation_steps', 'derived_column', 1, 'expression', 'quantity * unit_price');
```

Use `derived_column` for time zone-aware timestamp normalization when the built-in transformations are not sufficient. The expression is executed as Spark SQL via `expr(...)`, so use Spark SQL functions such as `to_timestamp`, `to_utc_timestamp`, and `from_utc_timestamp` rather than arbitrary Python/PySpark code.

Best practice for `TimestampNTZ` inputs:
- If the source value is local wall-clock time in a known zone, create a new UTC column with `to_utc_timestamp(...)`.
- If the source value is already UTC and you need a reporting-zone column, use `from_utc_timestamp(...)`.
- If the source time zone is unknown, do not invent one in metadata. Keep the original column and escalate the contract question.

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'created_utc'),
    (103, 'data_transformation_steps', 'derived_column', 2, 'expression', 'to_utc_timestamp(created_ntz, ''America/Chicago'')');
```

##### 3. filter_data - Filter rows

> ⚠️ **COMMON LLM MISTAKE**: Do NOT use `filter_data` for data quality validation!
>
> | Use Case | Correct Pattern |
> |----------|----------------|
> | Business scope (e.g., "only orders from 2024") | `filter_data` |
> | Data quality (e.g., "reject NULL keys", "validate positive amounts") | `validate_condition` (data_quality category) |
>
> **Why it matters**: `filter_data` silently drops rows with no audit trail. `validate_condition` quarantines bad records for investigation and creates audit trails.
>
> **See also**: [Error Prevention #9](#9-using-filter_data-transformation-for-data-quality-validation) for detailed examples.

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'order_date >= ''2024-01-01'' AND status = ''completed''');
```

##### 4. drop_duplicates - Remove duplicates

**Mode is determined by presence of `order_by`:**
- **If `order_by` is NOT specified**: Exact duplicate removal on `column_name` columns
- **If `order_by` IS specified**: Ordered deduplication - keeps one row per `column_name` combination based on `order_by`

**Attributes:**
- `column_name`: **Required.** Columns to deduplicate on. Use `*` for all columns, or comma-separated column names.
- `order_by`: Optional. If provided, keeps one row per `column_name` combination based on these sort columns.
- `order_direction`: Optional. Sort direction (`asc`/`desc`), default `desc`.

```sql
-- Exact match deduplication across all columns (no order_by = exact mode)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', '*');
    (103, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', '*');

-- Exact match deduplication on specific columns
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'customer_id,product_id');
    (103, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'customer_id,product_id');

-- Ordered deduplication: keep most recent per customer_id (desc is default)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'drop_duplicates', 2, 'column_name', 'customer_id'),
    (103, 'data_transformation_steps', 'drop_duplicates', 2, 'order_by', 'modified_date');
    (103, 'data_transformation_steps', 'drop_duplicates', 2, 'column_name', 'customer_id'),
    (103, 'data_transformation_steps', 'drop_duplicates', 2, 'order_by', 'modified_date');
```

##### 5. change_data_types - Convert data types
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'customer_id,order_id'),
    (103, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'string');
    (103, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'customer_id,order_id'),
    (103, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'string');
```

##### 6. join_data - Join with another table

**Join Condition Aliases:**
- `a.` = Current dataset (left table)
- `b.` = Table being joined (right table)

**Note:** The `join_condition` can only reference columns from these two tables (`a.` and `b.`). You cannot use subqueries, `EXISTS`, `IN` with other tables, or reference columns from tables other than `a.` and `b.`.

**Attributes:**
- `join_type`: Required. `'left'`, `'right'`, `'inner'`, `'full'`, or `'cross'`.
- `right_table_name`: Required. Fully qualified table name to join with.
- `join_condition`: Required (except for cross joins). Join condition using `a.` and `b.` aliases.
- `left_columns`: Optional. Comma-separated columns from left table. Default: `'*'` (all columns).
- `right_columns`: Optional. Comma-separated columns from right table. Default: `'*'` (all columns).
- `broadcast_hint`: Optional. Set to `'true'` to apply a Spark broadcast hint for small dimension joins, improving performance. Default: `'false'`.

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'join_data', 1, 'join_type', 'left'),
    (103, 'data_transformation_steps', 'join_data', 1, 'right_table_name', 'silver.dbo.customers'),
    (103, 'data_transformation_steps', 'join_data', 1, 'join_condition', 'a.customer_id = b.customer_id'),
    (103, 'data_transformation_steps', 'join_data', 1, 'left_columns', '*'),
    (103, 'data_transformation_steps', 'join_data', 1, 'right_columns', 'customer_name,customer_tier');
```

##### 7. aggregate_data - Aggregations

**Attributes:**
- `group_by_columns`: Required. Comma-separated columns to group by.
- `column_name`: Required. Comma-separated columns to aggregate (must match count of `aggregation` and `output_column_name`).
- `aggregation`: Required. Comma-separated aggregation functions. Valid values: `sum`, `avg`, `count`, `min`, `max`, `first`, `last`, `count_distinct`.
- `output_column_name`: Required. Comma-separated output column names (must match count of `column_name`).

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'aggregate_data', 1, 'group_by_columns', 'customer_id,product_id'),
    (103, 'data_transformation_steps', 'aggregate_data', 1, 'column_name', 'amount,quantity'),
    (103, 'data_transformation_steps', 'aggregate_data', 1, 'aggregation', 'sum,avg'),
    (103, 'data_transformation_steps', 'aggregate_data', 1, 'output_column_name', 'total_amount,avg_quantity');
```

##### 8. create_surrogate_key - Generate dimension surrogate keys

**Required Attributes:**
- `type`: Surrogate key generation method (currently only `'auto_increment'` supported)
- `column_name`: Name of the surrogate key column to create

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'create_surrogate_key', 1, 'type', 'auto_increment'),
    (103, 'data_transformation_steps', 'create_surrogate_key', 1, 'column_name', 'customer_key');
```

##### 9. attach_dimension_surrogate_key - Lookup and attach surrogate keys from dimension tables

**Join Logic Aliases:**
- `a.` = Current dataset (source data)
- `b.` = Dimension table being joined

**Note:** The `dimension_table_join_logic` can only reference columns from these two tables (`a.` and `b.`). You cannot use subqueries, `EXISTS`, `IN` with other tables, or reference columns from tables other than `a.` and `b.`.

**Required Attributes:**
- `dimension_table_name`: Name of the dimension table
- `dimension_table_join_logic`: Join condition between source and dimension (use `a.` for current table, `b.` for dimension)
- `dimension_table_key_column_name`: Surrogate key column name in dimension table
- `dimension_key_output_column_name`: Name of the surrogate key column to create in the target (fact) table

**Optional Attributes:**
- `dimension_columns_to_add_to_fact`: Additional dimension columns to denormalize into target table

> **Star and snowflake friendly**: Point `dimension_table_name` to any conformed dimension (primary, degenerate, bridge, or helper dimension) to support either classic star schemas or deeper snowflake hierarchies. Chain multiple `attach_dimension_surrogate_key` steps (each with its own `Configuration_Name_Instance_Number`) to walk a snowflake, or use a single step per spoke in a star.

**Understanding Star vs Snowflake Schema:**
- **Star Schema**: Fact table has direct FKs to all dimensions (fact → dim_customer, fact → dim_product, fact → dim_date). Use one `attach_dimension_surrogate_key` step per dimension.
- **Snowflake Schema**: Dimensions are normalized with parent-child relationships (fact → dim_season → dim_tv_show). The fact table only references its immediate dimension, and that dimension references its parent. Configure the parent FK in the dimension's Table_ID, not the fact's.

**Snowflake Example**: TV Show Data Model
```
fact_episode (has season_id)
     ↓
dim_season (has season_key surrogate, plus tv_show_id natural key, plus tv_show_key FK)
     ↓
dim_tv_show (has tv_show_key surrogate)
```

In this model:
- `fact_episode` gets `season_key` via `attach_dimension_surrogate_key` joining to `dim_season`
- `dim_season` gets `tv_show_key` via `attach_dimension_surrogate_key` joining to `dim_tv_show` (configured in `dim_season`'s Table_ID, not the fact's)

**Fact Table Configuration** (joins only to immediate dimension):
```sql
-- ✅ Fact table gets season_key - that's the only surrogate key it needs
-- The snowflake relationship (season → tv_show) is handled in dim_season, not the fact
(304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_name', 'gold.dbo.dim_season'),
(304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.season_id = b.id'),
(304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_key_column_name', 'season_key'),
(304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_key_output_column_name', 'season_sk')
```

**Dimension Table Configuration** (`dim_season` gets the FK to its parent `dim_tv_show`):
```sql
-- ✅ dim_season gets tv_show_key surrogate key pointing to its parent dimension
-- This is configured in the Table_ID that builds dim_season, NOT the fact table
(305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_name', 'gold.dbo.dim_tv_show'),
(305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.tv_show_id = b.id'),
(305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_key_column_name', 'tv_show_key'),
(305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_key_output_column_name', 'tv_show_sk')
```

##### 10. custom_transformation_function - Execute custom Python logic

> **⚠️ When using `custom_transformation_function`, you MUST create the actual notebook file, not just the metadata!**

**Required Attributes:**
- `functions_to_execute` — Comma-separated list of function names to call
- `files_to_run` — Comma-separated list of notebook names containing the functions (must match notebooks you create in `src/`)

**Custom Attributes (Optional):**
You can add any additional attributes with the same `Configuration_Name_Instance_Number`. These are passed to your function via the `metadata` dict and can be accessed in your custom function.

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Required attributes
    (103, 'data_transformation_steps', 'custom_transformation_function', 1, 'functions_to_execute', 'calculate_metrics,apply_business_rules'),
    (103, 'data_transformation_steps', 'custom_transformation_function', 1, 'files_to_run', 'custom_Calculations,NB_Business_Rules'),
    -- Optional custom attributes (accessed via metadata dict in your function)
    (103, 'data_transformation_steps', 'custom_transformation_function', 1, 'threshold_value', '100'),
    (103, 'data_transformation_steps', 'custom_transformation_function', 1, 'output_column_name', 'calculated_metric');
```

**Function Signature:** `def your_function(new_data: DataFrame, metadata: dict, spark) -> DataFrame`

<details>
<summary><b>📁 Click to expand: Notebook Template & Folder Structure</b></summary>

**Deliverables when using custom_transformation_function:**
1. ✅ The metadata SQL file referencing the custom function
2. ✅ the actual Python module file (`.sql` folder) containing the Python function(s)

**Create the notebook at:** `src/custom_<your_module>.sqlnotebook-content.py`

```python
# Cell 1: Import Warning
# ⚠️ Do NOT put imports at the top level of custom notebooks.
# Custom notebooks execute in a shared namespace — top-level imports
# pollute globals() and can shadow critical symbols.
# Place all imports INSIDE your function body instead.

# Cell 2: Main Custom Function (REQUIRED)
# Must have EXACTLY these three parameters and return a Spark DataFrame
def your_function_name(new_data: DataFrame, metadata: dict, spark) -> DataFrame:
    """Your function description."""
    # Imports — must be INSIDE the function (not at top level)
    from pyspark.sql import functions as f
    from pyspark.sql.types import StructType, StructField, StringType  # only what you need

    # Available metadata keys for custom_transformation_function:
    orchestration_metadata = metadata.get('orchestration_metadata', {})
    primary_config = metadata.get('primary_config', {})
    datastore_config = metadata.get('datastore_config', [])  # List of dicts from Datastore_Configuration table
    event_payload = metadata.get('event_payload', '')
    
    # Use _get_datastore_config() to look up datastore properties (available from helper_functions_3.py)
    # Signature: _get_datastore_config(datastore_config, datastore_name, property_name)
    # Available datastore names: 'bronze', 'silver', 'gold', 'metadata', plus any entry in external_datastores
    # Available properties: 'Datastore_Name', 'Datastore_Kind', 'Medallion_Layer', 'Workspace_ID', 'Workspace_URL', 'SQL_Warehouse_ID', 'Catalog_Name', 'Connection_Details'
    # NOTE: 'Catalog_Name' / 'SQL_Warehouse_ID' / 'Workspace_URL' / 'Workspace_ID' are populated for Datastore_Kind='databricks'. 'Connection_Details' is a JSON string for external datastores (e.g., sql_server, snowflake, rest_api).
    # Example: silver_catalog = _get_datastore_config(datastore_config, 'silver', 'Catalog_Name')
    
    # ⭐ RECOMMENDED: Access custom attributes from function_config
    # function_config contains your transformation step attributes as simple key-value pairs
    function_config = metadata.get('function_config', {})
    my_custom_param = function_config.get('my_custom_attribute')
    
    # ⚠️ AVOID: advanced_config is a list of dicts and difficult to parse
    # Only use if you need to access OTHER transformation steps' config
    # advanced_config = metadata.get('advanced_config', [])  # Not recommended for your own step's config
    
    # Your transformation logic here
    new_data = new_data.withColumn("new_column", f.lit("value"))
    
    return new_data  # MUST return a Spark DataFrame
```

**Folder structure:**
```
src/
└── custom_your_module_name.sql
    ├── # module file
    └── notebook-content.py # Your Python code
```

**`.platform` file template:**
```json
{
  "$schema": "removed - not needed for .py files.json",
  "metadata": {
    "type": "Notebook",
    "displayName": "custom_your_module_name",
    "description": "Custom transformation functions for [describe purpose]"
  },
  "config": {
    "version": "2.0",
    "logicalId": "00000000-0000-0000-0000-000000000000"
  }
}
```

See also: [Custom_Transformation_Function.py](resources/Custom_Transformation_Function.py) for a reference example.

</details>

> **⚠️ Non-Standard Library Warning**: If your custom function imports libraries NOT included in the standard Databricks Runtime (e.g., `requests`, `beautifulsoup4`, custom internal packages), you MUST:
> 1. **Create a custom Databricks compute cluster** with those packages
> 2. **Attach the Environment** to `batch_processing.py` notebook
> 3. See: [Manage libraries on Databricks compute](https://learn.microsoft.com/azure/databricks/libraries/)
> 
> The validator will detect non-standard imports and warn you about this requirement.

> **⚠️ Config Attribute Validation**: If your function accesses `function_config.get('some_attribute')`, the validator verifies that `some_attribute` is actually defined in the `custom_transformation_function` instance metadata. For example:
> ```python
> threshold = function_config.get('threshold_value')  # Validator checks metadata has 'threshold_value' attribute
> ```
> Missing attributes generate warnings to help prevent runtime errors.

##### 11. derived_column - Create a Derived Column
**Use this for custom calculations ONLY if a specific built-in pattern does not exist.** For creating row hashes, use the `add_row_hash` pattern instead.

**Formatting Rule**: Define the new column's name and the expression used to compute its value.

```sql
-- Create a 'full_name' column by concatenating first and last names
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'full_name'),
    (101, 'data_transformation_steps', 'derived_column', 2, 'expression', 'concat(first_name, " ", last_name)');
```

##### 12. add_row_hash - Add Row Hash
Use this to create a hash value for a row to track changes or create a unique identifier. This is the PREFERRED method for row hashing.

**Formatting Rule**: Specify the columns to include in the hash. Use `*` to include all columns. You can optionally provide a name for the new hash column and choose the hash algorithm.

**Hash Algorithm Options:**
- **`xxhash64` (default)**: Fast non-cryptographic hash (10-20 GB/s) for standard change detection. Use for most scenarios with <100M records.
- **`sha256`**: Collision-free cryptographic hash (500 MB/s - 2 GB/s) for critical applications, compliance requirements, or large datasets (>100M records). 5-10x slower but virtually eliminates collision risk.

```sql
-- Example 1: Default xxhash64 for fast change detection
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'add_row_hash', 3, 'column_name', 'product_id,price,description'),
    (101, 'data_transformation_steps', 'add_row_hash', 3, 'output_column_name', 'change_hash');

-- Example 2: SHA-256 for collision-free hashing (critical data)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (102, 'data_transformation_steps', 'add_row_hash', 1, 'column_name', '*'),
    (102, 'data_transformation_steps', 'add_row_hash', 1, 'hash_algorithm', 'sha256'),
    (102, 'data_transformation_steps', 'add_row_hash', 1, 'output_column_name', 'row_hash_sha256');
```

##### 13. remove_columns - Remove Columns
Use this to remove one or more columns from the dataset.

**Formatting Rule**: Provide a comma-separated list of column names to remove.

```sql
-- Remove temporary or unnecessary columns
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'remove_columns', 4, 'column_name', 'temp_col1,temp_col2');
```

##### 14. select_columns - Select Columns
Use this to explicitly select a subset of columns to keep in the dataset. All other columns will be dropped.

**Formatting Rule**: Provide a comma-separated list of column names to keep. By default, internal metadata columns (like `delta__...`) are retained; set `retain_metadata_columns` to `false` to drop them.

```sql
-- Select only the key business columns for the final output
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'select_columns', 5, 'column_name', 'customer_id,product_id,order_date,amount'),
    (101, 'data_transformation_steps', 'select_columns', 5, 'retain_metadata_columns', 'true');
```

##### 15. apply_null_handling - Handle Null Values
Use this to apply various null handling strategies to one or more columns.

**Formatting Rule**: Specify the column(s), the action to perform, and any additional parameters required by that action.
- `action`: `drop_rows`, `replace_with_default`, `convert_values_to_null`
- `logic`: (for `drop_rows`) `any` or `all`
- `default_value`: (for `replace_with_default`) The value to use as a replacement.
- `values_to_convert`: (for `convert_values_to_null`) A comma-separated list of string literals to be converted to null.

```sql
-- Drop rows where 'email' or 'phone' is null
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'apply_null_handling', 6, 'column_name', 'email,phone'),
    (101, 'data_transformation_steps', 'apply_null_handling', 6, 'action', 'drop_rows'),
    (101, 'data_transformation_steps', 'apply_null_handling', 6, 'logic', 'any');
```

```sql
-- Replace null 'status' values with 'Unknown'
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'apply_null_handling', 7, 'column_name', 'status'),
    (101, 'data_transformation_steps', 'apply_null_handling', 7, 'action', 'replace_with_default'),
    (101, 'data_transformation_steps', 'apply_null_handling', 7, 'default_value', 'Unknown');
```

```sql
-- Convert specific string values like 'N/A' and 'Not Applicable' to null in the 'notes' column
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'apply_null_handling', 8, 'column_name', 'notes'),
    (101, 'data_transformation_steps', 'apply_null_handling', 8, 'action', 'convert_values_to_null'),
    (101, 'data_transformation_steps', 'apply_null_handling', 8, 'values_to_convert', 'N/A,Not Applicable');
```

##### 16. replace_values - Replace Values
Use this to perform find-and-replace operations on a column.

**Formatting Rule**: Specify the column, the values to find, and the single replacement value.
- `column_name`: Comma-separated list of column names to apply value replacement to.
- `values_to_replace`: Comma-separated list of exact values to be replaced. Only complete column value matches will be replaced, not partial string matches.
- `replacement_value`: The single value to replace all specified exact matches with.
- `values_delimiter`: Optional. Delimiter to separate each value. Default: `,`. Change it if there's a comma in one of your values.

```sql
-- In the 'country' column, replace 'USA' with 'United States'
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'replace_values', 9, 'column_name', 'country'),
    (101, 'data_transformation_steps', 'replace_values', 9, 'values_to_replace', 'USA'),
    (101, 'data_transformation_steps', 'replace_values', 9, 'replacement_value', 'United States');
```

##### 17. mask_sensitive_data - Mask Sensitive Data
Use this to mask sensitive data in one or more columns using character-by-character replacement. Each character type (uppercase, lowercase, digit, other) can be replaced with a different masking character.

**Formatting Rule**: Specify the column(s) to mask and optionally specify replacement characters for each character type. If not specified, defaults to `X` for all character types.
- `column_name`: **Column(s) to mask - use comma-separated list for multiple columns with the same masking pattern**
- `upper_char`: Character to replace uppercase letters (optional, default: `X`)
- `lower_char`: Character to replace lowercase letters (optional, default: `X`)
- `digit_char`: Character to replace digits (optional, default: `X`)
- `other_char`: Character to replace other characters like punctuation (optional, leave empty to preserve)

> **⚠️ IMPORTANT**: When masking multiple columns with the **same settings**, use a single instance with comma-separated column names. Do NOT create duplicate instances. See [Golden Rule #18](#golden-rules-for-llm-metadata-generation).

```sql
-- ✅ PREFERRED: Mask multiple PII columns with same pattern in ONE instance
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'mask_sensitive_data', 10, 'column_name', 'email,phone,credit_card_number'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 10, 'upper_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 10, 'lower_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 10, 'digit_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 10, 'other_char', 'X');
-- Result: All three columns masked with X characters
```

```sql
-- Custom masking for SSN: Use # for digits, preserve dashes
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'mask_sensitive_data', 11, 'column_name', 'social_security_number'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 11, 'digit_char', '#'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 11, 'other_char', '-');
-- Result: "123-45-6789" → "###-##-####"
```

```sql
-- When columns need DIFFERENT masking patterns, use separate instances
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Instance 12: Preserve email structure (@ and . visible)
    (101, 'data_transformation_steps', 'mask_sensitive_data', 12, 'column_name', 'email'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 12, 'upper_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 12, 'lower_char', 'x'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 12, 'digit_char', '9'),
    -- other_char not specified = preserve @ and . characters
    -- Instance 13: Full masking for credit card (hide dashes too)
    (101, 'data_transformation_steps', 'mask_sensitive_data', 13, 'column_name', 'credit_card'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 13, 'digit_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 13, 'other_char', 'X');
```

##### 18. pivot_data - Pivot Data
Use this to pivot a table, transforming rows into columns. Supports single or multiple value columns with corresponding aggregations. PySpark now controls the output column aliases entirely, so no additional metadata is needed to rename them.

**Formatting Rule**: Specify the columns to group by, the column whose values will become new column headers, the value column(s), and the aggregation function(s).
- `group_columns`: Comma-separated list of columns to group by during pivot operation.
- `pivot_column`: The column whose values will become new column names.
- `value_column`: The column(s) whose values will populate the pivoted columns. Can be comma-separated for multiple value columns (e.g., `revenue,units,profit`).
- `aggregation`: **Required.** Aggregation function(s) to apply. For multiple value columns, provide comma-separated aggregations matching each value column (e.g., `sum,avg,max`). Count must match value_column count. Options: `sum`, `avg`, `min`, `max`, `count`, `first`, `last`.

```sql
-- Example 1: Single value column pivot with detailed naming
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'pivot_data', 13, 'group_columns', 'product'),
    (101, 'data_transformation_steps', 'pivot_data', 13, 'pivot_column', 'year'),
    (101, 'data_transformation_steps', 'pivot_data', 13, 'value_column', 'sales'),
    (101, 'data_transformation_steps', 'pivot_data', 13, 'aggregation', 'sum');
-- Results in columns like: 2023_sum(sales), 2024_sum(sales)

-- Example 2: Multiple value columns with simplified naming
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (102, 'data_transformation_steps', 'pivot_data', 1, 'group_columns', 'region'),
    (102, 'data_transformation_steps', 'pivot_data', 1, 'pivot_column', 'quarter'),
    (102, 'data_transformation_steps', 'pivot_data', 1, 'value_column', 'revenue,units,profit'),
    (102, 'data_transformation_steps', 'pivot_data', 1, 'aggregation', 'sum,avg,max');
-- Results in columns like: Q1_sum(revenue), Q1_avg(units), Q1_max(profit)
```

##### 19. unpivot_data - Unpivot Data
Use this to unpivot a table, transforming columns into rows. This is the inverse of a pivot operation.

**Formatting Rule**: Specify the columns to keep as-is, the columns to unpivot, and the names for the new columns that will hold the unpivoted column names and their values.
- `id_columns`: Comma-separated list of columns to keep as identifiers (not unpivoted).
- `value_columns`: Comma-separated list of columns to unpivot into rows.
- `variable_name`: Optional. Name for the new column containing the original column names. Default: `variable`.
- `value_name`: Optional. Name for the new column containing the values. Default: `value`.

```sql
-- Unpivot a table that has sales data in columns for each year (e.g., '2022', '2023')
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'unpivot_data', 14, 'id_columns', 'product_id,product_name'),
    (101, 'data_transformation_steps', 'unpivot_data', 14, 'value_columns', '2022,2023,2024'),
    (101, 'data_transformation_steps', 'unpivot_data', 14, 'variable_name', 'sales_year'),
    (101, 'data_transformation_steps', 'unpivot_data', 14, 'value_name', 'total_sales');
```

##### 20. add_window_function - Add Window Function
Use this to add a new column based on a window function calculation (e.g., `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM OVER`).
Use this to add a new column based on a window function calculation (e.g., `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM OVER`).

**Formatting Rule**: Define the source column (for aggregations), output column name, window function type, and optional partitioning/ordering.
- `column_name`: Required for aggregation or analytic window functions (`sum`, `avg`, `min`, `max`, `count`, `first_value`, `last_value`). Omit for ranking functions (`row_number`, `rank`, `dense_rank`).
- `output_column_name`: **Required.** The name of the output column to be created.
- `window_function`: The type of window function. Valid values: `row_number`, `rank`, `dense_rank`, `sum`, `avg`, `count`, `min`, `max`, `first_value`, `last_value`.
- `window_function`: The type of window function. Valid values: `row_number`, `rank`, `dense_rank`, `sum`, `avg`, `count`, `min`, `max`, `first_value`, `last_value`.
- `partition_by`: Optional. Comma-separated list of columns to partition by.
- `order_by`: Optional. Comma-separated list of columns to order by.
- `order_direction`: Optional. Sort direction - single value applied to all order_by columns, or comma-separated values matching each order_by column. Valid values: `asc`, `desc`. Default: `asc`.

```sql
-- Add a row number to each customer's orders, ordered by date
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'add_window_function', 15, 'output_column_name', 'order_rank'),
    (101, 'data_transformation_steps', 'add_window_function', 15, 'window_function', 'row_number'),
    (101, 'data_transformation_steps', 'add_window_function', 15, 'partition_by', 'customer_id'),
    (101, 'data_transformation_steps', 'add_window_function', 15, 'order_by', 'order_date'),
    (101, 'data_transformation_steps', 'add_window_function', 15, 'order_direction', 'desc');

-- Multiple order columns with individual directions
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'add_window_function', 16, 'output_column_name', 'priority_rank'),
    (101, 'data_transformation_steps', 'add_window_function', 16, 'window_function', 'rank'),
    (101, 'data_transformation_steps', 'add_window_function', 16, 'partition_by', 'region'),
    (101, 'data_transformation_steps', 'add_window_function', 16, 'order_by', 'priority,order_date,customer_id'),
    (101, 'data_transformation_steps', 'add_window_function', 16, 'order_direction', 'desc,desc,asc');
```

```sql
-- Calculate the running total of sales within each product category
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'add_window_function', 17, 'column_name', 'sales_amount'),
    (101, 'data_transformation_steps', 'add_window_function', 17, 'output_column_name', 'running_total'),
    (101, 'data_transformation_steps', 'add_window_function', 17, 'window_function', 'sum'),
    (101, 'data_transformation_steps', 'add_window_function', 17, 'partition_by', 'category'),
    (101, 'data_transformation_steps', 'add_window_function', 17, 'order_by', 'order_date');
```

##### 21. transform_datetime - Transform Datetime
Use this to perform various transformations on datetime columns, such as formatting or extracting date/time parts.

**Formatting Rule**: Specify the source column, the operation type, output column name, and any required parameters.
- `column_name`: Name of the date/time column to transform.
- `operation`: Type of datetime transformation to apply. Options: `year`, `month`, `day`, `dayofmonth`, `dayofweek`, `dayofyear`, `hour`, `minute`, `second`, `quarter`, `weekofyear`, `add_days`, `subtract_days`, `add_months`, `date_diff`, `format_date`, `to_timestamp`, `current_date`, `current_timestamp`.
- `output_column_name`: **Required.** Name for the new output column.
- `date_format`: Required when operation is `format_date`. Date format string (e.g., `yyyy-MM-dd`).
- `days`: Required when operation is `add_days` or `subtract_days`. Number of days to add/subtract.
- `months`: Required when operation is `add_months`. Number of months to add.
- `end_date_column`: Required when operation is `date_diff`. Column containing the end date.

```sql
-- Format the 'order_date' column to 'yyyy-MM-dd' format
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'transform_datetime', 17, 'column_name', 'order_date'),
    (101, 'data_transformation_steps', 'transform_datetime', 17, 'operation', 'format_date'),
    (101, 'data_transformation_steps', 'transform_datetime', 17, 'date_format', 'yyyy-MM-dd'),
    (101, 'data_transformation_steps', 'transform_datetime', 17, 'output_column_name', 'formatted_order_date');
```

```sql
-- Extract the year from the 'order_date' and store it in a new 'order_year' column
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'transform_datetime', 18, 'column_name', 'order_date'),
    (101, 'data_transformation_steps', 'transform_datetime', 18, 'operation', 'year'),
    (101, 'data_transformation_steps', 'transform_datetime', 18, 'output_column_name', 'order_year');
```

##### 22. union_data - Union Data
Use this to combine the rows from one or more tables into the current table.

**Formatting Rule**: Specify the table(s) to union with and union options.
- `union_tables`: Comma-separated list of table names to union with. Use datastore.schema.table format or just table name.
- `union_type`: Type of union operation. `by_name` uses PySpark unionByName() (column name matching). `by_position` uses PySpark unionAll() (positional matching). Default: `by_name`.
- `allow_missing_columns`: Whether to allow missing columns in union by name. Default: `true`.
- `deduplicate`: Whether to remove duplicates after union. Default: `false`.

```sql
-- Union the current table with another table, matching columns by name
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'union_data', 19, 'union_tables', 'silver.dbo.other_sales'),
    (101, 'data_transformation_steps', 'union_data', 19, 'union_type', 'by_name'),
    (101, 'data_transformation_steps', 'union_data', 19, 'allow_missing_columns', 'true'),
    (101, 'data_transformation_steps', 'union_data', 19, 'deduplicate', 'false');
```

##### 23. entity_resolution - Entity Resolution
Use this to perform entity resolution by comparing records from primary and secondary datasets using fuzzy matching.

**Formatting Rule**: Specify aliases, comparison fields, comparison types, and matching logic.
- `primary_dataset_alias`: Required. Alias for the primary dataset (derived from files or source delta via query).
- `secondary_dataset_alias`: Required. Alias for the secondary dataset.
- `secondary_dataset_query`: Required. SQL query to fetch the secondary dataset for comparison.
- `primary_dataset_comparison_fields`: Required. Comma-separated column names from the primary dataset for comparison. Order matters.
- `secondary_dataset_comparison_fields`: Required. Comma-separated column names from the secondary dataset for comparison. Order matches `primary_dataset_comparison_fields`.
- `comparison_types`: Required. Comma-separated comparison types. Available options: `soundex`, `fuzzy`, `exact`, `percent_difference`. Order matches field lists.
- `only_fuzzy_match_if_one_exact_match`: Optional. If `true`, fuzzy matching only applied to entities with at least one exact or soundex match. Default: `true`.
- `use_ngram_filtering_for_fuzzy_comparisons`: Optional. If `true`, ngram matching at 50% threshold runs before levenshtein matching. Default: `false`.
- `auto_match_logic`: Required. Spark SQL filter expression to identify automatic matches. Result columns named: `{primary_alias}_{primary_field}_{secondary_alias}_{secondary_field}_{type}`.
- `match_with_manual_review_logic`: Required. Spark SQL filter expression for matches needing manual review.

```sql
-- Resolve customer entities by comparing primary and secondary customer datasets
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'primary_dataset_alias', 'customers_a'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'secondary_dataset_alias', 'customers_b'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'secondary_dataset_query', 'select * from silver.dbo.other_customers'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'primary_dataset_comparison_fields', 'customer_name,zip_code'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'secondary_dataset_comparison_fields', 'name,postal_code'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'comparison_types', 'fuzzy,exact'),
    (101, 'data_transformation_steps', 'entity_resolution', 20, 'auto_match_logic', 'customers_a_customer_name_customers_b_name_fuzzy > 0.9 and customers_a_zip_code_customers_b_postal_code_exact = 1');
```

##### 24. change_data_types - Change Data Types
Use this to change the data type of one or more columns.

**Formatting Rule**: Specify the column(s) and the new data type.

```sql
-- Change data type of columns
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'change_data_types', 5, 'column_name', 'order_id'),
    (101, 'data_transformation_steps', 'change_data_types', 5, 'new_type', 'string');
```

##### 25. custom_table_ingestion_function - Custom Table Ingestion Function
Use this to execute custom Python functions for complex data ingestion from databases or Delta tables. This is configured in the **Primary Configuration** table under the `source_details` category, not in Advanced Configuration.

> **⚠️ When using `custom_table_ingestion_function`, you MUST create the actual notebook file, not just the metadata!**

> **🔴 STOP! Do you actually need `custom_table_ingestion_function`?**
>
> **Most file ingestion does NOT require custom code!** The accelerator has built-in support for:
> - ✅ CSV files → Use `wildcard_folder_path` + `delimiter` + `file_has_header_row` (see Section B)
> - ✅ JSON files → Use `wildcard_folder_path` + `multiline` (see Section B)
> - ✅ Parquet files → Use `wildcard_folder_path` (see Section B)
> - ✅ Excel files → Use `wildcard_folder_path` + `sheet_name` (see Section B)
> - ✅ Simple XML → Use `wildcard_folder_path` + `xml_xpath` (see Section B)
>
> **Only use `custom_table_ingestion_function` when:**
> - ❌ Multi-table joins during ingestion from databases
> - ❌ Complex SQL logic requiring dynamic query generation
> - ❌ Reading from multiple Delta tables with complex joins
>
> **For external API or SDK data sources**, use `custom_source_function` when the function should return a DataFrame, or `custom_staging_function` when the function must write staged files itself.
> **For proprietary file formats**, use `custom_file_ingestion_function` (Section 25b) instead.
>
> **Common mistake:** Seeing complex Teradata/Oracle SQL with joins and thinking you need custom ingestion.
> **Reality:** Ingest files with built-ins, then use `custom_transformation_function` (Section 10) for complex transformations.

**Required Attributes:**
- `custom_table_ingestion_function`: Name of a single Python function to execute (without parentheses). **Note: Only ONE function can be specified, not comma-separated lists.**
- `custom_table_ingestion_function_notebook`: Name of the notebook containing the function **without** the `.py` extension (must match a notebook you create in `src/`)

**Custom Attributes (Optional):**
You can add any additional attributes prefixed with `custom_table_ingestion_function_` under the `source_details` category. These are passed to your function via the `metadata` dict and accessed using the **exact** `Configuration_Name` from your metadata:

```python
# If metadata has: (200, 'source_details', 'custom_table_ingestion_function_base_url', 'https://api.example.com')
# Access it as:
primary_config = metadata.get('primary_config', {})
base_url = primary_config.get('custom_table_ingestion_function_base_url')  # Returns 'https://api.example.com'
```

**Use Cases:**
- Reading from multiple database tables with complex joins or unions
- Applying custom business logic during data extraction from databases
- Complex query logic requiring dynamic SQL generation
- Custom aggregations or transformations during ingestion from Delta tables
- When standard source_details configurations are insufficient

**Important Notes:**
- Custom functions must follow specific parameter and return value signatures
- See reference examples: [Custom Table Ingestion Function](resources/Custom_Table_Ingestion_Function.py) and [Custom File Ingestion Function](resources/Custom_File_Ingestion_Function.py)
- **Only ONE custom function** can be specified per Table_ID
- Can be combined with `watermark_details` for incremental loading
- Can be combined with `data_cleansing` settings for standardization
- When watermarking is enabled, the watermark value is passed to your custom function
- **When using custom ingestion with Delta tables AND watermarking**, you must specify `watermark_details.table_name` to indicate which table to extract the watermark value from

<details>
<summary><b>📁 Click to expand: Notebook Template & Folder Structure</b></summary>

**Deliverables when using custom_table_ingestion_function:**
1. ✅ The metadata SQL file referencing the custom function
2. ✅ the actual Python module file (`.sql` folder) containing the Python function

**Create the notebook at:** `src/custom_<your_module>.sqlnotebook-content.py`

```python
# Cell 1: Import Warning
# ⚠️ Do NOT put imports at the top level of custom notebooks.
# Custom notebooks execute in a shared namespace — top-level imports
# pollute globals() and can shadow critical symbols.
# Place all imports INSIDE your function body instead.

# Cell 2: Main Custom Table Ingestion Function (REQUIRED)
# Signature is different from custom_transformation_function - no new_data parameter
def your_table_ingestion_function(metadata: dict, spark) -> DataFrame:
    """Custom table ingestion function - returns a new DataFrame."""
    # Imports — must be INSIDE the function (not at top level)
    from pyspark.sql import functions as f
    from pyspark.sql.types import StructType, StructField, StringType  # only what you need

    # Available metadata keys for custom_table_ingestion_function:
    orchestration_metadata = metadata.get('orchestration_metadata', {})
    primary_config = metadata.get('primary_config', {})
    advanced_config = metadata.get('advanced_config', [])
    datastore_config = metadata.get('datastore_config', [])  # List of dicts from Datastore_Configuration table
    event_payload = metadata.get('event_payload', '')
    
    # Use _get_datastore_config() to look up datastore properties (available from helper_functions_3.py)
    # Example: silver_catalog = _get_datastore_config(datastore_config, 'silver', 'Catalog_Name')
    
    # For incremental loading with watermarks:
    watermark_filter = metadata.get('watermark_filter', '')  # Pre-built SQL WHERE clause
    watermark_column_name = metadata.get('watermark_column_name', '')
    watermark_value = metadata.get('watermark_value', '')
    
    # Access any custom_table_ingestion_function_* attributes from primary_config
    my_param = primary_config.get('custom_table_ingestion_function_my_param')
    
    # Your custom ingestion logic here
    df = spark.sql(f"""
        SELECT * FROM silver.dbo.source_table
        WHERE modified_date > '{watermark_value}'
    """)
    
    return df  # MUST return a Spark DataFrame
```

**Folder structure:**
```
src/
└── custom_your_module_name.sql
    ├── # module file
    └── notebook-content.py # Your Python code
```

**`.platform` file template:**
```json
{
  "$schema": "removed - not needed for .py files.json",
  "metadata": {
    "type": "Notebook",
    "displayName": "custom_your_module_name",
    "description": "Custom ingestion functions for [describe purpose]"
  },
  "config": {
    "version": "2.0",
    "logicalId": "00000000-0000-0000-0000-000000000000"
  }
}
```

</details>

> **⚠️ Non-Standard Library Warning**: If your custom ingestion function imports libraries NOT included in the standard Databricks Runtime, you MUST configure a Databricks compute cluster with those libraries installed. See [Manage libraries on Databricks compute](https://learn.microsoft.com/azure/databricks/libraries/). The validator will detect non-standard imports automatically.

> **⚠️ Config Attribute Validation**: If your ingestion function accesses `primary_config.get('custom_table_ingestion_function_my_param')`, the validator verifies that attribute exists in your `source_details` metadata. For example:
> ```python
> api_key = primary_config.get('custom_table_ingestion_function_api_key')  # Validator checks metadata defines this attribute
> ```
> Missing attributes generate warnings to help prevent runtime errors.

**Custom Function Requirements:**
1. **Function Signature**: Must accept specific parameters defined in the framework (see example notebooks)
2. **Return Value**: Must return a PySpark DataFrame
3. **Error Handling**: Include try-except blocks for robust error handling
4. **Logging**: Use print statements for debugging (appears in pipeline logs)
5. **Schema Validation**: Validate DataFrame schema before returning

```sql
-- Example 1: Basic custom table ingestion from Delta tables
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (101, 'source_details', 'custom_table_ingestion_function', 'extract_sales_with_aggregations'),
    (101, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_Sales_Ingestion');
```

```sql
-- Example 2: Custom table ingestion with watermark for incremental loading from Delta tables
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (102, 'source_details', 'custom_table_ingestion_function', 'extract_incremental_sales'),
    (102, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_sales_extractors'),
    -- Enable watermarking (watermark value passed to custom function)
    -- REQUIRED: Specify table name when using custom ingestion with Delta tables
    (102, 'watermark_details', 'table_name', 'silver.dbo.sales_transactions'),
    (102, 'watermark_details', 'column_name', 'ModifiedDate'),
    (102, 'watermark_details', 'data_type', 'datetime');
```

```sql
-- Example 3: Custom file ingestion (e.g., complex XML parsing)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (103, 'source_details', 'wildcard_folder_path', 'oracle/Sales/*/*.xml'),
    (103, 'source_details', 'datastore_name', 'bronze'),
    (103, 'source_details', 'custom_file_ingestion_function', 'ingest_xml_sales_data'),
    (103, 'source_details', 'custom_file_ingestion_function_notebook', 'custom_ingest_sales_xml_data');
```

```sql
-- Example 4: Custom table ingestion with data cleansing
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (104, 'source_details', 'custom_table_ingestion_function', 'extract_complex_sales_query'),
    (104, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_Queries'),
    -- Apply column name cleansing after custom extraction
    (104, 'column_cleansing', 'trim', 'true'),
    (104, 'column_cleansing', 'apply_case', 'lower'),
    (104, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
    -- Apply data value cleansing (use '*' for all string columns or comma-separated column names)
    (104, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
    (104, 'data_cleansing', 'trim_data_in_string_columns', '*');
```

```sql
-- Example 5: Custom table ingestion with multiple watermark columns (Delta tables ONLY)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (105, 'source_details', 'custom_table_ingestion_function', 'extract_modified_or_created_records'),
    (105, 'source_details', 'custom_table_ingestion_function_notebook', 'custom_delta_extractors'),
    -- Multiple watermark columns (uses OR logic) - ONLY supported for Delta tables
    (105, 'watermark_details', 'table_name', 'silver.dbo.products'),
    (105, 'watermark_details', 'column_name', 'ModifiedDate, CreatedDate'),
    (105, 'watermark_details', 'data_type', 'datetime');
```

##### 25b. custom_file_ingestion_function - Custom File Ingestion
Use this when you need custom parsing logic for files (XML, complex CSV, proprietary formats, etc.) combined with `wildcard_folder_path`. The function signature is **different** from `custom_table_ingestion_function`.

> **⚠️ IMPORTANT: File-based custom ingestion uses a DIFFERENT function signature!**

**Required Attributes:**
- `wildcard_folder_path`: Glob pattern for files to process (e.g., `oracle/Sales/*/*.xml`)
- `custom_file_ingestion_function`: Name of the Python function to execute
- `custom_file_ingestion_function_notebook`: Name of the notebook containing the function

**Optional Attributes:**
- `datastore_name`: catalog containing the files (default: `bronze`)

**Use Cases:**
- Complex XML/JSON parsing with nested structures
- Proprietary file formats requiring custom parsing
- Files requiring data enrichment during ingestion
- Multi-file processing with custom union/merge logic

**Function Signature (FILE-BASED - Different signature!):**
```python
def your_file_ingestion_function(file_paths: list, all_metadata: dict, spark) -> DataFrame:
    """
    Custom file ingestion function.
    
    Args:
        file_paths: List of abfss:// paths to files matching wildcard_folder_path
        all_metadata: Dictionary with all configuration details
        spark: SparkSession instance
    
    Returns:
        PySpark DataFrame with ingested data
    """
    # Available keys in all_metadata for custom_file_ingestion_function:
    orchestration_metadata = all_metadata.get('orchestration_metadata', {})
    primary_config = all_metadata.get('primary_config', {})
    advanced_config = all_metadata.get('advanced_config', [])
    datastore_config = all_metadata.get('datastore_config', [])  # List of dicts from Datastore_Configuration table
    event_payload = all_metadata.get('event_payload', '')
    
    # Use _get_datastore_config() to look up datastore properties (available from helper_functions_3.py)
    # Example: silver_catalog = _get_datastore_config(datastore_config, 'silver', 'Catalog_Name')
    
    result_df = None
    for file_path in file_paths:
        df = spark.read.text(file_path)  # or .xml(), .json(), etc.
        if result_df is None:
            result_df = df
        else:
            result_df = result_df.unionByName(df, allowMissingColumns=True)
    
    return result_df
```

> **⚠️ ABFSS Path Compatibility:** The `file_paths` parameter contains ABFSS paths. **Spark and Pandas** can use these directly. However, **standard Python file I/O** (`open()`, `zipfile`, `PIL`, `PyPDF2`, etc.) requires **local file system paths**. Use the helper functions from `helper_functions_3.py`:
> - `_mount_catalog_for_local_access(file_paths, table_id)` - Mount catalog, returns `(mount_point, local_mount_path, catalog_root)`
> - `_mount_abfss_path_for_local_access(abfss_path, table_id)` - Mount and resolve a single ABFSS path for write-oriented scenarios such as custom staging
> - `_convert_abfss_paths_to_local(file_paths, local_mount_path, catalog_root)` - Convert ABFSS to local paths
> - `_unmount_catalog(mount_point)` - Cleanup when done
>
> See [Custom File Ingestion Function Template](resources/Custom_File_Ingestion_Function.py) for complete examples.

**Reference Example:** See [Custom File Ingestion Function](resources/Custom_File_Ingestion_Function.py) for complete implementation.

```sql
-- Example: Custom XML file ingestion
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (106, 'source_details', 'wildcard_folder_path', 'erp/orders/*/*.xml'),
    (106, 'source_details', 'datastore_name', 'bronze'),
    (106, 'source_details', 'custom_file_ingestion_function', 'parse_erp_order_xml'),
    (106, 'source_details', 'custom_file_ingestion_function_notebook', 'custom_erp_xml_parser');
```

##### 25c. custom_staging_function - Custom Staging Function
Use this when you need **full control over both data extraction AND file writing** during the staging phase. Unlike `custom_table_ingestion_function` (which returns a DataFrame for the orchestrator to write), a custom staging function handles the entire staging process and returns only a result summary.

> **⚠️ When using `custom_staging_function`, you MUST create the actual notebook file, not just the metadata!**

> **🔴 STOP! Do you actually need `custom_staging_function`?**
> - Can you use a standard `source` type (`azure_sql`, `oracle`, `sftp`, etc.)? → Use that instead
> - Do you just need custom data extraction logic from databases/Delta tables? → Use `custom_table_ingestion_function` (Section 25a) — simpler, the orchestrator writes the data
> - Do you need custom **file parsing**? → Use `custom_file_ingestion_function` (Section 25b) instead
>
> **Only use `custom_staging_function` when:**
> - You need to ingest from external data sources (APIs, third-party services) with custom PySpark logic
> - You need full control over HOW and WHERE data files are written (file format, partitioning, chunking)
> - You need to call external APIs (tabular or file-based)
> - You need custom retry/pagination logic during file writes
> - You need to process and write data incrementally in chunks

**Required Attributes:**
- `custom_staging_function`: Name of a single Python function to execute (without parentheses). **Note: Only ONE function can be specified.**
- `custom_staging_function_notebook`: Name of the notebook containing the function **without** the `.py` extension (must match a notebook you create in `src/`)
- `staging_volume_name`: Name of the catalog to stage files in
- `staging_folder_path`: Subfolder path within the staging volume

You can add any additional attributes prefixed with `custom_staging_function_` under the `source_details` category. These are passed to your function via the `metadata['primary_config']` dict:

```python
# If metadata has: (200, 'source_details', 'custom_staging_function_api_endpoint', 'https://api.example.com')
# In the primary_config dict, access it as the FULL attribute name with category prefix:
# primary_config = metadata.get('primary_config', {})
api_endpoint = primary_config.get('source_details_custom_staging_function_api_endpoint')  # Returns 'https://api.example.com'
```

**⚠️ Important Orchestrator Differences from Table/File Ingestion:**
- Orchestrated by `batch_processing.py`
- Triggered by `custom_staging_function` metadata instead of a `source` switch value
- `Processing_Method` must be `'batch'`
- Use `source_details.exit_after_staging = 'true'` when you want notebook-owned stage-only behavior
- See reference examples: [Custom Staging Function](resources/Custom_Staging_Function.py), [Custom Table Ingestion Function](resources/Custom_Table_Ingestion_Function.py)

**Deliverables when using custom_staging_function:**
1. **Metadata SQL** with `custom_staging_function` and required staging attributes
2. **Notebook file** at `src/{notebook_name}.sqlnotebook-content.py` containing the function

**Function Signature:**
```python
def your_staging_function(metadata: dict, spark) -> dict:
    """
    Custom staging function — handles both extraction and file writing.
    
    Args:
        metadata: Dictionary with all pipeline parameters:
            - watermark_value: Previous high watermark for incremental processing
            - primary_config: Source details configuration (all source_details_* attributes)
            - staging_datastore_config: Staging datastore configuration
            - source_datastore_config: Source datastore configuration
            - orchestration_metadata: Orchestration metadata (Table_ID, Processing_Method, etc.)
            - target_folderpath_w_ingestion_type: Target staging folder path with timestamp (no trailing slash)
        spark: SparkSession instance
    
    Returns:
        dict with keys:
            - rows_copied (int): Number of records staged
            - next_watermark_value (str): Updated watermark for next incremental run
    """
    watermark_value = metadata.get('watermark_value', '')
    primary_config = metadata.get('primary_config', {})
    target_folderpath = metadata.get('target_folderpath_w_ingestion_type', '')
    staging_datastore_config = metadata['staging_datastore_config']
    
    # Look up workspace URL and catalog for ABFSS / volume path construction
    staging_workspace_url = staging_datastore_config['Workspace_URL']
    staging_catalog = staging_datastore_config['Catalog_Name']
    
    # Extract data from source
    df = spark.sql("SELECT * FROM source_table WHERE modified_date > '{}'".format(watermark_value))
    
    # Write to staging folder
    output_path = f"/Volumes/{catalog}/{schema}/files/{target_folderpath}"
    df.write.mode("overwrite").parquet(output_path)

    # Your function owns filenames/extensions inside target_folderpath.
    # Append '/{file_name}' when you need a specific file path under the folder.
    # Write direct child files only. Do not create subfolders under target_folderpath.
    # For non-Spark writes, use _mount_abfss_path_for_local_access(output_path, Table_ID).
    
    rows_copied = df.count()
    next_watermark = df.agg({"modified_date": "max"}).collect()[0][0] or watermark_value
    
    return {
        "rows_copied": rows_copied,
        "next_watermark_value": str(next_watermark)
    }
```

> **⚠️ Config Attribute Validation**: If your staging function accesses `primary_config.get('source_details_custom_staging_function_my_param')`, the validator verifies that attribute exists in your `source_details` metadata. For example:
>
> `api_endpoint = primary_config.get('source_details_custom_staging_function_api_endpoint')`  # Validator checks metadata defines this attribute

**Reference Example:** See [Custom Staging Function](resources/Custom_Staging_Function.py) for complete implementation.

> **Important:** The framework supplies the timestamped staging folder via `target_folderpath_w_ingestion_type` without a trailing slash. Custom staging functions are responsible for naming files, choosing extensions, and selecting Spark vs non-Spark write methods inside that folder. Write direct child files only. Zip files are allowed. Subfolders are rejected, and an empty folder is treated as no new data.

```sql
-- Example: Custom staging with API data extraction
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (107, 'source_details', 'custom_staging_function', 'stage_api_data'),
    (107, 'source_details', 'custom_staging_function_notebook', 'custom_API_Staging'),
    (107, 'source_details', 'staging_volume_name', 'Bronze'),
    (107, 'source_details', 'staging_folder_path', 'staging/api_data');
```

##### 26. execute_databricks_notebook / execute_databricks_job / execute_warehouse_sp - Execute workspace items
Use these processing methods to execute existing notebooks or dataflows as part of your orchestrated data pipeline.

**Formatting Rule**: Configure in **Primary Configuration** table under `source_details` category.
- `source_details.datastore_name`: Name of the Datastore_Configuration entry that stores the item GUID and workspace GUID

**Use Cases:**
- Execute specialized notebooks (ML training, API calls, complex processing)
- Run Power Query dataflows as part of pipeline
- Cross-workspace item execution

**Important Notes:**
- `Processing_Method` must be set to `'execute_databricks_notebook'`, `'execute_databricks_job'`, or `'execute_warehouse_sp'`
- `Primary_Keys` is NOT required for these processing methods
- All executed items are logged in framework logging table
- **CI/CD:** Register the executable artifact under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json` and reference it by `datastore_name`. For notebooks/jobs, use `kind: "databricks_notebook"` or `kind: "databricks_job"` with `workspace_url` and the notebook path / job ID in `connection_details`. For warehouse stored procedures, register the warehouse host and put the procedure name in `source_details.stored_procedure_name`.

**Datastore Configuration Setup:**
Add entries to `databricks_batch_engine/datastores/datastore_<ENV>.json` under `external_datastores`. Example for DEV:
```jsonc
"external_datastores": {
  "my_ml_notebook": {
    "kind": "databricks_notebook",
    "connection_details": {
      "workspace_url": "https://adb-dev.azuredatabricks.net",
      "notebook_path": "/Workspace/Shared/ml_predictions"
    }
  },
  "my_databricks_job": {
    "kind": "databricks_job",
    "connection_details": {
      "workspace_url": "https://adb-dev.azuredatabricks.net",
      "job_id": "123456789"
    }
  }
}
```
Promote by editing `datastore_PROD.json` with the PROD workspace URL and IDs. `/fdp-04-commit` MERGEs each environment's entries into the `Datastore_Configuration` Delta table.

```sql
-- Example 1: Execute notebook
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES ('ExecuteItems', 1, 106, 'gold', 'dbo.ml_predictions', 'execute_databricks_notebook', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (106, 'source_details', 'datastore_name', 'my_ml_notebook');
```

```sql
-- Example 2: Execute external job
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES ('ExecuteItems', 2, 107, 'silver', 'dbo.pq_transformed', 'execute_databricks_job', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (107, 'source_details', 'datastore_name', 'my_pq_dataflow');
```

```sql
-- Example 3: Cross-workspace execution (workspace_id is set in Datastore_Configuration)
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES ('ExecuteItems', 3, 108, 'gold', 'dbo.cross_workspace_data', 'execute_databricks_notebook', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (108, 'source_details', 'datastore_name', 'external_workspace_notebook');
```

```sql
-- Example 4: Execute warehouse stored procedure
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Processing_Method],[Ingestion_Active])
VALUES ('ExecuteItems', 4, 109, 'gold', 'dbo.sales_load_audit', 'execute_warehouse_sp', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (109, 'source_details', 'datastore_name', 'sales_warehouse'),
    (109, 'source_details', 'stored_procedure_name', '[dbo].[RunSalesLoad]');
```

##### 27. sort_data - Sort Data
Use this to sort the DataFrame by one or more columns with configurable sort directions.

**Formatting Rule**: Specify the column(s) to sort by and optionally the sort direction(s).
- `column_name`: Required. Comma-separated column names to sort by.
- `sort_direction`: Optional. `asc` or `desc` for each column (comma-separated if multiple). Default: `asc` for all.

```sql
-- Sort by single column descending
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'sort_data', 21, 'column_name', 'created_date'),
    (101, 'data_transformation_steps', 'sort_data', 21, 'sort_direction', 'desc');

-- Sort by multiple columns with different directions
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'sort_data', 21, 'column_name', 'customer_id, order_date'),
    (101, 'data_transformation_steps', 'sort_data', 21, 'sort_direction', 'asc, desc');
```

##### 28. explode_array - Explode Array Column
Use this to explode an array column into multiple rows. Each array element becomes a separate row.

**Formatting Rule**: Specify the array column and optionally preserve nulls.
- `column_name`: Required. The array column to explode.
- `output_column_name`: Optional. Name for the exploded column. Default: same as input.
- `preserve_nulls`: Optional. `true` to include rows with null/empty arrays (uses explode_outer). Default: `false`.

```sql
-- Basic array explosion
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'explode_array', 22, 'column_name', 'tags'),
    (101, 'data_transformation_steps', 'explode_array', 22, 'output_column_name', 'tag');

-- Preserve rows with null/empty arrays
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'explode_array', 22, 'column_name', 'phone_numbers'),
    (101, 'data_transformation_steps', 'explode_array', 22, 'output_column_name', 'phone'),
    (101, 'data_transformation_steps', 'explode_array', 22, 'preserve_nulls', 'true');
```

##### 29. flatten_struct - Flatten Nested Struct
Use this to flatten a nested struct column into individual columns.

**Formatting Rule**: Specify the struct column and optionally prefix and fields to include.
- `column_name`: Required. The struct column to flatten.
- `prefix`: Optional. Prefix for flattened column names. Default: `{column_name}_`.
- `fields_to_include`: Optional. Comma-separated list of specific fields to extract. Default: all fields.
- `drop_original`: Optional. `true` to drop original struct column. Default: `true`.

```sql
-- Flatten all fields in address struct
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'flatten_struct', 23, 'column_name', 'address');
-- Result: address_street, address_city, address_zip columns

-- Flatten with custom prefix and specific fields
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'flatten_struct', 23, 'column_name', 'customer_info'),
    (101, 'data_transformation_steps', 'flatten_struct', 23, 'prefix', 'cust_'),
    (101, 'data_transformation_steps', 'flatten_struct', 23, 'fields_to_include', 'name, email, phone'),
    (101, 'data_transformation_steps', 'flatten_struct', 23, 'drop_original', 'false');
-- Result: cust_name, cust_email, cust_phone columns (keeps original customer_info)
```

##### 30. split_column - Split String Column
Use this to split a string column into multiple columns based on a delimiter.

**Formatting Rule**: Specify the column, delimiter, and output column names.
- `column_name`: Required. The string column to split.
- `delimiter`: Optional. The delimiter character(s). Default: `,`.
- `output_column_names`: Required. Comma-separated names for output columns.
- `max_splits`: Optional. Maximum number of splits to perform. When set, the remainder is preserved in the last output column. For example, `max_splits=2` on `'A-B-C-D-E'` produces `['A', 'B', 'C-D-E']`. Default: `-1` (split on all delimiters).
- `drop_original`: Optional. `true` to drop original column. Default: `false`.

```sql
-- Split full name into parts
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'split_column', 24, 'column_name', 'full_name'),
    (101, 'data_transformation_steps', 'split_column', 24, 'delimiter', ','),
    (101, 'data_transformation_steps', 'split_column', 24, 'output_column_names', 'first_name, last_name, suffix'),
    (101, 'data_transformation_steps', 'split_column', 24, 'drop_original', 'true');
-- Input: 'John,Doe,Jr' → Output: first_name='John', last_name='Doe', suffix='Jr'

-- Split with pipe delimiter
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'split_column', 24, 'column_name', 'categories'),
    (101, 'data_transformation_steps', 'split_column', 24, 'delimiter', '|'),
    (101, 'data_transformation_steps', 'split_column', 24, 'output_column_names', 'category1, category2, category3');
```

##### 31. concat_columns - Concatenate Columns
Use this to concatenate multiple columns into a single column.

**Formatting Rule**: Specify the columns to concatenate, output name, and optionally separator and null handling.
- `column_name`: Required. Comma-separated columns to concatenate.
- `output_column_name`: Required. Name for the concatenated column.
- `separator`: Optional. String to insert between values. Default: empty string.
- `null_handling`: Optional. How to handle NULLs: `skip` (omit nulls), `empty` (replace with empty string), `keep` (result is null if any input is null). Default: `skip`.
- `drop_original`: Optional. `true` to drop original columns. Default: `false`.

```sql
-- Concatenate name columns with space separator
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'concat_columns', 25, 'column_name', 'first_name, last_name'),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'output_column_name', 'full_name'),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'separator', ' ');
-- Input: first_name='John', last_name='Doe' → Output: full_name='John Doe'

-- Build full address with null handling
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'concat_columns', 25, 'column_name', 'street, city, state, zip'),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'output_column_name', 'full_address'),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'separator', ', '),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'null_handling', 'skip'),
    (101, 'data_transformation_steps', 'concat_columns', 25, 'drop_original', 'true');
```

##### 32. conditional_column - Conditional Column (CASE WHEN)
Use this to create a column with conditional logic, equivalent to SQL CASE WHEN.

Important limitation: `conditional_column` only emits literal values from metadata. `values` and `default_value` are not interpreted as column references or Spark SQL expressions.

**Formatting Rule**: Specify the output column name, conditions, values, and optionally a default value.
- `column_name`: Required. Name for the output column.
- `conditions`: Required. Comma-separated list of Spark SQL conditions.
- `values`: Required. Comma-separated list of literal values (must match conditions count).
- `default_value`: Optional. Literal value when no conditions match. Default: `null`.
- `values_delimiter`: Optional. Delimiter for parsing. Default: `,`. Change if conditions/values contain commas.

```sql
-- Size category based on value
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'conditional_column', 26, 'column_name', 'size_category'),
    (101, 'data_transformation_steps', 'conditional_column', 26, 'conditions', 'size < 10, size < 100'),
    (101, 'data_transformation_steps', 'conditional_column', 26, 'values', 'Small, Medium'),
    (101, 'data_transformation_steps', 'conditional_column', 26, 'default_value', 'Large');
-- Equivalent to: CASE WHEN size < 10 THEN 'Small' WHEN size < 100 THEN 'Medium' ELSE 'Large' END

-- Grade based on score
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'conditional_column', 27, 'column_name', 'grade'),
    (101, 'data_transformation_steps', 'conditional_column', 27, 'conditions', 'score >= 90, score >= 80, score >= 70, score >= 60'),
    (101, 'data_transformation_steps', 'conditional_column', 27, 'values', 'A, B, C, D'),
    (101, 'data_transformation_steps', 'conditional_column', 27, 'default_value', 'F');
-- Equivalent to: CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ... ELSE 'F' END

-- Custom delimiter (when conditions contain commas)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'conditional_column', 28, 'column_name', 'status_label'),
    (101, 'data_transformation_steps', 'conditional_column', 28, 'conditions', 'status = ''A''; status = ''I'''),
    (101, 'data_transformation_steps', 'conditional_column', 28, 'values', 'Active; Inactive'),
    (101, 'data_transformation_steps', 'conditional_column', 28, 'values_delimiter', ';'),
    (101, 'data_transformation_steps', 'conditional_column', 28, 'default_value', 'Unknown');

-- Risk level based on multiple factors
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'conditional_column', 29, 'column_name', 'risk_level'),
    (101, 'data_transformation_steps', 'conditional_column', 29, 'conditions', 'amount > 10000 AND country != ''US'', amount > 5000'),
    (101, 'data_transformation_steps', 'conditional_column', 29, 'values', 'High, Medium'),
    (101, 'data_transformation_steps', 'conditional_column', 29, 'default_value', 'Low');

-- If a branch must preserve an existing column value, use derived_column instead
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'derived_column', 30, 'column_name', 'source_table_name'),
    (101, 'data_transformation_steps', 'derived_column', 30, 'expression', 'CASE WHEN source_table_name IS NULL THEN ''silver.dbo.oracle_sales2'' ELSE source_table_name END');

-- Do NOT model that pattern with conditional_column + default_value='source_table_name'
-- because it will write the literal string 'source_table_name'
```

##### 33. string_functions - String Functions
Use this to apply string transformation functions to columns.

**Formatting Rule**: Specify the column, function, and output column name.
- `column_name`: Required. Use `*` for all string columns, or comma-separated column names.
- `function`: Required. The string function to apply (see table below).
- `output_column_name`: Required. Name for the output column (becomes **prefix** when using `*`, e.g., `trimmed` → `trimmed_col1`, `trimmed_col2`).

**Available Functions:**
| Function | Description |
|----------|-------------|
| `upper` | Convert to uppercase |
| `lower` | Convert to lowercase |
| `trim` | Remove leading/trailing whitespace |
| `ltrim` | Remove leading whitespace |
| `rtrim` | Remove trailing whitespace |
| `reverse` | Reverse string |
| `length` | Get string length |
| `initcap` | Capitalize first letter of each word |
| `soundex` | Get phonetic soundex code |

> **Note:** For advanced string operations (substring, padding, replace, regex), use `derived_column` with Spark SQL expressions.

```sql
-- Convert to uppercase
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'string_functions', 30, 'column_name', 'customer_name'),
    (101, 'data_transformation_steps', 'string_functions', 30, 'function', 'upper'),
    (101, 'data_transformation_steps', 'string_functions', 30, 'output_column_name', 'customer_name_upper');

-- Trim whitespace
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'string_functions', 31, 'column_name', 'description'),
    (101, 'data_transformation_steps', 'string_functions', 31, 'function', 'trim'),
    (101, 'data_transformation_steps', 'string_functions', 31, 'output_column_name', 'description_trimmed');

-- Get string length
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'string_functions', 32, 'column_name', 'notes'),
    (101, 'data_transformation_steps', 'string_functions', 32, 'function', 'length'),
    (101, 'data_transformation_steps', 'string_functions', 32, 'output_column_name', 'notes_length');

-- Apply to ALL string columns using *
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'string_functions', 33, 'column_name', '*'),
    (101, 'data_transformation_steps', 'string_functions', 33, 'function', 'trim'),
    (101, 'data_transformation_steps', 'string_functions', 33, 'output_column_name', 'trimmed');
-- Creates: trimmed_col1, trimmed_col2, etc. for all string columns (output_column_name is used as prefix)

-- For advanced operations, use derived_column instead:
-- Extract area code: derived_column with expression 'substring(phone_number, 1, 3)'
-- Left-pad: derived_column with expression 'lpad(product_code, 10, ''0'')'
-- Regex replace: derived_column with expression 'regexp_replace(phone, ''[^0-9]'', '''')'
```

##### 34. normalize_text - Normalize Text
Use this to apply multiple text normalization operations in sequence for consistent data matching.

**Formatting Rule**: Specify the column and the operations to apply in order.
- `column_name`: Required. Use `*` for all string columns, or comma-separated column names.
- `operations`: Optional. Comma-separated operations to apply in order. Default: `trim,lowercase,collapse_whitespace`.
- `output_column_name`: Optional. Output column name. Default: overwrites input column.

**Available Operations:**
| Operation | Description |
|-----------|-------------|
| `lowercase` | Convert to lowercase |
| `uppercase` | Convert to uppercase |
| `trim` | Remove leading/trailing whitespace |
| `collapse_whitespace` | Replace multiple spaces with single space |
| `remove_punctuation` | Remove punctuation characters |
| `remove_digits` | Remove numeric digits |
| `remove_special_chars` | Remove non-alphanumeric characters (keeps spaces) |
| `ascii_only` | Remove non-ASCII characters |
| `strip_accents` | Remove accent marks (é→e, ñ→n, etc.) |

```sql
-- Basic normalization: trim, lowercase, collapse whitespace
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'normalize_text', 28, 'column_name', 'company_name'),
    (101, 'data_transformation_steps', 'normalize_text', 28, 'operations', 'trim,lowercase,collapse_whitespace');
-- Input: '  ACME   Corp  ' → Output: 'acme corp'

-- Comprehensive normalization for entity matching
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'normalize_text', 28, 'column_name', 'customer_name'),
    (101, 'data_transformation_steps', 'normalize_text', 28, 'operations', 'trim,lowercase,collapse_whitespace,remove_punctuation,strip_accents'),
    (101, 'data_transformation_steps', 'normalize_text', 28, 'output_column_name', 'customer_name_normalized');
-- Input: '  José  O''Brien, Jr.  ' → Output: 'jose obrien jr'

-- Normalize multiple columns for matching
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'normalize_text', 29, 'column_name', 'first_name, last_name, company'),
    (101, 'data_transformation_steps', 'normalize_text', 29, 'operations', 'trim,uppercase,remove_special_chars');

-- Apply to ALL string columns using *
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (101, 'data_transformation_steps', 'normalize_text', 30, 'column_name', '*'),
    (101, 'data_transformation_steps', 'normalize_text', 30, 'operations', 'trim,collapse_whitespace');
-- Normalizes all string columns in-place
```
