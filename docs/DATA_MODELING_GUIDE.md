# Data Modeling Guide

> Last updated: 2026-03-29

Use this file when the question is about building dimension tables, fact tables, surrogate keys, SCD Type 2, or star schema patterns.

This guide covers dimension and fact table loading patterns including surrogate keys, SCD Type 2, and denormalization.

Use the thinner canonical docs when the question is really about contract or decision authority:
- [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for accepted metadata values and configuration rules
- [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for metadata authoring patterns and SQL structure

---

## Table of Contents

- [Loading data into a dimension table](#loading-data-into-a-dimension-table)
- [Loading data into a fact table](#loading-data-into-a-fact-table)

---

## Data Modeling

### Loading data into a dimension table

**Keywords:** dimension table, SCD Type 2, SCD2, slowly changing dimension, surrogate key, business key, historical changes, is_current, effective_date, end_date, dimension loading

#### Overview
Dimension tables store descriptive business entities (customers, products, locations) and support analytical queries. The framework provides built-in support for surrogate keys and Slowly Changing Dimension Type 2 (SCD2) to track historical changes.

#### Core Pattern

Use dimensions when you need descriptive entities and optionally history. The runtime decisions that matter most are:
- define the business key in `Primary_Keys`
- create a surrogate key with `create_surrogate_key`
- choose `merge_type='merge'` for current-state dimensions or `merge_type='scd2'` for history
- optionally set source-deletion handling with `column_to_mark_source_data_deletion` and `delete_rows_with_value`

#### Minimal Type 1 Example

```sql
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('MyDataProductTrigger', 3, 19, 'gold', 'dbo.dim_Products', 'product_id', 'batch', 1);

INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (19, 'source_details', 'table_name', 'Silver.dbo.oracle_Products'),
    (19, 'target_details', 'merge_type', 'merge');

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (19, 'data_transformation_steps', 'create_surrogate_key', 1, 'type', 'auto_increment'),
    (19, 'data_transformation_steps', 'create_surrogate_key', 1, 'column_name', 'product_key');
```

#### Minimal SCD2 Example

```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (19, 'source_details', 'table_name', 'Silver.dbo.customers'),
    (19, 'target_details', 'merge_type', 'scd2'),
    (19, 'target_details', 'source_timestamp_column_name', 'modified_date'),
    (19, 'target_details', 'column_to_mark_source_data_deletion', 'is_deleted'),
    (19, 'target_details', 'delete_rows_with_value', 'true');

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (19, 'data_transformation_steps', 'create_surrogate_key', 1, 'type', 'auto_increment'),
    (19, 'data_transformation_steps', 'create_surrogate_key', 1, 'column_name', 'customer_key');
```

#### Runtime Behavior

- `create_surrogate_key` currently uses `auto_increment` and also creates an unknown-member row with key `-1`.
- `merge_type='scd2'` adds `scd_active`, `scd_start_date`, and `scd_end_date` and writes a new row when the tracked record changes.
- source deletions are hard deletes for non-SCD2 dimensions and soft-retire actions for SCD2 dimensions.
- multiple watermark columns are still a special-case pattern; keep those details in [METADATA_REFERENCE.md](METADATA_REFERENCE.md) instead of duplicating them here.

#### Querying SCD2 Dimensions

```sql
SELECT *
FROM gold.dbo.dim_Customer
WHERE scd_active = 1;

SELECT *
FROM gold.dbo.dim_Customer
WHERE scd_start_date <= '2024-06-01'
  AND (scd_end_date > '2024-06-01' OR scd_end_date IS NULL);
```

#### Reference Pointers

- Use [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for the full `target_details` and `create_surrogate_key` contract.
- Use [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) for authoring patterns.
- Use `merge_type='scd2'` only when downstream users actually need historical versions; otherwise prefer the simpler current-state pattern.

#### Best Practices
1. **Use SCD2 for tracking changes**: Enable for dimensions where history matters (customers, products, employees)
2. **Choose appropriate comparison column**: Use last_modified_date or similar timestamp
3. **Consistent naming**: Use standard surrogate key names across all dimensions
4. **Monitor dimension growth**: SCD2 increases table size over time
5. **Index properly**: Index on business keys and scd_active for query performance
6. **Unknown member**: Framework automatically creates -1 surrogate key row for missing dimensions

### Loading data into a fact table

**Keywords:** fact table, surrogate key lookup, dimension join, star schema, merge, append, overwrite, business events, sales, orders, transactions, fact loading, snowflake dimension

#### Overview
Fact tables store measurable business events (sales, orders, transactions) and reference dimension tables via surrogate keys. The framework automatically joins dimensions to insert surrogate keys and supports various merge strategies for different data loading patterns.

#### Key Capabilities
- **Automatic surrogate key lookup** from dimension tables
- **Multiple dimension joins** in single configuration
- **Denormalization support** bringing dimension columns into fact
- **Custom surrogate key column naming** for flexibility
- **Star *and* snowflake ready** by pointing `dimension_table_name` at any conformed dimension (primary, degenerate, helper) and chaining steps to walk multi-hop hierarchies
- **Flexible merge strategies** for different loading patterns
- **Source deletion tracking** for data quality

#### Use Cases
- **Sales facts** linking products, customers, time, and promotions
- **Order facts** with customer, product, and shipping dimensions
- **Transaction facts** tracking financial events
- **Event facts** logging user interactions with dimensions

#### Configuration Steps
1.  Update/Enter the appropriate values in the SQL statements below
2.  Execute the SQL in the Metadata Warehouse
3.  Execute the Trigger Step Orchestrator pipeline with the inputted `Trigger_Name` value

#### Basic Fact Table Load
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('SalesFacts', '4', '20', 'gold', 'dbo.fact_Sales', 'sale_id', 'batch','1')

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    ('20', 'source_details', 'table_name', 'Silver.dbo.sales_transactions')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Insert surrogate key from product dimension
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_sk_fk')
```

**Join Logic Aliases:**
In the `dimension_table_join_logic` SQL condition:
- **`a`** refers to the current dataset being transformed
- **`b`** refers to the **dimension table** specified in `dimension_table_name`

**Result:**
- Framework joins to `dim_Product` dimension
- Looks for surrogate key column (default name: `Key_SK`)
- Adds column to fact table (default name: `SK_Gold_dbo_dim_Product`)
- Inserts surrogate key value from matching dimension row

#### Multiple Dimension Lookups
```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('SalesFacts', '4', '20', 'gold', 'dbo.fact_Sales', 'sale_id', 'batch','1')

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    ('20', 'source_details', 'table_name', 'Silver.dbo.sales_transactions')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Product dimension lookup
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_sk_fk'),
    
    -- Customer dimension lookup
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_name', 'Gold.dbo.dim_Customer'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_join_logic', 'a.customer_id = b.customer_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_key_column_name', 'customer_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_key_output_column_name', 'customer_sk_fk'),
    
    -- Time dimension lookup
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_name', 'Gold.dbo.dim_Date'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_join_logic', 'a.sale_date = b.date_key'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_key_column_name', 'date_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_key_output_column_name', 'date_sk_fk'),
    
    -- Store dimension lookup
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_name', 'Gold.dbo.dim_Store'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_join_logic', 'a.store_id = b.store_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_key_column_name', 'store_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_key_output_column_name', 'store_sk_fk')
```

**Execution Order:**
Surrogate keys are inserted based on `Configuration_Name_Instance_Number` (1, 2, 3, 4...).

#### Custom Surrogate Key Column Names

**Scenario 1: Dimension has non-standard surrogate key column**

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    -- Dimension has 'product_key' instead of default 'Key_SK'
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_key'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_key_fk')
```

**Scenario 2: Custom column name in fact table**

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_key'),
    -- dimension_key_output_column_name is required: specifies the column name in the fact table
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_key_fk')
```

#### Denormalization - Bring Dimension Columns to Fact

Add commonly used dimension attributes directly to fact table for query performance:

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_sk_fk'),
    -- Denormalize commonly queried columns into fact
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_columns_to_add_to_fact', 'product_name, category, subcategory, brand')
```

**Result:**
Fact table will have both:
- Surrogate key: `product_sk` (as specified by `dimension_key_output_column_name`)
- Denormalized columns: `product_name`, `category`, `subcategory`, `brand`

**When to denormalize:**
- ✅ Frequently filtered/grouped columns (category, status, type)
- ✅ Slowly changing attributes (product name, customer tier)
- ❌ Large text fields (descriptions)
- ❌ Rapidly changing attributes (defeats purpose)

#### Handling SCD2 Dimensions in Facts

When joining to SCD2 dimensions, ensure you get the current version:

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Customer'),
    -- Join logic includes scd_active filter for SCD2 dimension
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 
     'a.customer_id = b.customer_id AND b.scd_active = 1')
```

**For point-in-time accuracy:**
```sql
-- Join based on transaction date falling within dimension's active period
('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 
 'a.customer_id = b.customer_id AND a.transaction_date >= b.scd_start_date AND (a.transaction_date < b.scd_end_date OR b.scd_end_date IS NULL)')
```

#### Merge Type Options for Facts

```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    ('20', 'target_details', 'merge_type', 'append')
```

> **All merge types are available for fact tables.** See the [target_details configuration reference](#target_details) for the full list of `merge_type` values, descriptions, and when to use each one.

**Most Common: append**
- Facts are typically immutable (transactions don't change)
- New data is always inserted, never updated
- Best performance for large fact tables

**When to use merge:**
- Late-arriving dimensions (dimension keys not available at initial load)
- Fact corrections/adjustments
- Slowly changing facts (order status updates)

#### Source Deletion Tracking

Mark or remove deleted transactions from source:

```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    ('20', 'source_details', 'table_name', 'Silver.dbo.sales_transactions'),
    
    -- Column indicating record deletion in source
    ('20', 'target_details', 'column_to_mark_source_data_deletion', 'is_cancelled'),
    
    -- Value indicating deletion (e.g., true, 1, 'DELETED')
    -- Records with this value will be DELETED from fact table
    ('20', 'target_details', 'delete_rows_with_value', 'true')
```

**Alternatives to hard delete:**
```sql
-- Instead of deleting, keep cancelled transactions with flag
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Filter out cancelled transactions from fact
    ('20', 'data_transformation_steps', 'filter_data', '1', 'filter_logic', 'is_cancelled = false')
    
-- Or: Keep all records, flag for reporting
-- (No filter, let reporting layer handle exclusion)
```

#### Complete Fact Table Example

```sql
-- Orchestration Table
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES 
    ('SalesDataMart', '4', '20', 'gold', 'dbo.fact_Sales', 'sale_id, line_item_id', 'batch','1')

--Primary Config Table
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    ('20', 'source_details', 'table_name', 'Silver.dbo.sales_transactions'),
    
    -- Use append for immutable facts
    ('20', 'target_details', 'merge_type', 'append'),
    
    -- Handle cancellations
    ('20', 'target_details', 'column_to_mark_source_data_deletion', 'is_cancelled'),
    ('20', 'target_details', 'delete_rows_with_value', 'true')

INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Product dimension with denormalization
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_name', 'Gold.dbo.dim_Product'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_table_key_column_name', 'product_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_columns_to_add_to_fact', 'product_name, category'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '1', 'dimension_key_output_column_name', 'product_sk_fk'),
    
    -- Customer dimension (SCD2) with point-in-time join
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_name', 'Gold.dbo.dim_Customer'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_join_logic', 
     'a.customer_id = b.customer_id AND b.scd_active = 1'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_table_key_column_name', 'customer_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_columns_to_add_to_fact', 'customer_name, customer_tier'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '2', 'dimension_key_output_column_name', 'customer_sk_fk'),
    
    -- Date dimension
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_name', 'Gold.dbo.dim_Date'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_join_logic', 'CAST(a.sale_date AS DATE) = b.date_key'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_table_key_column_name', 'date_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '3', 'dimension_key_output_column_name', 'date_sk_fk'),
    
    -- Store dimension
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_name', 'Gold.dbo.dim_Store'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_join_logic', 'a.store_id = b.store_id'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_columns_to_add_to_fact', 'store_name, region'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_table_key_column_name', 'store_sk'),
    ('20', 'data_transformation_steps', 'attach_dimension_surrogate_key', '4', 'dimension_key_output_column_name', 'store_sk_fk')
```

**Resulting Fact Table Schema:**
```sql
-- Fact table will have:
sale_id BIGINT,                 -- Business key
line_item_id INT,               -- Business key
product_sk BIGINT,              -- Surrogate key from dim_Product
product_name STRING,            -- Denormalized from dim_Product
category STRING,                -- Denormalized from dim_Product
customer_sk BIGINT,             -- Surrogate key from dim_Customer
customer_name STRING,           -- Denormalized from dim_Customer
customer_tier STRING,           -- Denormalized from dim_Customer
date_sk INT,                    -- Surrogate key from dim_Date
store_sk BIGINT,                -- Surrogate key from dim_Store
store_name STRING,              -- Denormalized from dim_Store
region STRING,                  -- Denormalized from dim_Store
quantity INT,                   -- Measure
unit_price DECIMAL(10,2),       -- Measure
total_amount DECIMAL(10,2),     -- Measure
-- ... other measures ...
delta__modified_datetime TIMESTAMP  -- Framework metadata
```

#### Handling Missing Dimensions (Unknown Members)

When fact references dimension that doesn't exist:

**Option 1: Use -1 Unknown Member (Recommended)**
```sql
-- Framework automatically creates -1 row in dimensions
-- Missing dimension lookups return -1
-- No special configuration needed
```

**Option 2: Filter Out Missing Dimensions**
```sql
-- After surrogate key lookup, filter out rows with -1
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_transformation_steps', 'filter_data', '5', 'filter_logic', 'product_sk != -1 AND customer_sk != -1')
```

**Option 3: Quarantine for Review**
```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    ('20', 'data_quality', 'validate_condition', '1', 'condition', 'product_sk != -1 AND customer_sk != -1'),
    ('20', 'data_quality', 'validate_condition', '1', 'if_not_compliant', 'quarantine'),
    ('20', 'data_quality', 'validate_condition', '1', 'message', 'All dimension keys must be resolved')
```

#### Best Practices
1. **Load dimensions before facts**: Ensure dimensions exist before fact loading (use Order_Of_Operations)
2. **Use append for immutable facts**: Most facts should use append merge type
3. **Denormalize strategically**: Balance query performance vs storage/maintenance
4. **Handle SCD2 correctly**: Use scd_active or point-in-time joins
5. **Index surrogate keys**: Create indexes on surrogate key columns for join performance
6. **Monitor unknown members**: Track -1 surrogate key usage to identify data quality issues
7. **Grain consistency**: Ensure fact table grain matches dimension relationships

---

