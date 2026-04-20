-- =====================================================================
-- Metadata Configuration: Sales Analytics (Silver -> Gold)
-- Trigger Name: sales_analytics
-- Generated: 2026-04-20
-- Source: scripts/Demo_Data_Engineering_Challenges.py
-- =====================================================================
--
-- Purpose:
--   Demo pipeline converted from a 7-challenge PySpark script. Joins three
--   silver Delta tables (orders, customers, products), validates data,
--   deduplicates, computes business metrics, masks PII, and writes to
--   gold.dbo.sales_analytics.
--
-- Table_ID Range:
--   Gold: 2001 (gap of >100 from any silver IDs: 1001-1003 in daily_load)
--
-- Source Operation Coverage (scripts/Demo_Data_Engineering_Challenges.py):
--   CHALLENGE 1 (multi-source join)       -> join_data x2
--   CHALLENGE 2 (DQ validation)           -> validate_condition x4
--   CHALLENGE 3 (dedup)                   -> drop_duplicates
--   CHALLENGE 4 (derived columns + CASE)  -> derived_column x2, conditional_column
--   CHALLENGE 5 (PII masking)             -> mask_sensitive_data (consolidated)
--   CHALLENGE 6 (window functions)        -> add_window_function x3
--   CHALLENGE 7 (final output)            -> select_columns
--
-- Notes:
--   - Delta-to-Delta: source_details uses only table_name (3-part naming);
--     no source/schema_name/query per Golden Rule #20.
--   - Processing_Method = batch (required for Delta-to-Delta).
--   - merge_type = overwrite (full reload each run matches the script's
--     .mode("overwrite") on gold.dbo.sales_analytics).
--   - Instance numbers are unique across all data_transformation_steps
--     within Table_ID 2001 (validator rule).
-- =====================================================================

-- =====================================================================
-- STEP 1: DELETE existing records for this trigger
-- =====================================================================
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'sales_analytics');

DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'sales_analytics');

DELETE FROM Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'sales_analytics';

-- =====================================================================
-- STEP 2: Orchestration Metadata
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Orchestration (Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
VALUES
('sales_analytics', 1, 2001, 'gold', 'dbo.sales_analytics', 'customer_id,product_id', 'batch', 1);

-- =====================================================================
-- STEP 3: Primary Configuration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(2001, 'source_details', 'table_name', 'silver.dbo.orders'),
(2001, 'target_details', 'merge_type', 'overwrite'),
(2001, 'watermark_details', 'data_type', 'datetime');

-- =====================================================================
-- STEP 4: Advanced Configuration - Data Quality + Data Transformation Steps
-- (single INSERT per table; instance numbers unique within category)
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- ---- Transformation Step 1: LEFT JOIN silver.dbo.customers (Challenge 1) ----
(2001, 'data_transformation_steps', 'join_data', 1, 'right_table_name', 'silver.dbo.customers'),
(2001, 'data_transformation_steps', 'join_data', 1, 'join_type', 'left'),
(2001, 'data_transformation_steps', 'join_data', 1, 'left_columns', 'customer_id'),
(2001, 'data_transformation_steps', 'join_data', 1, 'right_columns', 'customer_id'),
(2001, 'data_transformation_steps', 'join_data', 1, 'join_condition', 'a.customer_id = b.customer_id'),
-- ---- Transformation Step 2: LEFT JOIN silver.dbo.products (Challenge 1) ----
(2001, 'data_transformation_steps', 'join_data', 2, 'right_table_name', 'silver.dbo.products'),
(2001, 'data_transformation_steps', 'join_data', 2, 'join_type', 'left'),
(2001, 'data_transformation_steps', 'join_data', 2, 'left_columns', 'product_id'),
(2001, 'data_transformation_steps', 'join_data', 2, 'right_columns', 'product_id'),
(2001, 'data_transformation_steps', 'join_data', 2, 'join_condition', 'a.product_id = b.product_id'),
-- ---- Transformation Step 3: drop_duplicates (Challenge 3) ----
(2001, 'data_transformation_steps', 'drop_duplicates', 3, 'column_name', 'customer_id,product_id'),
-- ---- Transformation Step 4: profit_margin (Challenge 4) ----
(2001, 'data_transformation_steps', 'derived_column', 4, 'column_name', 'profit_margin'),
(2001, 'data_transformation_steps', 'derived_column', 4, 'expression', 'round((order_total - cost * quantity) / order_total * 100, 2)'),
-- ---- Transformation Step 5: customer_segment CASE WHEN (Challenge 4) ----
(2001, 'data_transformation_steps', 'conditional_column', 5, 'column_name', 'customer_segment'),
(2001, 'data_transformation_steps', 'conditional_column', 5, 'conditions', 'order_total >= 5000; order_total >= 1000'),
(2001, 'data_transformation_steps', 'conditional_column', 5, 'values', 'Premium; Standard'),
(2001, 'data_transformation_steps', 'conditional_column', 5, 'values_delimiter', ';'),
(2001, 'data_transformation_steps', 'conditional_column', 5, 'default_value', 'Basic'),
-- ---- Transformation Step 6: days_since_order (Challenge 4) ----
(2001, 'data_transformation_steps', 'derived_column', 6, 'column_name', 'days_since_order'),
(2001, 'data_transformation_steps', 'derived_column', 6, 'expression', 'datediff(current_timestamp(), order_date)'),
-- ---- Transformation Step 7: mask email + phone with X (Challenge 5) ----
(2001, 'data_transformation_steps', 'mask_sensitive_data', 7, 'column_name', 'email,phone'),
(2001, 'data_transformation_steps', 'mask_sensitive_data', 7, 'upper_char', 'X'),
(2001, 'data_transformation_steps', 'mask_sensitive_data', 7, 'lower_char', 'X'),
(2001, 'data_transformation_steps', 'mask_sensitive_data', 7, 'digit_char', 'X'),
(2001, 'data_transformation_steps', 'mask_sensitive_data', 7, 'other_char', 'X'),
-- ---- Transformation Step 8: customer_total_revenue = SUM(order_total) OVER(customer_id) (Challenge 6) ----
(2001, 'data_transformation_steps', 'add_window_function', 8, 'column_name', 'order_total'),
(2001, 'data_transformation_steps', 'add_window_function', 8, 'output_column_name', 'customer_total_revenue'),
(2001, 'data_transformation_steps', 'add_window_function', 8, 'window_function', 'sum'),
(2001, 'data_transformation_steps', 'add_window_function', 8, 'partition_by', 'customer_id'),
-- ---- Transformation Step 9: customer_order_rank = row_number() OVER(customer_id ORDER BY order_date DESC) (Challenge 6) ----
(2001, 'data_transformation_steps', 'add_window_function', 9, 'output_column_name', 'customer_order_rank'),
(2001, 'data_transformation_steps', 'add_window_function', 9, 'window_function', 'row_number'),
(2001, 'data_transformation_steps', 'add_window_function', 9, 'partition_by', 'customer_id'),
(2001, 'data_transformation_steps', 'add_window_function', 9, 'order_by', 'order_date'),
(2001, 'data_transformation_steps', 'add_window_function', 9, 'order_direction', 'desc'),
-- ---- Transformation Step 10: customer_avg_order = AVG(order_total) OVER(customer_id) (Challenge 6) ----
(2001, 'data_transformation_steps', 'add_window_function', 10, 'column_name', 'order_total'),
(2001, 'data_transformation_steps', 'add_window_function', 10, 'output_column_name', 'customer_avg_order'),
(2001, 'data_transformation_steps', 'add_window_function', 10, 'window_function', 'avg'),
(2001, 'data_transformation_steps', 'add_window_function', 10, 'partition_by', 'customer_id'),
-- ---- Transformation Step 11: final column projection (Challenge 7) ----
(2001, 'data_transformation_steps', 'select_columns', 11, 'column_name', 'order_id,order_date,customer_id,customer_name,email,phone,customer_segment,product_id,product_name,category,quantity,order_total,profit_margin,days_since_order,customer_total_revenue,customer_order_rank,customer_avg_order'),
-- ---- Data Quality (Challenge 2: validate required fields, quarantine bad records) ----
(2001, 'data_quality', 'validate_condition', 1, 'condition', 'customer_id IS NOT NULL'),
(2001, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
(2001, 'data_quality', 'validate_condition', 1, 'message', 'customer_id must not be null'),
(2001, 'data_quality', 'validate_condition', 2, 'condition', 'product_id IS NOT NULL'),
(2001, 'data_quality', 'validate_condition', 2, 'if_not_compliant', 'quarantine'),
(2001, 'data_quality', 'validate_condition', 2, 'message', 'product_id must not be null'),
(2001, 'data_quality', 'validate_condition', 3, 'condition', 'order_total > 0'),
(2001, 'data_quality', 'validate_condition', 3, 'if_not_compliant', 'quarantine'),
(2001, 'data_quality', 'validate_condition', 3, 'message', 'order_total must be positive'),
(2001, 'data_quality', 'validate_condition', 4, 'condition', 'quantity > 0'),
(2001, 'data_quality', 'validate_condition', 4, 'if_not_compliant', 'quarantine'),
(2001, 'data_quality', 'validate_condition', 4, 'message', 'quantity must be positive');