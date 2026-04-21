/*
Data Engineering Challenges Demo - T-SQL Script

Purpose:
    Showcases common data engineering challenges that teams face daily,
    and how they can be solved using the Databricks Data Platform Accelerator's
    metadata-driven approach - NO CODE REQUIRED.

Challenges Demonstrated:
    1. Multi-source data integration (joining 3 tables)
    2. Data quality validation with quarantine handling
    3. Deduplication of source records
    4. Business logic calculations and derived columns
    5. PII data masking for compliance
    6. Customer analytics with window functions
    7. Final output column selection

Use this script as input to demonstrate metadata generation capabilities.

Author: Databricks Data Platform Accelerator Demo
*/

-- =====================================================================
-- CHALLENGE 1: Multi-Source Data Integration
-- Business Problem: Data lives in 3 different source systems
-- =====================================================================

-- Source tables: silver.dbo.customers, silver.dbo.orders, silver.dbo.products
-- Join all sources together

WITH combined_data AS (
    SELECT 
        o.*,
        c.customer_name,
        c.email,
        c.phone,
        p.product_name,
        p.category,
        p.cost
    FROM silver.dbo.orders o
    LEFT JOIN silver.dbo.customers c ON o.customer_id = c.customer_id
    LEFT JOIN silver.dbo.products p ON o.product_id = p.product_id
),

-- =====================================================================
-- CHALLENGE 2: Data Quality Validation
-- Business Problem: Bad data causes downstream analytics failures
-- =====================================================================

-- Validate required fields - quarantine records with NULL keys
validated_data AS (
    SELECT *
    FROM combined_data
    WHERE customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND order_total > 0
      AND quantity > 0
),

-- =====================================================================
-- CHALLENGE 3: Deduplication
-- Business Problem: Source systems often have duplicate records
-- =====================================================================

-- Remove duplicates - keep most recent order per customer/product
deduped_data AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id, product_id 
                   ORDER BY order_date DESC
               ) AS rn
        FROM validated_data
    ) ranked
    WHERE rn = 1
),

-- =====================================================================
-- CHALLENGE 4: Business Logic & Derived Columns
-- Business Problem: Analysts need calculated metrics for reporting
-- =====================================================================

enriched_data AS (
    SELECT *,
           -- Profit margin calculation
           ROUND((order_total - (cost * quantity)) / order_total * 100, 2) AS profit_margin,
           
           -- Customer segmentation based on order value
           CASE 
               WHEN order_total >= 5000 THEN 'Premium'
               WHEN order_total >= 1000 THEN 'Standard'
               ELSE 'Basic'
           END AS customer_segment,
           
           -- Days since order
           DATEDIFF(day, order_date, GETDATE()) AS days_since_order
    FROM deduped_data
),

-- =====================================================================
-- CHALLENGE 5: PII Data Masking for Compliance
-- Business Problem: GDPR/CCPA requires masking sensitive data
-- =====================================================================

masked_data AS (
    SELECT *,
           -- Mask email - replace all characters with X
           REPLICATE('X', LEN(email)) AS email_masked,
           
           -- Mask phone - replace all characters with X
           REPLICATE('X', LEN(phone)) AS phone_masked
    FROM enriched_data
),

-- =====================================================================
-- CHALLENGE 6: Customer Analytics with Window Functions
-- Business Problem: Need running totals and rankings per customer
-- =====================================================================

analytics_data AS (
    SELECT *,
           -- Customer total revenue across all orders
           SUM(order_total) OVER (PARTITION BY customer_id) AS customer_total_revenue,
           
           -- Rank orders per customer by date (most recent = 1)
           ROW_NUMBER() OVER (
               PARTITION BY customer_id 
               ORDER BY order_date DESC
           ) AS customer_order_rank,
           
           -- Average order value per customer
           AVG(order_total) OVER (PARTITION BY customer_id) AS customer_avg_order
    FROM masked_data
)

-- =====================================================================
-- CHALLENGE 7: Final Output Selection
-- Business Problem: Only expose necessary columns to consumers
-- =====================================================================

SELECT
    order_id,
    order_date,
    customer_id,
    customer_name,
    email_masked AS email,
    phone_masked AS phone,
    customer_segment,
    product_id,
    product_name,
    category,
    quantity,
    order_total,
    profit_margin,
    days_since_order,
    customer_total_revenue,
    customer_order_rank,
    customer_avg_order
INTO gold.dbo.sales_analytics
FROM analytics_data;

-- Alternative: Use MERGE for incremental loads
/*
MERGE gold.dbo.sales_analytics AS target
USING (
    -- Insert the full query from above here
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET 
        target.order_date = source.order_date,
        target.customer_id = source.customer_id,
        -- ... update all columns
WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, customer_id, ...)
    VALUES (source.order_id, source.order_date, source.customer_id, ...);
*/

PRINT 'Pipeline completed successfully!';
