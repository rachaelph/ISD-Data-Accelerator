"""
Data Engineering Challenges Demo - PySpark Script

Purpose:
    Showcases common data engineering challenges that teams face daily,
    and how they can be solved using the Fabric Data Platform Accelerator's
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

Author: Fabric Data Platform Accelerator Demo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, when, lit, current_timestamp,
    datediff, round, concat_ws, regexp_replace, row_number
)
from pyspark.sql.window import Window

# =====================================================================
# CHALLENGE 1: Multi-Source Data Integration
# Business Problem: Data lives in 3 different source systems
# =====================================================================

# Source tables
df_customers = spark.read.format("delta").table("silver.dbo.customers")
df_orders = spark.read.format("delta").table("silver.dbo.orders") 
df_products = spark.read.format("delta").table("silver.dbo.products")

# Join all sources together
df_combined = df_orders.alias("o") \
    .join(df_customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
    .join(df_products.alias("p"), col("o.product_id") == col("p.product_id"), "left")

# =====================================================================
# CHALLENGE 2: Data Quality Validation
# Business Problem: Bad data causes downstream analytics failures
# =====================================================================

# Validate required fields - quarantine records with NULL keys
df_validated = df_combined \
    .filter(col("o.customer_id").isNotNull()) \
    .filter(col("o.product_id").isNotNull()) \
    .filter(col("o.order_total") > 0) \
    .filter(col("o.quantity") > 0)

# =====================================================================
# CHALLENGE 3: Deduplication
# Business Problem: Source systems often have duplicate records
# =====================================================================

# Remove duplicates - keep most recent order per customer/product
df_deduped = df_validated \
    .dropDuplicates(["customer_id", "product_id"])

# =====================================================================
# CHALLENGE 4: Business Logic & Derived Columns
# Business Problem: Analysts need calculated metrics for reporting
# =====================================================================

df_enriched = df_deduped \
    .withColumn("profit_margin", 
                round((col("o.order_total") - col("p.cost") * col("o.quantity")) / 
                      col("o.order_total") * 100, 2)) \
    .withColumn("customer_segment",
                when(col("o.order_total") >= 5000, "Premium")
                .when(col("o.order_total") >= 1000, "Standard")
                .otherwise("Basic")) \
    .withColumn("days_since_order",
                datediff(current_timestamp(), col("o.order_date")))

# =====================================================================
# CHALLENGE 5: PII Data Masking for Compliance
# Business Problem: GDPR/CCPA requires masking sensitive data
# =====================================================================

df_masked = df_enriched \
    .withColumn("email_masked", 
                regexp_replace(col("c.email"), ".", "X")) \
    .withColumn("phone_masked",
                regexp_replace(col("c.phone"), ".", "X"))

# =====================================================================
# CHALLENGE 6: Customer Analytics with Window Functions
# Business Problem: Need running totals and rankings per customer
# =====================================================================

customer_window = Window.partitionBy("c.customer_id").orderBy(col("o.order_date").desc())

df_analytics = df_masked \
    .withColumn("customer_total_revenue", 
                sum("o.order_total").over(Window.partitionBy("c.customer_id"))) \
    .withColumn("customer_order_rank",
                row_number().over(customer_window)) \
    .withColumn("customer_avg_order",
                avg("o.order_total").over(Window.partitionBy("c.customer_id")))

# =====================================================================
# CHALLENGE 7: Final Output Selection
# Business Problem: Only expose necessary columns to consumers
# =====================================================================

df_final = df_analytics.select(
    col("o.order_id"),
    col("o.order_date"),
    col("c.customer_id"),
    col("c.customer_name"),
    col("email_masked").alias("email"),
    col("phone_masked").alias("phone"),
    col("customer_segment"),
    col("p.product_id"),
    col("p.product_name"),
    col("p.category"),
    col("o.quantity"),
    col("o.order_total"),
    col("profit_margin"),
    col("days_since_order"),
    col("customer_total_revenue"),
    col("customer_order_rank"),
    col("customer_avg_order")
)

# Write to Gold layer
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold.dbo.sales_analytics")

print("Pipeline completed successfully!")
