"""
Tests for verify_conversion.py

Run: python -m pytest .github/skills/conversion-verification/test_verify_conversion.py -v
"""

import json
import sys
from pathlib import Path

import pytest

# Ensure the module is importable
sys.path.insert(0, str(Path(__file__).parent))
from verify_conversion import (
    normalize_table_name,
    extract_table_part,
    tables_match,
    is_system_table,
    detect_language,
    strip_comments,
    extract_source_tables,
    detect_complexity_signals,
    detect_operation_types,
    parse_metadata_sql,
    check_table_coverage,
    check_complexity_coverage,
    check_operation_coverage,
    run,
    TableRef,
    ComplexitySignal,
    MetadataInfo,
)


# =============================================================================
# TABLE NAME NORMALIZATION
# =============================================================================


class TestNormalization:
    def test_bracket_quoting(self):
        assert normalize_table_name("[dbo].[Orders]") == "dbo.orders"

    def test_double_quote_quoting(self):
        assert normalize_table_name('"PUBLIC"."ORDERS"') == "public.orders"

    def test_backtick_quoting(self):
        assert normalize_table_name("`schema`.`table`") == "schema.table"

    def test_bare_identifier(self):
        assert normalize_table_name("orders") == "orders"

    def test_three_part_name(self):
        assert normalize_table_name("[MyDB].[dbo].[Orders]") == "mydb.dbo.orders"

    def test_mixed_quoting(self):
        assert normalize_table_name('[dbo]."Orders"') == "dbo.orders"

    def test_extract_table_part_single(self):
        assert extract_table_part("orders") == "orders"

    def test_extract_table_part_two(self):
        assert extract_table_part("dbo.orders") == "orders"

    def test_extract_table_part_three(self):
        assert extract_table_part("mydb.dbo.orders") == "orders"

    def test_tables_match_exact(self):
        assert tables_match("dbo.orders", "dbo.orders")

    def test_tables_match_different_schema(self):
        assert tables_match("dbo.orders", "silver.dbo.orders")

    def test_tables_match_no_match(self):
        assert not tables_match("dbo.orders", "dbo.customers")

    def test_system_table(self):
        assert is_system_table("sys.objects")
        assert is_system_table("information_schema.columns")
        assert not is_system_table("dbo.orders")


# =============================================================================
# LANGUAGE DETECTION
# =============================================================================


class TestLanguageDetection:
    def test_sql_by_extension(self):
        assert detect_language("anything", "test.sql") == "sql"

    def test_python_by_extension(self):
        assert detect_language("anything", "test.py") == "python"

    def test_sql_by_content(self):
        assert detect_language("SELECT * FROM orders WHERE id = 1") == "sql"

    def test_python_by_content(self):
        assert detect_language("import pyspark\ndf = spark.read.table('x')") == "python"

    def test_mixed_content(self):
        code = "import pyspark\ndf = spark.sql('SELECT * FROM orders')"
        assert detect_language(code) == "mixed"


# =============================================================================
# COMMENT STRIPPING
# =============================================================================


class TestCommentStripping:
    def test_sql_line_comment(self):
        code = "-- SELECT FROM hidden_table\nSELECT * FROM real_table"
        cleaned = strip_comments(code, "sql")
        assert "hidden_table" not in cleaned
        assert "real_table" in cleaned

    def test_sql_block_comment(self):
        code = "/* SELECT FROM hidden */ SELECT * FROM real_table"
        cleaned = strip_comments(code, "sql")
        assert "hidden" not in cleaned
        assert "real_table" in cleaned

    def test_python_comment(self):
        code = "# df = spark.read.table('hidden')\ndf = spark.read.table('real')"
        cleaned = strip_comments(code, "python")
        assert "hidden" not in cleaned
        assert "real" in cleaned

    def test_python_docstring(self):
        code = '"""This references spark.read.table("hidden")"""\ndf = spark.read.table("real")'
        cleaned = strip_comments(code, "python")
        assert "hidden" not in cleaned
        assert "real" in cleaned


# =============================================================================
# TABLE EXTRACTION — SQL
# =============================================================================


class TestSQLTableExtraction:
    def test_simple_from(self):
        code = "SELECT * FROM dbo.Orders"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names

    def test_join(self):
        code = "SELECT * FROM dbo.Orders o LEFT JOIN dbo.Customers c ON o.id = c.id"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names
        assert "dbo.customers" in names

    def test_bracket_quoting(self):
        code = "SELECT * FROM [dbo].[Orders]"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names

    def test_double_quote_quoting(self):
        code = 'SELECT * FROM "public"."orders"'
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "public.orders" in names

    def test_backtick_quoting(self):
        code = "SELECT * FROM `mydb`.`orders`"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "mydb.orders" in names

    def test_multiple_joins(self):
        code = """
        SELECT a.*, b.name, c.total
        FROM dbo.Orders a
        INNER JOIN dbo.Customers b ON a.cust_id = b.id
        LEFT JOIN dbo.OrderTotals c ON a.order_id = c.order_id
        """
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names
        assert "dbo.customers" in names
        assert "dbo.ordertotals" in names

    def test_cte_excluded(self):
        code = """
        WITH order_totals AS (
            SELECT order_id, SUM(amount) as total FROM dbo.LineItems GROUP BY order_id
        )
        SELECT * FROM order_totals o JOIN dbo.Orders ord ON o.order_id = ord.id
        """
        tables = extract_source_tables(code, "sql")
        ctes = [t for t in tables if t.is_cte]
        non_ctes = [t for t in tables if not t.is_cte]
        cte_names = {t.normalized for t in ctes}
        non_cte_names = {t.normalized for t in non_ctes}
        assert "order_totals" in cte_names
        assert "dbo.lineitems" in non_cte_names
        assert "dbo.orders" in non_cte_names

    def test_temp_table_detected(self):
        code = "SELECT * INTO #staging FROM dbo.Orders\nSELECT * FROM #staging"
        tables = extract_source_tables(code, "sql")
        temps = [t for t in tables if t.is_temp]
        assert any("#staging" in t.normalized for t in temps)

    def test_merge_into(self):
        code = "MERGE INTO dbo.Target t USING dbo.Source s ON t.id = s.id"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.target" in names

    def test_update(self):
        code = "UPDATE dbo.Orders SET status = 'shipped'"
        tables = extract_source_tables(code, "sql")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names

    def test_commented_out_not_extracted(self):
        code = "-- SELECT * FROM hidden_table\nSELECT * FROM real_table"
        cleaned = strip_comments(code, "sql")
        tables = extract_source_tables(cleaned, "sql")
        names = {t.normalized for t in tables}
        assert "hidden_table" not in names
        assert "real_table" in names


# =============================================================================
# TABLE EXTRACTION — PYSPARK
# =============================================================================


class TestPySparkTableExtraction:
    def test_read_table(self):
        code = 'df = spark.read.table("my_catalog.schema.orders")'
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "my_catalog.schema.orders" in names

    def test_spark_table(self):
        code = 'df = spark.table("orders")'
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "orders" in names

    def test_read_parquet(self):
        code = 'df = spark.read.parquet("abfss://container@storage/path/orders")'
        tables = extract_source_tables(code, "python")
        assert len(tables) >= 1
        assert tables[0].context == "read.file"
        assert tables[0].confidence == "medium"

    def test_delta_table_forname(self):
        code = 'dt = DeltaTable.forName(spark, "silver.dbo.orders")'
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "silver.dbo.orders" in names

    def test_save_as_table(self):
        code = 'df.write.mode("overwrite").saveAsTable("gold.dbo.summary")'
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "gold.dbo.summary" in names

    def test_jdbc_dbtable(self):
        code = 'df = spark.read.format("jdbc").option("dbtable", "dbo.Orders").load()'
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "dbo.orders" in names

    def test_temp_view_detected(self):
        code = 'df.createOrReplaceTempView("staging_orders")'
        tables = extract_source_tables(code, "python")
        temps = [t for t in tables if t.is_temp]
        assert any("staging_orders" in t.normalized for t in temps)

    def test_spark_sql_embedded(self):
        code = """df = spark.sql("SELECT * FROM orders JOIN customers ON orders.id = customers.order_id")"""
        tables = extract_source_tables(code, "python")
        names = {t.normalized for t in tables}
        assert "orders" in names
        assert "customers" in names


# =============================================================================
# COMPLEXITY SIGNALS
# =============================================================================


class TestComplexitySignals:
    def test_cursor(self):
        code = "DECLARE order_cur CURSOR FOR SELECT * FROM orders"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "cursor" in types

    def test_while_loop(self):
        code = "WHILE @i < 100 BEGIN\n  SET @i = @i + 1\nEND"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "loop" in types

    def test_recursive_cte(self):
        code = "WITH RECURSIVE emp_tree AS (SELECT * FROM employees)"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "recursion" in types

    def test_dynamic_sql(self):
        code = "EXEC('SELECT * FROM ' + @tableName)"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "dynamic_sql" in types

    def test_sp_executesql(self):
        code = "EXEC sp_executesql @sql, @params"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "dynamic_sql" in types

    def test_udf_decorator(self):
        code = "@udf\ndef my_func(x):\n  return x * 2"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "udf" in types

    def test_udf_function(self):
        code = "my_udf = udf(lambda x: x.upper(), StringType())"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "udf" in types

    def test_rdd_operations(self):
        code = "result = df.rdd.mapPartitions(process_batch)"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "rdd" in types

    def test_api_call(self):
        code = "response = requests.get('https://api.example.com/data')"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "api_call" in types

    def test_ml_inference(self):
        code = "predictions = model.predict(features)"
        signals = detect_complexity_signals(code)
        types = {s.signal_type for s in signals}
        assert "ml_inference" in types

    def test_no_signals_in_simple_sql(self):
        code = "SELECT a.*, b.name FROM orders a JOIN customers b ON a.cust_id = b.id"
        signals = detect_complexity_signals(code)
        assert len(signals) == 0


# =============================================================================
# OPERATION TYPE DETECTION
# =============================================================================


class TestOperationTypes:
    def test_join_sql(self):
        ops = detect_operation_types("SELECT * FROM a JOIN b ON a.id = b.id")
        assert ops["join"] is True

    def test_join_pyspark(self):
        ops = detect_operation_types('df1.join(df2, "key")')
        assert ops["join"] is True

    def test_aggregate_sql(self):
        ops = detect_operation_types("SELECT region, SUM(amount) FROM sales GROUP BY region")
        assert ops["aggregate"] is True

    def test_aggregate_pyspark(self):
        ops = detect_operation_types('df.groupBy("region").agg(F.sum("amount"))')
        assert ops["aggregate"] is True

    def test_conditional_sql(self):
        ops = detect_operation_types("SELECT CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END")
        assert ops["conditional"] is True

    def test_conditional_pyspark(self):
        ops = detect_operation_types('df.withColumn("label", F.when(F.col("x") > 0, "pos"))')
        assert ops["conditional"] is True

    def test_window_function_sql(self):
        ops = detect_operation_types("SELECT ROW_NUMBER() OVER(PARTITION BY dept ORDER BY salary DESC)")
        assert ops["window_function"] is True

    def test_window_function_pyspark(self):
        ops = detect_operation_types("w = Window.partitionBy('dept').orderBy('salary')")
        assert ops["window_function"] is True

    def test_union_sql(self):
        ops = detect_operation_types("SELECT * FROM a UNION ALL SELECT * FROM b")
        assert ops["union"] is True

    def test_pivot_sql(self):
        ops = detect_operation_types("SELECT * FROM sales PIVOT (SUM(amount) FOR region IN ('East', 'West'))")
        assert ops["pivot"] is True

    def test_unpivot_pandas(self):
        ops = detect_operation_types("df.melt(id_vars=['id'], value_vars=['a', 'b'])")
        assert ops["unpivot"] is True

    def test_null_handling_sql(self):
        ops = detect_operation_types("SELECT COALESCE(name, 'Unknown') FROM customers")
        assert ops["null_handling"] is True

    def test_type_cast_sql(self):
        ops = detect_operation_types("SELECT CAST(amount AS DECIMAL(10,2)) FROM orders")
        assert ops["type_cast"] is True

    def test_dedup_sql(self):
        ops = detect_operation_types("SELECT DISTINCT customer_id FROM orders")
        assert ops["dedup"] is True

    def test_row_hash_sql(self):
        ops = detect_operation_types("SELECT HASHBYTES('SHA2_256', col1 + col2) FROM t")
        assert ops["row_hash"] is True

    def test_explode_pyspark(self):
        ops = detect_operation_types('df.select(F.explode("tags"))')
        assert ops["explode"] is True

    def test_datetime_sql(self):
        ops = detect_operation_types("SELECT DATEADD(day, 7, order_date) FROM orders")
        assert ops["datetime"] is True

    def test_filter_sql(self):
        ops = detect_operation_types("SELECT * FROM orders WHERE status = 'active'")
        assert ops["filter"] is True

    def test_derived_column_pyspark(self):
        ops = detect_operation_types('df.withColumn("total", F.col("qty") * F.col("price"))')
        assert ops["derived_column"] is True

    def test_string_functions_sql(self):
        ops = detect_operation_types("SELECT UPPER(name), TRIM(address) FROM customers")
        assert ops["string_functions"] is True

    def test_no_false_positives_on_simple(self):
        ops = detect_operation_types("SELECT 1")
        # Most should be False
        true_count = sum(1 for v in ops.values() if v)
        assert true_count == 0


# =============================================================================
# METADATA PARSING
# =============================================================================

SAMPLE_METADATA = """
-- =====================================================================
-- DELETE existing records
-- =====================================================================
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'SalesAnalytics');

DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'SalesAnalytics');

DELETE FROM Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'SalesAnalytics';

-- =====================================================================
-- Orchestration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Orchestration
([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
('SalesAnalytics', 1, 1001, 'bronze', 'dbo.raw_orders', 'order_id', 'batch', 1),
('SalesAnalytics', 2, 1101, 'silver', 'dbo.clean_orders', 'order_id', 'batch', 1),
('SalesAnalytics', 3, 1201, 'gold', 'dbo.sales_summary', 'region', 'batch', 1);

-- =====================================================================
-- Primary Configuration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(1001, 'source_details', 'table_name', 'ERP.dbo.Orders'),
(1001, 'source_details', 'datastore_name', 'OracleProduction'),
(1001, 'target_details', 'merge_type', 'overwrite'),
(1101, 'source_details', 'table_name', 'bronze.dbo.raw_orders'),
(1101, 'target_details', 'merge_type', 'overwrite'),
(1201, 'source_details', 'table_name', 'silver.dbo.clean_orders'),
(1201, 'source_details', 'custom_transformation_function', 'custom_sales_aggregation'),
(1201, 'target_details', 'merge_type', 'overwrite');

-- =====================================================================
-- Advanced Configuration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
(1101, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'status != ''cancelled'''),
(1101, 'data_transformation_steps', 'join_data', 2, 'join_type', 'left'),
(1101, 'data_transformation_steps', 'join_data', 2, 'right_table_name', 'bronze.dbo.raw_customers'),
(1101, 'data_transformation_steps', 'join_data', 2, 'join_condition', 'a.customer_id = b.customer_id'),
(1201, 'data_transformation_steps', 'aggregate_data', 1, 'group_by_columns', 'region'),
(1201, 'data_transformation_steps', 'aggregate_data', 1, 'aggregation', 'SUM(order_total) AS total_sales'),
(1201, 'data_transformation_steps', 'conditional_column', 2, 'column_name', 'performance_tier'),
(1201, 'data_transformation_steps', 'conditional_column', 2, 'expression', 'CASE WHEN total_sales > 1000000 THEN ''high'' ELSE ''standard'' END'),
(1201, 'data_quality', 'validate_condition', 1, 'condition', 'region IS NOT NULL'),
(1201, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine');
"""


class TestMetadataParsing:
    def test_parse_orchestration(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert 1001 in info.table_ids
        assert 1101 in info.table_ids
        assert 1201 in info.table_ids

    def test_parse_target_entities(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert "dbo.raw_orders" in info.target_entities
        assert "dbo.clean_orders" in info.target_entities
        assert "dbo.sales_summary" in info.target_entities

    def test_parse_source_tables(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert "erp.dbo.orders" in info.source_tables
        assert "bronze.dbo.raw_orders" in info.source_tables

    def test_parse_join_right_tables(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert "bronze.dbo.raw_customers" in info.join_right_tables

    def test_parse_transformation_types(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert "filter_data" in info.transformation_types
        assert "join_data" in info.transformation_types
        assert "aggregate_data" in info.transformation_types
        assert "conditional_column" in info.transformation_types

    def test_parse_custom_functions(self):
        info = parse_metadata_sql(SAMPLE_METADATA)
        assert "custom_sales_aggregation" in info.custom_functions


# =============================================================================
# CHECK 1: TABLE COVERAGE
# =============================================================================


class TestCheckTableCoverage:
    def test_all_matched(self):
        tables = [
            TableRef("dbo.Orders", "dbo.orders", "FROM", 1, "high"),
            TableRef("dbo.Customers", "dbo.customers", "JOIN", 2, "high"),
        ]
        metadata = MetadataInfo(
            source_tables=["dbo.orders"],
            target_entities=["dbo.customers"],
        )
        result = check_table_coverage(tables, metadata)
        assert result.status == "PASS"
        assert result.details["matched"] == 2
        assert len(result.details["unmatched"]) == 0

    def test_unmatched_table(self):
        tables = [
            TableRef("dbo.Orders", "dbo.orders", "FROM", 1, "high"),
            TableRef("dbo.AuditLog", "dbo.auditlog", "FROM", 5, "high"),
        ]
        metadata = MetadataInfo(source_tables=["dbo.orders"])
        result = check_table_coverage(tables, metadata)
        assert result.status == "WARN"
        assert len(result.details["unmatched"]) == 1
        assert result.details["unmatched"][0]["normalized"] == "dbo.auditlog"

    def test_cte_excluded(self):
        tables = [
            TableRef("order_totals", "order_totals", "FROM", 1, "high", is_cte=True),
            TableRef("dbo.Orders", "dbo.orders", "FROM", 3, "high"),
        ]
        metadata = MetadataInfo(source_tables=["dbo.orders"])
        result = check_table_coverage(tables, metadata)
        assert result.status == "PASS"
        assert "order_totals" in result.details["excluded_ctes"]

    def test_temp_table_excluded(self):
        tables = [
            TableRef("#staging", "#staging", "INTO", 1, "high", is_temp=True),
            TableRef("dbo.Orders", "dbo.orders", "FROM", 3, "high"),
        ]
        metadata = MetadataInfo(source_tables=["dbo.orders"])
        result = check_table_coverage(tables, metadata)
        assert result.status == "PASS"

    def test_fuzzy_match_different_schema(self):
        tables = [
            TableRef("dbo.Orders", "dbo.orders", "FROM", 1, "high"),
        ]
        metadata = MetadataInfo(source_tables=["silver.dbo.orders"])
        result = check_table_coverage(tables, metadata)
        assert result.status == "PASS"

    def test_found_in_join_right_table(self):
        tables = [
            TableRef("dbo.Customers", "dbo.customers", "JOIN", 1, "high"),
        ]
        metadata = MetadataInfo(join_right_tables=["bronze.dbo.customers"])
        result = check_table_coverage(tables, metadata)
        assert result.status == "PASS"


# =============================================================================
# CHECK 2: COMPLEXITY COVERAGE
# =============================================================================


class TestCheckComplexity:
    def test_skip_when_no_signals(self):
        result = check_complexity_coverage([], MetadataInfo())
        assert result.status == "SKIP"

    def test_pass_with_custom_function(self):
        signals = [ComplexitySignal("cursor", 10, "DECLARE cur CURSOR")]
        metadata = MetadataInfo(custom_functions=["custom_order_processing"])
        result = check_complexity_coverage(signals, metadata)
        assert result.status == "PASS"

    def test_fail_without_custom_function(self):
        signals = [ComplexitySignal("cursor", 10, "DECLARE cur CURSOR")]
        metadata = MetadataInfo()
        result = check_complexity_coverage(signals, metadata)
        assert result.status == "FAIL"

    def test_multiple_signals(self):
        signals = [
            ComplexitySignal("cursor", 10, "DECLARE cur CURSOR"),
            ComplexitySignal("udf", 50, "@udf"),
        ]
        metadata = MetadataInfo()
        result = check_complexity_coverage(signals, metadata)
        assert result.status == "FAIL"
        assert "cursor" in result.details["verdict"]
        assert "udf" in result.details["verdict"]


# =============================================================================
# CHECK 3: OPERATION COVERAGE
# =============================================================================


class TestCheckOperations:
    def test_all_matched(self):
        ops = {"join": True, "aggregate": True, "filter": False}
        metadata = MetadataInfo(
            transformation_types={"join_data", "aggregate_data"}
        )
        result = check_operation_coverage(ops, metadata)
        assert result.status == "PASS"
        assert "join_data" in result.details["matched"]
        assert "aggregate_data" in result.details["matched"]

    def test_unmatched_operation(self):
        ops = {"join": True, "window_function": True, "filter": False}
        metadata = MetadataInfo(transformation_types={"join_data"})
        result = check_operation_coverage(ops, metadata)
        assert result.status == "WARN"
        assert any(
            u["expected_transformation"] == "add_window_function"
            for u in result.details["unmatched"]
        )

    def test_no_operations(self):
        ops = {k: False for k in ["join", "aggregate", "filter"]}
        metadata = MetadataInfo()
        result = check_operation_coverage(ops, metadata)
        assert result.status == "PASS"


# =============================================================================
# END-TO-END
# =============================================================================


SAMPLE_LEGACY_SQL = """
-- Sales Analytics ETL
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.region,
        o.order_date,
        SUM(li.amount) AS total_amount,
        CASE WHEN SUM(li.amount) > 1000 THEN 'high' ELSE 'standard' END AS order_tier
    FROM dbo.Orders o
    INNER JOIN dbo.LineItems li ON o.order_id = li.order_id
    WHERE o.status != 'cancelled'
    GROUP BY o.order_id, o.customer_id, o.region, o.order_date
)
SELECT
    ot.*,
    c.customer_name,
    COALESCE(c.segment, 'Unknown') AS segment,
    ROW_NUMBER() OVER(PARTITION BY ot.region ORDER BY ot.total_amount DESC) as rank_in_region
FROM order_totals ot
LEFT JOIN dbo.Customers c ON ot.customer_id = c.customer_id
"""


class TestEndToEnd:
    def test_full_run(self):
        result, exit_code = run(
            [(SAMPLE_LEGACY_SQL, "legacy.sql")],
            SAMPLE_METADATA,
            output_format="json",
        )
        # Should not FAIL (no complexity signals)
        assert exit_code == 0
        assert result["check_2_complexity"]["status"] == "SKIP"

        # Tables: Orders, LineItems, Customers should be checked
        # order_totals is a CTE and should be excluded
        tc = result["check_1_tables"]
        unmatched_names = {u["normalized"] for u in tc["unmatched"]}
        # LineItems is not in the sample metadata — should be unmatched
        assert "dbo.lineitems" in unmatched_names
        # Orders should match (ERP.dbo.Orders in metadata matches by table part)
        assert "dbo.orders" not in unmatched_names

        # Operations: join, aggregate, conditional, window_function, filter, null_handling
        oc = result["check_3_operations"]
        assert "join_data" in oc["matched"]
        assert "aggregate_data" in oc["matched"]
        assert "filter_data" in oc["matched"]

    def test_complexity_fail(self):
        cursor_sql = """
        DECLARE order_cur CURSOR FOR SELECT * FROM dbo.Orders
        OPEN order_cur
        FETCH NEXT FROM order_cur
        WHILE @@FETCH_STATUS = 0 BEGIN
            -- process row
            FETCH NEXT FROM order_cur
        END
        """
        # Metadata with NO custom functions
        simple_metadata = """
        INSERT INTO Data_Pipeline_Metadata_Orchestration
        ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
        VALUES
        ('Test', 1, 1001, 'bronze', 'dbo.orders', 'order_id', 'batch', 1);

        INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
        (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
        VALUES
        (1001, 'source_details', 'table_name', 'dbo.Orders'),
        (1001, 'target_details', 'merge_type', 'overwrite');
        """
        result, exit_code = run(
            [(cursor_sql, "cursor.sql")],
            simple_metadata,
            output_format="json",
        )
        assert exit_code == 1
        assert result["check_2_complexity"]["status"] == "FAIL"

    def test_pyspark_source(self):
        pyspark_code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window

df_orders = spark.read.table("bronze.dbo.raw_orders")
df_customers = spark.read.table("bronze.dbo.raw_customers")

df_joined = df_orders.join(df_customers, "customer_id", "left")
df_filtered = df_joined.filter(F.col("status") != "cancelled")
df_agg = df_filtered.groupBy("region").agg(F.sum("amount").alias("total"))

w = Window.partitionBy("region").orderBy(F.desc("total"))
df_ranked = df_agg.withColumn("rank", F.row_number().over(w))

df_ranked.write.mode("overwrite").saveAsTable("gold.dbo.sales_summary")
        """
        result, exit_code = run(
            [(pyspark_code, "etl.py")],
            SAMPLE_METADATA,
            output_format="json",
        )
        assert exit_code == 0
        # Tables should be found
        tc = result["check_1_tables"]
        matched_count = tc["matched"]
        assert matched_count >= 2  # raw_orders and raw_customers at minimum

        # Operations
        oc = result["check_3_operations"]
        assert "join_data" in oc["matched"]
        assert "aggregate_data" in oc["matched"]
        assert "filter_data" in oc["matched"]
