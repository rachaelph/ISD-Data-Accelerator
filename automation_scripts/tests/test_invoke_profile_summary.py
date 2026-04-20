from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MODULE_PATH = REPO_ROOT / ".github" / "skills" / "table-observability" / "scripts" / "invoke_profile_summary.py"

spec = importlib.util.spec_from_file_location("invoke_profile_summary", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load module from {MODULE_PATH}")
profile_summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile_summary)


class InvokeProfileSummaryTests(unittest.TestCase):
    def test_build_live_profile_query_numeric_includes_numeric_aggregates(self) -> None:
        query = profile_summary.build_live_profile_query(
            "dbo",
            "oracle_customers",
            "cust_id",
            "int",
            "silver.dbo.oracle_customers",
        )

        self.assertIn("AVG(CAST(`cust_id` AS DOUBLE))", query)
        self.assertIn("STDDEV_SAMP(CAST(`cust_id` AS DOUBLE))", query)
        self.assertIn("FROM `dbo`.`oracle_customers`", query)
        self.assertIn("'silver.dbo.oracle_customers' AS Table_Name", query)

    def test_build_live_profile_query_temporal_skips_mean_and_stddev(self) -> None:
        query = profile_summary.build_live_profile_query(
            "dbo",
            "oracle_customers",
            "cust_eff_from",
            "date",
            "silver.dbo.oracle_customers",
        )

        self.assertIn("CAST(NULL AS DECIMAL(20,4)) AS Mean", query)
        self.assertIn("CAST(NULL AS DECIMAL(20,4)) AS Std_Dev", query)
        self.assertIn("MIN(`cust_eff_from`) AS `Min`", query)
        self.assertIn("MAX(`cust_eff_from`) AS `Max`", query)

    def test_get_live_profile_rows_returns_ordered_results_and_metadata(self) -> None:
        args = SimpleNamespace(
            source_directory=REPO_ROOT,
            engine_folder=None,
            environment="DEV",
        )
        target_resolution = SimpleNamespace(
            target_entity="dbo.oracle_customers",
            datastore_name="silver",
            datastore_type="Lakehouse",
            medallion_layer="silver",
            endpoint="endpoint.fabric.microsoft.com",
        )
        schema_rows = [
            {"COLUMN_NAME": "cust_id", "DATA_TYPE": "int", "ORDINAL_POSITION": 1},
            {"COLUMN_NAME": "cust_eff_from", "DATA_TYPE": "date", "ORDINAL_POSITION": 2},
        ]
        query_results = {
            "cust_id": SimpleNamespace(rows=[{"Column_Name": "cust_id", "Data_Type": "int"}]),
            "cust_eff_from": SimpleNamespace(rows=[{"Column_Name": "cust_eff_from", "Data_Type": "date"}]),
        }

        with patch.object(profile_summary, "resolve_datastore_endpoint", return_value=target_resolution), patch.object(
            profile_summary, "execute_sql_query", return_value=SimpleNamespace(rows=schema_rows)
        ), patch.object(profile_summary, "execute_sql_queries", return_value=query_results):
            metadata, rows = profile_summary.get_live_profile_rows(args, 15)

        self.assertEqual(metadata["targetEntity"], "dbo.oracle_customers")
        self.assertEqual(metadata["datastoreName"], "silver")
        self.assertEqual(metadata["medallionLayer"], "silver")
        self.assertEqual(rows, [
            {"Column_Name": "cust_id", "Data_Type": "int"},
            {"Column_Name": "cust_eff_from", "Data_Type": "date"},
        ])


if __name__ == "__main__":
    unittest.main()