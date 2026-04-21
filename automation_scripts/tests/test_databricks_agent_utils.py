from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import databricks_agent_utils as utils

SCRIPT_DIR = Path(__file__).resolve().parent


def _build_engine_fixture(repo_root: Path) -> Path:
    """Create a minimal databricks_batch_engine/ layout plus a sample metadata SQL file."""
    engine = repo_root / "databricks_batch_engine"
    (engine / "metadata").mkdir(parents=True, exist_ok=True)
    (engine / "custom_functions").mkdir(parents=True, exist_ok=True)
    (engine / "datastores").mkdir(parents=True, exist_ok=True)

    (engine / "metadata" / "metadata_OracleSales.sql").write_text(
        "-- sample metadata\n"
        "INSERT INTO dbo.Data_Movement_Metadata (Trigger_Name, Trigger_Step, Table_ID, Target_Datastore_Name, Target_Entity, Source_Datastore_Name, Source_Entity)\n"
        "VALUES\n"
        "  ('OracleSales', 1, 15, 'silver', 'dbo.oracle_customers', 'bronze', 'dbo.oracle_customers_raw');\n",
        encoding="utf-8",
    )

    (engine / "datastores" / "datastore_DEV.json").write_text(
        json.dumps(
            {
                "environment": "DEV",
                "workspace_id": "1111111111111111",
                "workspace_url": "https://adb-1111111111111111.1.azuredatabricks.net",
                "sql_warehouse_id": "wh-dev-default",
                "layers": {
                    "bronze": {"catalog": "dev_bronze"},
                    "silver": {"catalog": "dev_silver"},
                    "gold":   {"catalog": "dev_gold"},
                },
                "metadata": {
                    "catalog": "dev_meta",
                    "schema": "accelerator",
                },
                "external_datastores": {
                    "sap_erp": {
                        "kind": "sql_server",
                        "connection_details": {
                            "host": "sap.contoso.com",
                            "database": "ERP",
                            "secret_scope": "kv-sap",
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return engine


class DatabricksAgentUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        utils._WORKSPACE_CLIENT = None
        utils._METADATA_TARGET_CACHE.clear()
        utils._METADATA_TABLE_NAME_CACHE.clear()
        utils._ENGINE_FOLDER_CACHE.clear()
        utils._DATASTORE_CONFIG_CACHE.clear()

    def tearDown(self) -> None:
        utils._WORKSPACE_CLIENT = None

    def test_get_workspace_client_caches_instance(self) -> None:
        mock_client = MagicMock()
        with patch.object(utils, "WorkspaceClient", return_value=mock_client) as ctor:
            first = utils.get_workspace_client()
            second = utils.get_workspace_client()

        self.assertIs(first, mock_client)
        self.assertIs(second, mock_client)
        self.assertEqual(ctor.call_count, 1)

    def test_get_workspace_client_wraps_auth_error_as_agent_error(self) -> None:
        with patch.object(utils, "WorkspaceClient", side_effect=RuntimeError("no creds")):
            with self.assertRaises(utils.AgentError) as ctx:
                utils.get_workspace_client()
            self.assertIn("Databricks authentication is not configured", str(ctx.exception))
            self.assertIn("no creds", str(ctx.exception))

    def test_execute_sql_query_returns_typed_rows(self) -> None:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status.state = utils.StatementState.SUCCEEDED
        col_id = MagicMock()
        col_id.name = "id"
        col_id.type_text = "INT"
        col_name = MagicMock()
        col_name.name = "name"
        col_name.type_text = "STRING"
        mock_response.manifest.schema.columns = [col_id, col_name]
        mock_response.result.data_array = [["1", "Alice"], ["2", "Bob"]]
        mock_client.statement_execution.execute_statement.return_value = mock_response

        with patch.object(utils, "get_workspace_client", return_value=mock_client):
            result = utils.execute_sql_query("wh-123", "SELECT id, name FROM t")

        self.assertEqual(result.columns, ["id", "name"])
        self.assertEqual(result.rows, [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])

    def test_execute_sql_query_raises_on_failure(self) -> None:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status.state = utils.StatementState.FAILED
        mock_response.status.error.message = "syntax error"
        mock_client.statement_execution.execute_statement.return_value = mock_response

        with patch.object(utils, "get_workspace_client", return_value=mock_client):
            with self.assertRaises(utils.AgentError) as ctx:
                utils.execute_sql_query("wh-123", "SELECT bad")
            self.assertIn("syntax error", str(ctx.exception))

    def test_execute_sql_queries_returns_labelled_results(self) -> None:
        mock_client = MagicMock()

        def make_response(val: str, type_text: str = "INT"):
            resp = MagicMock()
            resp.status.state = utils.StatementState.SUCCEEDED
            col = MagicMock()
            col.name = "v"
            col.type_text = type_text
            resp.manifest.schema.columns = [col]
            resp.result.data_array = [[val]]
            return resp

        mock_client.statement_execution.execute_statement.side_effect = [
            make_response("1"),
            make_response("2"),
        ]

        with patch.object(utils, "get_workspace_client", return_value=mock_client):
            results = utils.execute_sql_queries(
                "wh-123",
                [("First", "SELECT 1 AS v"), ("Second", "SELECT 2 AS v")],
            )

        self.assertEqual(results["First"].rows, [{"v": 1}])
        self.assertEqual(results["Second"].rows, [{"v": 2}])

    def test_execute_sql_batch_splits_and_executes_statements(self) -> None:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status.state = utils.StatementState.SUCCEEDED
        mock_client.statement_execution.execute_statement.return_value = mock_response

        with patch.object(utils, "get_workspace_client", return_value=mock_client):
            utils.execute_sql_batch("wh-123", "INSERT INTO t VALUES (1); DELETE FROM t WHERE id = 2;")

        self.assertEqual(mock_client.statement_execution.execute_statement.call_count, 2)

    def test_resolve_table_id_from_metadata_accepts_unqualified_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            table_id = utils.resolve_table_id_from_metadata(repo_root, None, "oracle_customers", "silver")
            self.assertEqual(table_id, 15)

    def test_resolve_table_id_from_metadata_accepts_schema_qualified_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            table_id = utils.resolve_table_id_from_metadata(repo_root, None, "dbo.oracle_customers", "silver")
            self.assertEqual(table_id, 15)

    def test_resolve_datastore_endpoint_uses_resolved_table_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            resolution = utils.resolve_datastore_endpoint(
                source_directory=repo_root,
                engine_folder=None,
                environment="DEV",
                table_id=15,
                datastore_name=None,
            )

            self.assertEqual(resolution.medallion_layer, "silver")
            self.assertEqual(resolution.datastore_name, "dev_silver")
            self.assertEqual(resolution.target_entity, "dbo.oracle_customers")
            self.assertEqual(resolution.endpoint, "wh-dev-default")
            self.assertEqual(resolution.environment, "DEV")

    def test_resolve_engine_folder_detects_marker_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            engine = _build_engine_fixture(repo_root)
            resolution = utils.resolve_engine_folder(repo_root)
            self.assertEqual(resolution.path, engine.resolve())
            self.assertTrue(resolution.metadata_dir.is_dir())
            self.assertTrue(resolution.custom_functions_dir.is_dir())
            self.assertTrue(resolution.datastores_dir.is_dir())

    def test_load_datastore_config_enforces_environment_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            config = utils.load_datastore_config(repo_root, "DEV")
            self.assertEqual(config.environment, "DEV")
            self.assertEqual(config.workspace_id, "1111111111111111")
            self.assertEqual(config.sql_warehouse_id, "wh-dev-default")
            self.assertEqual(config.layers["silver"]["catalog"], "dev_silver")
            self.assertEqual(config.metadata_catalog, "dev_meta")
            self.assertEqual(config.metadata_sql_warehouse_id, "wh-dev-default")

    def test_load_datastore_config_applies_branch_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            engine = _build_engine_fixture(repo_root)
            override_dir = engine / "datastores" / "overrides"
            override_dir.mkdir()
            override_path = override_dir / "feature-hschwarz-add-sales.json"
            override_path.write_text(
                json.dumps(
                    {
                        "sql_warehouse_id": "wh-feature-hschwarz",
                        "layers": {
                            "silver": {"catalog": "dev_silver_feature_hschwarz"},
                        },
                        "metadata": {"schema": "feature_hschwarz_meta"},
                        "external_datastores": {
                            "sap_erp": {
                                "connection_details": {"database": "ERP_FEATURE"}
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = utils.load_datastore_config(
                repo_root, "DEV", branch="feature/hschwarz/add-sales"
            )
            self.assertEqual(config.active_branch_override, "feature/hschwarz/add-sales")
            self.assertEqual(config.override_source_path.resolve(), override_path.resolve())
            self.assertEqual(config.sql_warehouse_id, "wh-feature-hschwarz")
            # layers only patch catalog; schema never lives here
            self.assertEqual(config.layers["silver"]["catalog"], "dev_silver_feature_hschwarz")
            self.assertNotIn("schema", config.layers["silver"])
            # Unpatched layer preserved
            self.assertEqual(config.layers["bronze"]["catalog"], "dev_bronze")
            self.assertEqual(config.metadata_schema, "feature_hschwarz_meta")
            self.assertEqual(config.metadata_catalog, "dev_meta")
            # External override merges nested connection details
            self.assertEqual(
                config.external_datastores["sap_erp"]["connection_details"]["database"],
                "ERP_FEATURE",
            )
            self.assertEqual(
                config.external_datastores["sap_erp"]["connection_details"]["host"],
                "sap.contoso.com",
            )

    def test_load_datastore_config_rejects_layer_schema_patch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            engine = _build_engine_fixture(repo_root)
            override_dir = engine / "datastores" / "overrides"
            override_dir.mkdir()
            (override_dir / "feature-invalid.json").write_text(
                json.dumps({"layers": {"silver": {"schema": "feature_silver"}}}),
                encoding="utf-8",
            )
            with self.assertRaises(utils.AgentError) as ctx:
                utils.load_datastore_config(repo_root, "DEV", branch="feature/invalid")
            self.assertIn("schema", str(ctx.exception).lower())

    def test_load_datastore_config_rejects_layer_schema_in_base_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            engine = _build_engine_fixture(repo_root)
            bad_config = engine / "datastores" / "datastore_DEV.json"
            bad_config.write_text(
                json.dumps(
                    {
                        "environment": "DEV",
                        "workspace_id": "x",
                        "workspace_url": "x",
                        "sql_warehouse_id": "x",
                        "layers": {"silver": {"catalog": "x", "schema": "nope"}},
                        "metadata": {"catalog": "x", "schema": "x"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(utils.AgentError) as ctx:
                utils.load_datastore_config(repo_root, "DEV", branch=None)
            self.assertIn("schema", str(ctx.exception).lower())

    def test_build_datastore_config_rows_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            config = utils.load_datastore_config(repo_root, "DEV", branch="main")
            rows = utils.build_datastore_config_rows(config)

            names = [r["Datastore_Name"] for r in rows]
            self.assertEqual(names, ["bronze", "gold", "silver", "metadata", "sap_erp"])
            for name in ("bronze", "silver", "gold"):
                row = next(r for r in rows if r["Datastore_Name"] == name)
                self.assertEqual(row["Datastore_Kind"], "databricks")
                self.assertEqual(row["Medallion_Layer"], name)
                self.assertEqual(row["Workspace_ID"], "1111111111111111")
                self.assertEqual(row["SQL_Warehouse_ID"], "wh-dev-default")
                self.assertIsNone(row["Connection_Details"])
                self.assertTrue(row["Catalog_Name"].startswith("dev_"))
            meta = next(r for r in rows if r["Datastore_Name"] == "metadata")
            self.assertEqual(meta["Medallion_Layer"], "metadata")
            self.assertEqual(meta["Catalog_Name"], "dev_meta")
            ext = next(r for r in rows if r["Datastore_Name"] == "sap_erp")
            self.assertEqual(ext["Datastore_Kind"], "sql_server")
            self.assertIsNone(ext["Medallion_Layer"])
            self.assertIsNone(ext["Catalog_Name"])
            details = json.loads(ext["Connection_Details"])
            self.assertEqual(details["database"], "ERP")
            # Delta DDL column set must match
            self.assertEqual(set(rows[0].keys()), set(utils._DATASTORE_CONFIG_COLUMNS))

    def test_build_datastore_config_merge_sql_contains_new_columns_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            _build_engine_fixture(repo_root)
            config = utils.load_datastore_config(repo_root, "DEV", branch="main")
            sql = utils.build_datastore_config_merge_sql(config)
            self.assertIn("MERGE INTO", sql)
            self.assertIn("Datastore_Kind", sql)
            self.assertIn("Connection_Details", sql)
            # Removed legacy columns must not appear
            for old_col in ("Datastore_Type", "Datastore_ID", "Workspace_Name", "Endpoint", "Connection_ID", "Schema_Name"):
                self.assertNotIn(old_col, sql, f"Old column '{old_col}' leaked into MERGE SQL")

    def test_load_datastore_config_ignores_override_for_unrelated_branch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            engine = _build_engine_fixture(repo_root)
            override_dir = engine / "datastores" / "overrides"
            override_dir.mkdir()
            (override_dir / "feature-other.json").write_text(
                json.dumps({"sql_warehouse_id": "wh-other"}), encoding="utf-8"
            )

            config = utils.load_datastore_config(repo_root, "DEV", branch="main")
            self.assertIsNone(config.active_branch_override)
            self.assertEqual(config.sql_warehouse_id, "wh-dev-default")

    def test_convert_value_handles_types(self) -> None:
        self.assertEqual(utils._convert_value("42", "INT"), 42)
        self.assertEqual(utils._convert_value("3.14", "DOUBLE"), 3.14)
        self.assertEqual(utils._convert_value("true", "BOOLEAN"), True)
        self.assertEqual(utils._convert_value("hello", "STRING"), "hello")
        self.assertIsNone(utils._convert_value(None, "INT"))


if __name__ == "__main__":
    unittest.main()
