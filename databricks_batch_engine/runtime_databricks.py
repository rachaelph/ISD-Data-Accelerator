# Databricks notebook source

# COMMAND ----------

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

# COMMAND ----------

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
# sc = spark.sparkContext  # Not supported on shared clusters (JVM_ATTRIBUTE_NOT_SUPPORTED)

# Allow duplicate map keys in SP results (Get_Advanced_Metadata uses map_from_arrays)
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

_DBUTILS = None


def _get_dbutils():
    global _DBUTILS
    if _DBUTILS is not None:
        return _DBUTILS

    try:
        from pyspark.dbutils import DBUtils

        _DBUTILS = DBUtils(spark)
        return _DBUTILS
    except Exception:
        # Fall back to the Databricks-injected notebook global (only present inside a notebook session).
        injected = globals().get("__builtins__", {})
        if isinstance(injected, dict):
            injected = injected.get("dbutils")
        else:
            injected = getattr(injected, "dbutils", None)
        if injected is not None:
            _DBUTILS = injected
            return _DBUTILS
        _DBUTILS = None
        return None


def _is_local_path(path: str) -> bool:
    return str(path or "").startswith(("/", "./", "../")) and "://" not in str(path or "")

# COMMAND ----------

class _FsAdapter:
    def exists(self, path: str) -> bool:
        if not path:
            return False

        if _is_local_path(path):
            return Path(path).exists()

        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            return False

        try:
            dbutils_obj.fs.ls(path)
            return True
        except Exception:
            return False

    def ls(self, path: str) -> list[SimpleNamespace]:
        if _is_local_path(path):
            return [
                SimpleNamespace(name=entry.name, path=str(entry), isDir=entry.is_dir())
                for entry in Path(path).iterdir()
            ]

        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            raise FileNotFoundError(path)

        entries = []
        for entry in dbutils_obj.fs.ls(path):
            try:
                is_dir = entry.isDir()
            except Exception:
                is_dir = str(getattr(entry, "path", "")).endswith("/")
            entries.append(
                SimpleNamespace(
                    name=getattr(entry, "name", ""),
                    path=getattr(entry, "path", ""),
                    isDir=is_dir,
                )
            )
        return entries

    def mkdirs(self, path: str) -> None:
        if _is_local_path(path):
            Path(path).mkdir(parents=True, exist_ok=True)
            return

        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            Path(path).mkdir(parents=True, exist_ok=True)
            return
        dbutils_obj.fs.mkdirs(path)

    def rm(self, path: str, recurse: bool = False) -> None:
        if not path:
            return

        if _is_local_path(path):
            target = Path(path)
            if not target.exists():
                return
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            return

        dbutils_obj = _get_dbutils()
        if dbutils_obj is not None:
            dbutils_obj.fs.rm(path, recurse)

    def mv(self, source: str, target: str, overwrite: bool = False) -> None:
        if _is_local_path(source) and _is_local_path(target):
            target_path = Path(target)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if overwrite and target_path.exists():
                if target_path.is_dir():
                    shutil.rmtree(target_path)
                else:
                    target_path.unlink()
            shutil.move(source, target)
            return

        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            raise RuntimeError("dbutils.fs.mv is unavailable in the current runtime.")
        dbutils_obj.fs.mv(source, target, overwrite)

    def mounts(self) -> list:
        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            return []
        try:
            return list(dbutils_obj.fs.mounts())
        except Exception:
            return []

    def mount(self, source: str, mount_point: str, extra_configs: dict | None = None) -> None:
        raise NotImplementedError(
            "Databricks volumes are directly addressable. Mount-based custom-function access is not required in this port."
        )

    def unmount(self, mount_point: str) -> None:
        return None

    def getMountPath(self, mount_point: str) -> str:
        return mount_point

# COMMAND ----------

class _NotebookAdapter:
    def exit(self, value: str = "") -> None:
        dbutils_obj = _get_dbutils()
        if dbutils_obj is not None:
            dbutils_obj.notebook.exit(value)
            return
        raise SystemExit(value)

    def getDefinition(self, notebook_name: str) -> str:
        raise NotImplementedError(
            "Custom functions load from workspace Python files in the Databricks port. Notebook definitions are not used."
        )

# COMMAND ----------

def get_databricks_context() -> dict[str, Any]:
    dbutils_obj = _get_dbutils()
    if dbutils_obj is None:
        return {}

    try:
        raw_context = dbutils_obj.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        return json.loads(raw_context)
    except Exception:
        return {}


class _DbUtilsProxy:
    """
    Databricks-native proxy around `dbutils` that adds local-path fallbacks for fs/notebook
    operations (useful when pipelines are imported outside a Databricks session) and
    forwards every other attribute (widgets, secrets, jobs, library, ...) to the real dbutils.
    """

    fs = _FsAdapter()
    notebook = _NotebookAdapter()
    runtime = SimpleNamespace(context=get_databricks_context())

    def __getattr__(self, name: str) -> Any:
        dbutils_obj = _get_dbutils()
        if dbutils_obj is None:
            raise AttributeError(f"dbutils.{name} is unavailable outside a Databricks runtime.")
        return getattr(dbutils_obj, name)


dbutils = _DbUtilsProxy()

# COMMAND ----------

def build_databricks_run_url() -> str:
    context = get_databricks_context()
    tags = context.get("tags", {}) if isinstance(context, dict) else {}

    workspace_url = None
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        workspace_url = None

    workspace_url = workspace_url or context.get("browserHostName")
    if not workspace_url:
        return ""

    workspace_url = str(workspace_url).replace("https://", "").strip("/")
    job_id = tags.get("jobId")
    run_id = tags.get("jobRunId") or tags.get("runId")
    if job_id and run_id:
        return f"https://{workspace_url}/#job/{job_id}/run/{run_id}"
    return f"https://{workspace_url}"

# COMMAND ----------

def is_path_target(target: str) -> bool:
    normalized = str(target or "")
    return normalized.startswith(("/", "dbfs:/")) or "://" in normalized


def delta_sql_identifier(target: str) -> str:
    return f"delta.`{target}`" if is_path_target(target) else target


def get_delta_table_for_target(target: str) -> DeltaTable:
    if is_path_target(target):
        return DeltaTable.forPath(spark, target)
    return DeltaTable.forName(spark, target)


def read_delta_target(target: str) -> DataFrame:
    if is_path_target(target):
        return spark.read.format("delta").load(target)
    return spark.table(target)


def write_delta_target(df: DataFrame, target: str, mode: str, **options: Any) -> None:
    writer = df.write.format("delta").mode(mode)
    for key, value in options.items():
        writer = writer.option(key, value)

    if is_path_target(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


def describe_history_target(target: str) -> DataFrame:
    return spark.sql(f"DESCRIBE HISTORY {delta_sql_identifier(target)}")


def delta_target_exists(target: str) -> bool:
    if is_path_target(target):
        return dbutils.fs.exists(target)
    return spark.catalog.tableExists(target)


def show_tblproperties_target(target: str) -> DataFrame:
    return spark.sql(f"SHOW TBLPROPERTIES {delta_sql_identifier(target)}")


def unset_tblproperty_target(target: str, property_name: str) -> None:
    spark.sql(
        f"ALTER TABLE {delta_sql_identifier(target)} UNSET TBLPROPERTIES IF EXISTS ('{property_name}')"
    )


def vacuum_target(target: str) -> None:
    get_delta_table_for_target(target).vacuum()

# COMMAND ----------

def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "":
        return "NULL"
    escaped = text.replace("'", "''")
    return f"'{escaped}'"


def _qualify_object_name(object_name: str, namespace: str | None) -> str:
    if not namespace:
        return object_name
    return f"{namespace}.{object_name}"


def _qualify_legacy_sql(sql: str, namespace: str | None) -> str:
    sql = re.sub(
        r"\[dbo\]\.\[([A-Za-z0-9_]+)\]",
        lambda match: _qualify_object_name(match.group(1), namespace),
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\bdbo\.([A-Za-z0-9_]+)\b",
        lambda match: _qualify_object_name(match.group(1), namespace),
        sql,
        flags=re.IGNORECASE,
    )
    return sql.replace("[", "").replace("]", "")


def _bind_sql_parameters(sql: str, params: Sequence[Any]) -> str:
    """Bind positional ? parameters safely, even when literal values contain '?'."""
    parts = sql.split("?")
    if len(parts) - 1 != len(params):
        raise ValueError(
            f"Parameter count mismatch: SQL has {len(parts) - 1} placeholders but {len(params)} params provided"
        )
    bound = parts[0]
    for i, value in enumerate(params):
        bound += _sql_literal(value) + parts[i + 1]
    return bound

# COMMAND ----------

def _normalize_field_name(name: str) -> str:
    """Normalize Spark column names to PascalCase_With_Underscores.

    Databricks Unity Catalog may return lowercase column names from spark.sql().
    The batch processing code expects PascalCase keys (e.g. Datastore_Name, Table_ID).
    This is idempotent — already-PascalCase names pass through unchanged.
    """
    _UPPER_WORDS = {'id', 'url', 'abfss', 'sql', 'ddl'}
    parts = name.split('_')
    return '_'.join(
        part.upper() if part.lower() in _UPPER_WORDS else part.capitalize()
        for part in parts
    )

# COMMAND ----------

class DatabricksSqlCursor:
    def __init__(self, namespace: str | None = None):
        self.namespace = namespace
        self._rows: list[tuple] = []
        self.description = None

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        params = tuple(params or ())
        qualified_sql = _qualify_legacy_sql(sql, self.namespace)
        normalized = " ".join(qualified_sql.strip().split())

        if normalized.upper().startswith("EXEC "):
            match = re.search(r"EXEC\s+([A-Za-z0-9_.`]+)", normalized, flags=re.IGNORECASE)
            if not match:
                raise ValueError(f"Unable to parse stored procedure call: {sql}")
            procedure_name = match.group(1)
            statement = f"CALL {procedure_name}({', '.join(_sql_literal(value) for value in params)})"
        else:
            statement = _bind_sql_parameters(qualified_sql, params)

        result = spark.sql(statement)
        # Keep original Spark field names for Row value extraction (case-sensitive)
        original_field_names = [field.name for field in result.schema.fields]
        # Normalize to PascalCase for dict keys used by the batch processing code
        normalized_field_names = [_normalize_field_name(n) for n in original_field_names]
        self.description = [(name, None, None, None, None, None, None) for name in normalized_field_names] if normalized_field_names else None
        collected = result.collect()
        self._rows = [tuple(row[name] for name in original_field_names) for row in collected]
        return self

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def nextset(self) -> bool:
        return False

    def setinputsizes(self, *args: Any, **kwargs: Any) -> None:
        return None

    def close(self) -> None:
        self._rows = []
        self.description = None


class DatabricksSqlConnection:
    def __init__(self, namespace: str | None = None):
        self.namespace = namespace
        self.autocommit = True

    def cursor(self) -> DatabricksSqlCursor:
        return DatabricksSqlCursor(namespace=self.namespace)

    def close(self) -> None:
        return None

# COMMAND ----------

def _default_custom_function_roots() -> list[Path]:
    configured_root = os.environ.get("DATABRICKS_CUSTOM_FUNCTIONS_PATH", "")
    roots = []
    if configured_root:
        roots.extend(Path(path) for path in configured_root.split(os.pathsep) if path)
    # In Databricks notebooks __file__ is not defined, so fall back to /Workspace paths
    try:
        context = get_databricks_context()
        notebook_path = context.get("extraContext", {}).get("notebook_path", "")
        if notebook_path:
            # notebook_path is like /Users/.../databricks_batch_engine/runtime
            # Go up two levels to get the repo root
            workspace_root = Path("/Workspace") / Path(notebook_path).parent.parent
            roots.extend([workspace_root / "custom_functions", workspace_root / "src" / "custom_functions"])
    except Exception:
        pass
    return roots


def load_workspace_module_into_globals(module_name: str, target_globals: dict[str, Any]) -> Any:
    candidate_name = module_name if module_name.endswith(".py") else f"{module_name}.py"

    for root in _default_custom_function_roots():
        candidate = root / candidate_name
        if not candidate.exists():
            continue

        spec = importlib.util.spec_from_file_location(f"custom_{candidate.stem}", candidate)
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to build an import spec for custom module '{candidate}'.")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        for attribute_name, value in vars(module).items():
            if attribute_name.startswith("_"):
                continue
            target_globals[attribute_name] = value

        return module

    searched_paths = ", ".join(str(root / candidate_name) for root in _default_custom_function_roots())
    raise FileNotFoundError(
        f"Custom module '{module_name}' was not found. Searched: {searched_paths}"
    )
