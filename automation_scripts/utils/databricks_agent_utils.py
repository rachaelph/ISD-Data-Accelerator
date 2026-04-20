"""Shared utilities for Databricks agent automation scripts.

Provides:
- Databricks SDK ``WorkspaceClient`` lifecycle
- SQL execution via the Statement Execution API
- Workspace config resolution and overrides
- Datastore / metadata file parsing
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AUTH_GUIDANCE_MESSAGE = (
    "Databricks authentication is not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN "
    "environment variables, or configure a Databricks CLI profile with 'databricks auth login'."
)
STATEMENT_TIMEOUT = "50s"

READ_ONLY_SQL_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE | re.DOTALL)
FORBIDDEN_SQL_KEYWORDS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bTRUNCATE\b",
        r"\bALTER\b",
        r"\bCREATE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
        r"\bMERGE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
    ]
]

# ---------------------------------------------------------------------------
# Module-level caches
# ---------------------------------------------------------------------------
_WORKSPACE_CLIENT: WorkspaceClient | None = None
_WORKSPACE_CLIENT_LOCK = Lock()
_METADATA_TARGET_CACHE: dict[str, dict[int, tuple[str, str]]] = {}
_METADATA_TABLE_NAME_CACHE: dict[str, list[tuple[int, str, str]]] = {}


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------
class AgentError(Exception):
    pass


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class MetadataWarehouseResolution:
    source_directory: Path
    git_folder_name: str
    git_folder_path: Path
    environment: str
    datastore_sql_path: Path
    metadata_datastore_name: str
    metadata_warehouse_id: str | None
    metadata_workspace_id: str | None
    metadata_workspace_name: str | None
    medallion_layer: str | None
    metadata_database_name: str
    connection_id: str | None


@dataclass
class DatastoreResolution:
    endpoint: str
    datastore_name: str
    datastore_type: str
    datastore_id: str | None
    medallion_layer: str | None
    workspace_name: str | None
    target_entity: str | None
    environment: str


@dataclass
class EngineFolderResolution:
    """Resolved location of the batch engine folder (marker-based discovery)."""
    path: Path
    metadata_dir: Path
    custom_functions_dir: Path
    datastores_dir: Path


@dataclass
class DatastoreConfig:
    """Parsed contents of ``datastores/datastore_<ENV>.json``.

    ``layers`` maps a medallion layer name (``"bronze"``, ``"silver"``, ...)
    to ``{"catalog": "<unity_catalog_name>"}``. Schemas are deliberately **not**
    stored here — they live on each metadata row's ``Target_Entity``
    (``"schema.table"``), so one datastore row serves every table in that layer.

    ``external_datastores`` describes non-Databricks systems (SAP, SQL Server,
    Snowflake, REST APIs, ADLS, ...). Each entry carries a ``kind`` discriminator
    plus a free-form ``connection_details`` dict that is serialized as JSON into
    the ``Connection_Details`` column of ``Datastore_Configuration``.
    """
    environment: str
    workspace_id: str
    workspace_url: str
    sql_warehouse_id: str
    layers: dict[str, dict[str, str]]
    metadata_catalog: str
    metadata_schema: str
    metadata_sql_warehouse_id: str
    source_path: Path
    external_datastores: dict[str, dict[str, Any]] = field(default_factory=dict)
    extra_variables: dict[str, str] = field(default_factory=dict)
    active_branch_override: str | None = None
    override_source_path: Path | None = None


@dataclass
class SqlResult:
    columns: list[str]
    rows: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Databricks SDK client
# ---------------------------------------------------------------------------
def get_workspace_client() -> WorkspaceClient:
    """Return a cached :class:`WorkspaceClient`.

    Authentication is resolved automatically by the Databricks SDK from
    environment variables (``DATABRICKS_HOST``, ``DATABRICKS_TOKEN``),
    a ``~/.databrickscfg`` profile, or Azure CLI credentials.
    """
    global _WORKSPACE_CLIENT
    with _WORKSPACE_CLIENT_LOCK:
        if _WORKSPACE_CLIENT is None:
            try:
                _WORKSPACE_CLIENT = WorkspaceClient()
            except Exception as exc:
                raise AgentError(f"{AUTH_GUIDANCE_MESSAGE} Details: {exc}") from exc
        return _WORKSPACE_CLIENT


# ---------------------------------------------------------------------------
# SQL Execution — Statement Execution API
# ---------------------------------------------------------------------------
def _convert_value(value: str | None, type_name: str) -> Any:
    """Best-effort conversion of Statement Execution API string values."""
    if value is None:
        return None
    type_upper = type_name.upper()
    if type_upper in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SHORT", "BYTE"):
        try:
            return int(value)
        except ValueError:
            return value
    if type_upper in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"):
        try:
            return float(value)
        except ValueError:
            return value
    if type_upper == "BOOLEAN":
        return value.lower() in ("true", "1")
    return value


def execute_sql_query(
    warehouse_id: str,
    query: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> SqlResult:
    """Execute a single SQL query and return structured results."""
    client = get_workspace_client()
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        catalog=catalog,
        schema=schema,
        wait_timeout=STATEMENT_TIMEOUT,
        disposition=Disposition.INLINE,
    )

    if response.status and response.status.state in (
        StatementState.FAILED,
        StatementState.CANCELED,
        StatementState.CLOSED,
    ):
        error_msg = (
            response.status.error.message
            if response.status.error
            else "Unknown error"
        )
        raise AgentError(f"SQL query failed: {error_msg}")

    if not response.manifest or not response.result:
        return SqlResult(columns=[], rows=[])

    columns = [col.name for col in response.manifest.schema.columns]
    type_names = [col.type_text or "STRING" for col in response.manifest.schema.columns]
    rows: list[dict[str, Any]] = []
    for data_row in response.result.data_array or []:
        row: dict[str, Any] = {}
        for i, col_name in enumerate(columns):
            raw_value = data_row[i] if i < len(data_row) else None
            row[col_name] = _convert_value(raw_value, type_names[i])
        rows.append(row)
    return SqlResult(columns=columns, rows=rows)


def execute_sql_queries(
    warehouse_id: str,
    queries: list[tuple[str, str]],
    catalog: str | None = None,
    schema: str | None = None,
) -> dict[str, SqlResult]:
    """Execute multiple labelled queries sequentially and return results keyed by label."""
    results: dict[str, SqlResult] = {}
    for label, query in queries:
        results[label] = execute_sql_query(warehouse_id, query, catalog, schema)
    return results


def _strip_sql_comments(statement: str) -> str:
    """Return the statement with ``--`` line comments and ``/* */`` block comments removed.

    Used to decide whether a split chunk contains any executable SQL. Does not
    attempt to be SQL-aware about string literals — metadata SQL here does not
    contain ``--`` inside literals.
    """
    # Remove /* ... */ blocks (possibly multi-line, non-greedy).
    without_blocks = re.sub(r"/\*.*?\*/", "", statement, flags=re.DOTALL)
    # Remove -- line comments.
    cleaned_lines = []
    for line in without_blocks.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("--"):
            continue
        # Inline trailing comment after code.
        if "--" in line:
            line = line.split("--", 1)[0]
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def execute_sql_batch(
    warehouse_id: str,
    sql_text: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> None:
    """Execute multiple semicolon-separated SQL statements (for metadata deployment).

    Skips chunks that only contain comments (``--`` or ``/* */``) so a file's
    header block does not produce a ``[PARSE_SYNTAX_ERROR]`` from the warehouse.
    """
    client = get_workspace_client()
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for statement in statements:
        if not _strip_sql_comments(statement):
            # Comment-only chunk (e.g. the file header block) — nothing to execute.
            continue
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            catalog=catalog,
            schema=schema,
            wait_timeout=STATEMENT_TIMEOUT,
        )
        if response.status and response.status.state in (
            StatementState.FAILED,
            StatementState.CANCELED,
            StatementState.CLOSED,
        ):
            error_msg = (
                response.status.error.message
                if response.status.error
                else "Unknown error"
            )
            raise AgentError(
                f"SQL execution failed: {error_msg}\nStatement: {statement[:200]}"
            )


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _normalize_source_directory(source_directory: Path | str) -> Path:
    return Path(source_directory).resolve()


# ---------------------------------------------------------------------------
# Engine folder resolution (marker-based)
# ---------------------------------------------------------------------------
ENGINE_FOLDER_MARKERS = ("metadata", "custom_functions", "datastores")
ENGINE_FOLDER_ENV_VAR = "FDP_BATCH_ENGINE_FOLDER"

_ENGINE_FOLDER_CACHE: dict[str, EngineFolderResolution] = {}


def _looks_like_engine_folder(candidate: Path) -> bool:
    return candidate.is_dir() and all((candidate / marker).is_dir() for marker in ENGINE_FOLDER_MARKERS)


def _build_engine_resolution(path: Path) -> EngineFolderResolution:
    return EngineFolderResolution(
        path=path,
        metadata_dir=path / "metadata",
        custom_functions_dir=path / "custom_functions",
        datastores_dir=path / "datastores",
    )


def resolve_engine_folder(
    source_directory: Path | str,
    explicit_engine_folder: str | Path | None = None,
) -> EngineFolderResolution:
    """Locate the batch engine folder under ``source_directory``.

    Resolution order:
    1. Explicit override argument.
    2. ``FDP_BATCH_ENGINE_FOLDER`` environment variable.
    3. Unique top-level directory that contains ``metadata/``, ``custom_functions/``, and ``datastores/``.
    """
    resolved_root = _normalize_source_directory(source_directory)
    override = explicit_engine_folder or os.environ.get(ENGINE_FOLDER_ENV_VAR)

    if override:
        candidate = Path(override)
        if not candidate.is_absolute():
            candidate = (resolved_root / candidate).resolve()
        if not _looks_like_engine_folder(candidate):
            raise AgentError(
                f"Engine folder '{candidate}' does not contain the required subfolders: "
                f"{', '.join(ENGINE_FOLDER_MARKERS)}."
            )
        return _build_engine_resolution(candidate)

    cache_key = str(resolved_root)
    cached = _ENGINE_FOLDER_CACHE.get(cache_key)
    if cached is not None:
        return cached

    candidates = [child for child in resolved_root.iterdir() if _looks_like_engine_folder(child)]
    if not candidates:
        raise AgentError(
            f"No batch engine folder found under '{resolved_root}'. "
            f"Expected a top-level directory containing: {', '.join(ENGINE_FOLDER_MARKERS)}. "
            f"Set {ENGINE_FOLDER_ENV_VAR} or pass --engine-folder to override."
        )
    if len(candidates) > 1:
        names = ", ".join(sorted(child.name for child in candidates))
        raise AgentError(
            f"Multiple candidate engine folders found under '{resolved_root}': {names}. "
            f"Set {ENGINE_FOLDER_ENV_VAR} or pass --engine-folder to disambiguate."
        )

    resolution = _build_engine_resolution(candidates[0])
    _ENGINE_FOLDER_CACHE[cache_key] = resolution
    return resolution


# ---------------------------------------------------------------------------
# Datastore JSON config (per-environment)
# ---------------------------------------------------------------------------
_DATASTORE_CONFIG_CACHE: dict[str, DatastoreConfig] = {}


def _require_nonempty(payload: dict, key: str, path: Path) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AgentError(f"'{key}' is missing or empty in datastore config '{path}'.")
    return value.strip()


# ---------------------------------------------------------------------------
# Branch-based overrides
# ---------------------------------------------------------------------------
_BRANCH_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
OVERRIDES_SUBDIR = "overrides"


def _sanitize_branch_name(branch: str) -> str:
    """Turn a git branch name into a filesystem-safe slug.

    ``feature/hschwarz/add-sales`` → ``feature-hschwarz-add-sales``.
    """
    slug = _BRANCH_SANITIZE_PATTERN.sub("-", branch.strip()).strip("-.")
    return slug.lower()


def _find_branch_override_file(
    engine: EngineFolderResolution,
    branch: str | None,
) -> Path | None:
    if not branch or branch in ("(unknown)", "HEAD"):
        return None
    override_dir = engine.datastores_dir / OVERRIDES_SUBDIR
    if not override_dir.is_dir():
        return None
    slug = _sanitize_branch_name(branch)
    if not slug:
        return None
    candidate = override_dir / f"{slug}.json"
    return candidate if candidate.is_file() else None


def _load_branch_override_payload(override_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(_read_text(override_path))
    except json.JSONDecodeError as exc:
        raise AgentError(f"Failed to parse branch override '{override_path}': {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise AgentError(f"Branch override '{override_path}' must be a JSON object.")
    return payload


def _apply_datastore_overrides(
    config: DatastoreConfig,
    override_payload: dict[str, Any],
    override_path: Path,
    branch: str,
) -> DatastoreConfig:
    """Return a new :class:`DatastoreConfig` with branch-level patches merged in.

    The override payload may contain any subset of:
    - ``sql_warehouse_id`` (top-level string)
    - ``layers.<name>.catalog`` (per-layer catalog isolation for feature branches)
    - ``metadata.catalog`` / ``metadata.schema`` / ``metadata.sql_warehouse_id``
    - ``external_datastores.<name>.kind`` / ``external_datastores.<name>.connection_details``

    workspace_id and workspace_url are deliberately NOT override-able — branch
    isolation happens at the catalog level, never across workspaces. Per-layer
    schemas live on ``Target_Entity`` in metadata rows, not here, so layer
    overrides never touch schema.
    """
    sql_warehouse_id = config.sql_warehouse_id
    warehouse_override = override_payload.get("sql_warehouse_id")
    if isinstance(warehouse_override, str) and warehouse_override.strip():
        sql_warehouse_id = warehouse_override.strip()

    layers = {name: dict(defn) for name, defn in config.layers.items()}
    layers_override = override_payload.get("layers")
    if isinstance(layers_override, dict):
        for layer_name, layer_patch in layers_override.items():
            if not isinstance(layer_patch, dict):
                raise AgentError(
                    f"Layer override '{layer_name}' in '{override_path}' must be an object."
                )
            key = layer_name.lower()
            if key not in layers:
                raise AgentError(
                    f"Override '{override_path}' references unknown layer '{layer_name}'. "
                    f"Base layers: {', '.join(sorted(layers))}."
                )
            if "schema" in layer_patch:
                raise AgentError(
                    f"Override '{override_path}' patches 'schema' on layer '{layer_name}'. "
                    "Schemas live on each metadata row's Target_Entity, not in the datastore "
                    "config; use a different 'catalog' for branch isolation instead."
                )
            catalog_patch = layer_patch.get("catalog")
            if catalog_patch is None:
                continue
            if not isinstance(catalog_patch, str) or not catalog_patch.strip():
                raise AgentError(
                    f"Override '{override_path}' has an invalid 'catalog' for layer '{layer_name}'."
                )
            layers[key]["catalog"] = catalog_patch.strip()

    metadata_catalog = config.metadata_catalog
    metadata_schema = config.metadata_schema
    metadata_warehouse = config.metadata_sql_warehouse_id
    metadata_override = override_payload.get("metadata")
    if isinstance(metadata_override, dict):
        for meta_field in ("catalog", "schema", "sql_warehouse_id"):
            patch_value = metadata_override.get(meta_field)
            if patch_value is None:
                continue
            if not isinstance(patch_value, str) or not patch_value.strip():
                raise AgentError(
                    f"Override '{override_path}' has an invalid 'metadata.{meta_field}'."
                )
            new_value = patch_value.strip()
            if meta_field == "catalog":
                metadata_catalog = new_value
            elif meta_field == "schema":
                metadata_schema = new_value
            else:
                metadata_warehouse = new_value

    external_datastores = {
        name: {"kind": defn["kind"], "connection_details": dict(defn.get("connection_details") or {})}
        for name, defn in config.external_datastores.items()
    }
    external_override = override_payload.get("external_datastores")
    if isinstance(external_override, dict):
        for ext_name, ext_patch in external_override.items():
            if not isinstance(ext_patch, dict):
                raise AgentError(
                    f"External datastore override '{ext_name}' in '{override_path}' must be an object."
                )
            existing = external_datastores.get(ext_name)
            kind = ext_patch.get("kind")
            if existing is None and (not isinstance(kind, str) or not kind.strip()):
                raise AgentError(
                    f"External datastore override '{ext_name}' in '{override_path}' introduces a new "
                    "datastore but is missing 'kind'."
                )
            if isinstance(kind, str) and kind.strip():
                if existing is None:
                    external_datastores[ext_name] = {"kind": kind.strip(), "connection_details": {}}
                else:
                    existing["kind"] = kind.strip()
            conn_patch = ext_patch.get("connection_details")
            if conn_patch is not None:
                if not isinstance(conn_patch, dict):
                    raise AgentError(
                        f"'connection_details' for '{ext_name}' in '{override_path}' must be an object."
                    )
                target = external_datastores[ext_name]
                target["connection_details"] = {**target["connection_details"], **conn_patch}

    extra_variables = dict(config.extra_variables)
    variables_override = override_payload.get("variables")
    if isinstance(variables_override, dict):
        for name, value in variables_override.items():
            if not isinstance(name, str) or not name.strip():
                continue
            extra_variables[name.strip()] = "" if value is None else str(value)

    return DatastoreConfig(
        environment=config.environment,
        workspace_id=config.workspace_id,
        workspace_url=config.workspace_url,
        sql_warehouse_id=sql_warehouse_id,
        layers=layers,
        metadata_catalog=metadata_catalog,
        metadata_schema=metadata_schema,
        metadata_sql_warehouse_id=metadata_warehouse,
        source_path=config.source_path,
        external_datastores=external_datastores,
        extra_variables=extra_variables,
        active_branch_override=branch,
        override_source_path=override_path,
    )


def load_datastore_config(
    source_directory: Path | str,
    environment: str,
    engine_folder: EngineFolderResolution | None = None,
    branch: str | None = None,
) -> DatastoreConfig:
    """Load and validate ``datastores/datastore_<ENV>.json``.

    If ``branch`` is provided and an override file exists at
    ``datastores/overrides/<sanitized-branch>.json``, its patches are merged
    onto the base config. Pass ``branch=None`` to disable override lookup.
    """
    if not environment or not environment.strip():
        raise AgentError("environment is required to load datastore config.")
    env = environment.strip()

    resolved_engine = engine_folder or resolve_engine_folder(source_directory)
    config_path = resolved_engine.datastores_dir / f"datastore_{env}.json"

    effective_branch = branch if branch is not None else get_current_branch_name(source_directory)
    override_path = _find_branch_override_file(resolved_engine, effective_branch)
    cache_key = (
        f"{config_path.resolve()}||{override_path.resolve() if override_path else ''}"
    )
    cached = _DATASTORE_CONFIG_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if not config_path.exists():
        available = sorted(
            p.stem.removeprefix("datastore_")
            for p in resolved_engine.datastores_dir.glob("datastore_*.json")
        )
        raise AgentError(
            f"Datastore config not found at '{config_path}'. "
            f"Available environments: {', '.join(available) if available else '(none)'}."
        )

    try:
        payload = json.loads(_read_text(config_path))
    except json.JSONDecodeError as exc:
        raise AgentError(f"Failed to parse '{config_path}': {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise AgentError(f"Datastore config '{config_path}' must be a JSON object.")

    declared_env = _require_nonempty(payload, "environment", config_path)
    if declared_env.upper() != env.upper():
        raise AgentError(
            f"Datastore config '{config_path}' declares environment '{declared_env}' "
            f"but the filename requires '{env}'."
        )

    workspace_id = _require_nonempty(payload, "workspace_id", config_path)
    workspace_url = _require_nonempty(payload, "workspace_url", config_path)
    sql_warehouse_id = _require_nonempty(payload, "sql_warehouse_id", config_path)

    layers_raw = payload.get("layers")
    if not isinstance(layers_raw, dict) or not layers_raw:
        raise AgentError(f"'layers' must be a non-empty object in '{config_path}'.")

    layers: dict[str, dict[str, str]] = {}
    for layer_name, layer_def in layers_raw.items():
        if not isinstance(layer_def, dict):
            raise AgentError(f"Layer '{layer_name}' in '{config_path}' must be an object.")
        catalog = layer_def.get("catalog")
        if not isinstance(catalog, str) or not catalog.strip():
            raise AgentError(f"Layer '{layer_name}' is missing 'catalog' in '{config_path}'.")
        if "schema" in layer_def:
            raise AgentError(
                f"Layer '{layer_name}' in '{config_path}' defines 'schema'. "
                "Per-layer schemas live on each metadata row's Target_Entity "
                "('schema.table'), not in the datastore config."
            )
        layers[layer_name.lower()] = {"catalog": catalog.strip()}

    metadata_block = payload.get("metadata")
    if not isinstance(metadata_block, dict):
        raise AgentError(f"'metadata' block is missing or invalid in '{config_path}'.")
    metadata_catalog = _require_nonempty(metadata_block, "catalog", config_path)
    metadata_schema = _require_nonempty(metadata_block, "schema", config_path)
    metadata_warehouse = metadata_block.get("sql_warehouse_id") or sql_warehouse_id
    if not isinstance(metadata_warehouse, str) or not metadata_warehouse.strip():
        raise AgentError(f"metadata.sql_warehouse_id must be a non-empty string in '{config_path}'.")

    external_raw = payload.get("external_datastores") or {}
    if not isinstance(external_raw, dict):
        raise AgentError(f"'external_datastores' must be an object in '{config_path}'.")
    external_datastores: dict[str, dict[str, Any]] = {}
    for ext_name, ext_def in external_raw.items():
        if not isinstance(ext_def, dict):
            raise AgentError(
                f"External datastore '{ext_name}' in '{config_path}' must be an object."
            )
        kind = ext_def.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            raise AgentError(
                f"External datastore '{ext_name}' in '{config_path}' is missing 'kind' "
                "(e.g. 'sql_server', 'snowflake', 'rest_api', 'adls_gen2')."
            )
        conn = ext_def.get("connection_details") or {}
        if not isinstance(conn, dict):
            raise AgentError(
                f"External datastore '{ext_name}' 'connection_details' must be an object in '{config_path}'."
            )
        if ext_name.lower() in layers:
            raise AgentError(
                f"External datastore name '{ext_name}' collides with medallion layer in '{config_path}'."
            )
        external_datastores[ext_name] = {"kind": kind.strip(), "connection_details": dict(conn)}

    extra_variables_raw = payload.get("variables") or {}
    if not isinstance(extra_variables_raw, dict):
        raise AgentError(f"'variables' must be an object in '{config_path}'.")
    extra_variables: dict[str, str] = {}
    for var_name, var_value in extra_variables_raw.items():
        if not isinstance(var_name, str) or not var_name.strip():
            continue
        extra_variables[var_name.strip()] = "" if var_value is None else str(var_value)

    config = DatastoreConfig(
        environment=env.upper(),
        workspace_id=workspace_id,
        workspace_url=workspace_url,
        sql_warehouse_id=sql_warehouse_id,
        layers=layers,
        metadata_catalog=metadata_catalog,
        metadata_schema=metadata_schema,
        metadata_sql_warehouse_id=metadata_warehouse.strip(),
        source_path=config_path,
        external_datastores=external_datastores,
        extra_variables=extra_variables,
    )

    if override_path is not None:
        override_payload = _load_branch_override_payload(override_path)
        config = _apply_datastore_overrides(config, override_payload, override_path, effective_branch)

    _DATASTORE_CONFIG_CACHE[cache_key] = config
    return config


def assert_workspace_matches(config: DatastoreConfig) -> None:
    """Verify the active Databricks workspace matches the declared ``workspace_id``.

    Fails fast so an agent cannot accidentally run DEV metadata against PROD.
    """
    client = get_workspace_client()
    try:
        actual = str(client.get_workspace_id())
    except Exception as exc:
        raise AgentError(
            f"Could not determine active Databricks workspace ID: {exc}. {AUTH_GUIDANCE_MESSAGE}"
        ) from exc
    if actual != config.workspace_id:
        raise AgentError(
            f"Workspace mismatch: datastore config '{config.source_path}' expects workspace_id "
            f"'{config.workspace_id}' (environment '{config.environment}'), but the active "
            f"Databricks session targets workspace '{actual}'. Re-authenticate against the correct "
            f"workspace before running this command."
        )


# ---------------------------------------------------------------------------
# Datastore_Configuration Delta table sync
# ---------------------------------------------------------------------------
DATASTORE_CONFIG_TABLE_NAME = "Datastore_Configuration"
DATABRICKS_KIND = "databricks"
METADATA_DATASTORE_NAME = "metadata"
METADATA_LAYER_NAME = "metadata"


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def build_datastore_config_rows(config: DatastoreConfig) -> list[dict[str, str | None]]:
    """Materialize the rows that ``Datastore_Configuration`` should contain.

    One row per medallion layer, one row for the metadata warehouse, and one row
    per entry in ``external_datastores``. Schemas are not included — table-level
    schemas come from each metadata row's ``Target_Entity``.
    """
    rows: list[dict[str, str | None]] = []
    for layer_name in sorted(config.layers):
        layer_def = config.layers[layer_name]
        rows.append(
            {
                "Datastore_Name": layer_name,
                "Datastore_Kind": DATABRICKS_KIND,
                "Medallion_Layer": layer_name,
                "Workspace_ID": config.workspace_id,
                "Workspace_URL": config.workspace_url,
                "SQL_Warehouse_ID": config.sql_warehouse_id,
                "Catalog_Name": layer_def["catalog"],
                "Connection_Details": None,
            }
        )
    rows.append(
        {
            "Datastore_Name": METADATA_DATASTORE_NAME,
            "Datastore_Kind": DATABRICKS_KIND,
            "Medallion_Layer": METADATA_LAYER_NAME,
            "Workspace_ID": config.workspace_id,
            "Workspace_URL": config.workspace_url,
            "SQL_Warehouse_ID": config.metadata_sql_warehouse_id,
            "Catalog_Name": config.metadata_catalog,
            "Connection_Details": None,
        }
    )
    for ext_name in sorted(config.external_datastores):
        ext_def = config.external_datastores[ext_name]
        rows.append(
            {
                "Datastore_Name": ext_name,
                "Datastore_Kind": ext_def["kind"],
                "Medallion_Layer": None,
                "Workspace_ID": None,
                "Workspace_URL": None,
                "SQL_Warehouse_ID": None,
                "Catalog_Name": None,
                "Connection_Details": json.dumps(
                    ext_def.get("connection_details") or {},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        )
    return rows


_DATASTORE_CONFIG_COLUMNS = (
    "Datastore_Name",
    "Datastore_Kind",
    "Medallion_Layer",
    "Workspace_ID",
    "Workspace_URL",
    "SQL_Warehouse_ID",
    "Catalog_Name",
    "Connection_Details",
)


def build_datastore_config_merge_sql(config: DatastoreConfig) -> str:
    """Return a single MERGE statement that upserts every row derived from ``config``.

    The MERGE is scoped to ``{metadata_catalog}.{metadata_schema}.Datastore_Configuration``
    and keys on ``Datastore_Name``. It performs insert + update only — stale rows
    are not deleted automatically (use ``--prune`` on the sync agent for that).
    """
    rows = build_datastore_config_rows(config)
    values_rows = []
    for row in rows:
        values_rows.append(
            "("
            + ", ".join(_sql_literal(row[col]) for col in _DATASTORE_CONFIG_COLUMNS)
            + ")"
        )
    columns_list = ", ".join(_DATASTORE_CONFIG_COLUMNS)
    fq_table = (
        f"`{config.metadata_catalog}`.`{config.metadata_schema}`.`{DATASTORE_CONFIG_TABLE_NAME}`"
    )
    update_assignments = ", ".join(f"tgt.{c} = src.{c}" for c in _DATASTORE_CONFIG_COLUMNS if c != "Datastore_Name")
    insert_cols = columns_list
    insert_vals = ", ".join(f"src.{c}" for c in _DATASTORE_CONFIG_COLUMNS)
    return (
        f"MERGE INTO {fq_table} AS tgt\n"
        f"USING (\n"
        f"  SELECT * FROM VALUES\n    "
        + ",\n    ".join(values_rows)
        + f"\n  AS v({columns_list})\n"
        f") AS src\n"
        f"ON tgt.Datastore_Name = src.Datastore_Name\n"
        f"WHEN MATCHED THEN UPDATE SET {update_assignments}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def build_datastore_config_prune_sql(config: DatastoreConfig) -> str:
    """Return a DELETE statement that removes rows not in the current config."""
    rows = build_datastore_config_rows(config)
    names = ", ".join(_sql_literal(row["Datastore_Name"]) for row in rows)
    fq_table = (
        f"`{config.metadata_catalog}`.`{config.metadata_schema}`.`{DATASTORE_CONFIG_TABLE_NAME}`"
    )
    return f"DELETE FROM {fq_table} WHERE Datastore_Name NOT IN ({names})"


def sync_datastore_config_to_delta(
    config: DatastoreConfig,
    *,
    prune: bool = False,
    verify_workspace: bool = True,
) -> dict[str, Any]:
    """Upsert the ``Datastore_Configuration`` Delta table from ``config``.

    Runs against ``config.metadata_sql_warehouse_id`` inside
    ``config.metadata_catalog.config.metadata_schema``. When ``prune=True``,
    also removes rows whose ``Datastore_Name`` is not present in the current
    config (useful when a layer was renamed or removed in Git).
    """
    if verify_workspace:
        assert_workspace_matches(config)

    rows = build_datastore_config_rows(config)
    merge_sql = build_datastore_config_merge_sql(config)
    execute_sql_batch(
        config.metadata_sql_warehouse_id,
        merge_sql,
        catalog=config.metadata_catalog,
        schema=config.metadata_schema,
    )

    pruned = False
    if prune:
        execute_sql_batch(
            config.metadata_sql_warehouse_id,
            build_datastore_config_prune_sql(config),
            catalog=config.metadata_catalog,
            schema=config.metadata_schema,
        )
        pruned = True

    return {
        "tableFullyQualifiedName": (
            f"{config.metadata_catalog}.{config.metadata_schema}.{DATASTORE_CONFIG_TABLE_NAME}"
        ),
        "rowsUpserted": len(rows),
        "datastoreNames": [row["Datastore_Name"] for row in rows],
        "pruned": pruned,
        "environment": config.environment,
        "activeBranchOverride": config.active_branch_override,
        "overrideSourcePath": (
            str(config.override_source_path) if config.override_source_path else None
        ),
    }


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------
def get_current_branch_name(source_directory: Path | str) -> str:
    resolved_source_directory = _normalize_source_directory(source_directory)
    try:
        completed = subprocess.run(
            ["git", "-C", str(resolved_source_directory), "branch", "--show-current"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return "(unknown)"

    branch_name = completed.stdout.strip()
    return branch_name or "(unknown)"


# ---------------------------------------------------------------------------
# Execution context resolution (datastore_<ENV>.json is the single source of truth)
# ---------------------------------------------------------------------------
_CORE_VARIABLE_NAMES = (
    "sql_warehouse_id",
    "metadata_catalog",
    "metadata_schema",
    "metadata_database",
    "metadata_sql_warehouse_id",
    "workspace_id",
    "workspace_url",
)


def _build_variables_from_datastore(config: DatastoreConfig) -> dict[str, str]:
    """Flatten :class:`DatastoreConfig` into the ``Variables`` dict consumed by callers.

    Extra keys from the optional top-level ``variables`` object in the JSON
    (e.g. ``job_id_batch_processing``) are merged on top, so the datastore
    config is the single source of truth for all runtime values.
    """
    variables: dict[str, str] = {
        "sql_warehouse_id": config.sql_warehouse_id,
        "metadata_catalog": config.metadata_catalog,
        "metadata_schema": config.metadata_schema,
        "metadata_database": config.metadata_schema,
        "metadata_sql_warehouse_id": config.metadata_sql_warehouse_id,
        "workspace_id": config.workspace_id,
        "workspace_url": config.workspace_url,
    }
    for name, value in config.extra_variables.items():
        variables[name] = value
    return variables


def resolve_execution_context(
    source_directory: Path | str,
    git_folder_name: str | None = None,
    required_variables: list[str] | None = None,
    environment: str = "DEV",
) -> dict[str, Any]:
    """Resolve the shared execution context from ``datastore_<ENV>.json``.

    ``git_folder_name`` is optional: pass it only when a workflow wants to scope
    ``git status`` / ``git add`` to a sub-tree. When omitted the full repo is in
    scope.

    ``required_variables`` is validated against the merged ``Variables`` dict
    (core datastore fields + any top-level ``variables`` block from the JSON,
    with branch overrides applied).
    """
    resolved_source_directory = _normalize_source_directory(source_directory)
    engine = resolve_engine_folder(resolved_source_directory)
    branch = get_current_branch_name(resolved_source_directory)
    config = load_datastore_config(
        resolved_source_directory,
        environment,
        engine_folder=engine,
        branch=branch,
    )

    variables = _build_variables_from_datastore(config)
    for variable_name in required_variables or []:
        value = variables.get(variable_name)
        if value is None or value == "":
            raise AgentError(
                f"'{variable_name}' is missing or empty in datastore config '{config.source_path}'. "
                f"Add it to the 'variables' block (or as a core field) in the JSON."
            )

    has_overrides = config.override_source_path is not None
    override_path = str(config.override_source_path) if config.override_source_path else None
    git_folder_path = (
        str(resolved_source_directory / git_folder_name) if git_folder_name else None
    )

    return {
        "SourceDirectory": str(resolved_source_directory),
        "Environment": config.environment,
        "EngineFolderName": engine.path.name,
        "EngineFolderPath": str(engine.path),
        "GitFolderName": git_folder_name,
        "GitFolderPath": git_folder_path,
        "DatastoreConfigPath": str(config.source_path),
        "BranchName": branch,
        "HasOverrides": has_overrides,
        "OverrideSourcePath": override_path,
        "OverrideStatusMessage": (
            f"Using branch overrides from '{override_path}'"
            if has_overrides
            else f"Using base datastore config '{config.source_path}' (no branch overrides active)"
        ),
        "Variables": variables,
    }


# ---------------------------------------------------------------------------
# Datastore resolution (JSON config + metadata SQL scanning)
# ---------------------------------------------------------------------------
_METADATA_RECORD_PATTERN = re.compile(
    r"\(\s*'[^']+'\s*,\s*\d+\s*,\s*(\d+)\s*,\s*'([^']+)'\s*,\s*'([^']+)'",
    re.IGNORECASE,
)


def _iter_metadata_sql_files(metadata_dir: Path):
    """Yield every metadata SQL file under ``metadata_dir`` (recursive, ``*.sql``)."""
    for path in sorted(metadata_dir.rglob("*.sql")):
        if path.is_file():
            yield path


def _get_metadata_target_lookup(metadata_dir: Path) -> dict[int, tuple[str, str]]:
    cache_key = str(metadata_dir.resolve())
    cached_lookup = _METADATA_TARGET_CACHE.get(cache_key)
    if cached_lookup is not None:
        return cached_lookup

    lookup: dict[int, tuple[str, str]] = {}
    for file_path in _iter_metadata_sql_files(metadata_dir):
        for table_id_text, target_datastore_name, target_entity in _METADATA_RECORD_PATTERN.findall(_read_text(file_path)):
            table_id = int(table_id_text)
            if table_id not in lookup:
                lookup[table_id] = (target_datastore_name, target_entity)

    _METADATA_TARGET_CACHE[cache_key] = lookup
    return lookup


def _get_metadata_table_name_lookup(metadata_dir: Path) -> list[tuple[int, str, str]]:
    cache_key = str(metadata_dir.resolve())
    cached_lookup = _METADATA_TABLE_NAME_CACHE.get(cache_key)
    if cached_lookup is not None:
        return cached_lookup

    lookup: list[tuple[int, str, str]] = []
    for file_path in _iter_metadata_sql_files(metadata_dir):
        for table_id_text, target_datastore_name, target_entity in _METADATA_RECORD_PATTERN.findall(_read_text(file_path)):
            lookup.append((int(table_id_text), target_datastore_name, target_entity))

    _METADATA_TABLE_NAME_CACHE[cache_key] = lookup
    return lookup


def resolve_table_id_from_metadata(
    source_directory: Path | str,
    engine_folder: str | Path | None,
    table_name: str,
    medallion_layer: str,
) -> int:
    """Resolve a Table_ID by scanning metadata SQL files in the engine folder.

    ``engine_folder`` is the override for :func:`resolve_engine_folder`; pass ``None``
    to use marker-based auto-discovery.
    """
    resolved_source_directory = _normalize_source_directory(source_directory)
    engine = resolve_engine_folder(resolved_source_directory, engine_folder)
    metadata_dir = engine.metadata_dir
    if not metadata_dir.exists():
        raise AgentError(f"Metadata directory not found at '{metadata_dir}'.")

    normalized_table_name = table_name.strip().lower()
    normalized_layer = medallion_layer.strip().lower()
    matches: list[tuple[int, str]] = []
    for table_id, target_datastore_name, target_entity in _get_metadata_table_name_lookup(metadata_dir):
        if target_datastore_name.strip().lower() != normalized_layer:
            continue
        normalized_entity = target_entity.strip().lower()
        entity_table_name = normalized_entity.split(".", 1)[1] if "." in normalized_entity else normalized_entity
        if normalized_table_name not in {normalized_entity, entity_table_name}:
            continue
        matches.append((table_id, target_entity))

    if not matches:
        raise AgentError(
            f"Table '{table_name}' was not found in medallion layer '{medallion_layer}' under '{metadata_dir}'."
        )

    unique_matches = sorted(set(matches))
    if len(unique_matches) > 1:
        formatted_matches = ", ".join(f"Table_ID {table_id} ({target_entity})" for table_id, target_entity in unique_matches)
        raise AgentError(
            f"Table '{table_name}' matched multiple entries in layer '{medallion_layer}': {formatted_matches}."
        )

    return unique_matches[0][0]


def resolve_metadata_warehouse_from_datastore(
    source_directory: Path | str,
    engine_folder: str | Path | None,
    environment: str | None,
) -> MetadataWarehouseResolution:
    """Return the metadata SQL warehouse + catalog.schema for ``environment``."""
    if not environment:
        raise AgentError("--environment is required.")

    resolved_source_directory = _normalize_source_directory(source_directory)
    engine = resolve_engine_folder(resolved_source_directory, engine_folder)
    config = load_datastore_config(resolved_source_directory, environment, engine_folder=engine)

    metadata_datastore_name = f"{config.metadata_catalog}.{config.metadata_schema}"
    return MetadataWarehouseResolution(
        source_directory=resolved_source_directory,
        git_folder_name=engine.path.name,
        git_folder_path=engine.path,
        environment=config.environment,
        datastore_sql_path=config.source_path,
        metadata_datastore_name=metadata_datastore_name,
        metadata_warehouse_id=config.metadata_sql_warehouse_id,
        metadata_workspace_id=config.workspace_id,
        metadata_workspace_name=config.workspace_url,
        medallion_layer=None,
        metadata_database_name=metadata_datastore_name,
        connection_id=None,
    )


def resolve_datastore_endpoint(
    source_directory: Path | str,
    engine_folder: str | Path | None,
    environment: str,
    table_id: int | None,
    datastore_name: str | None,
) -> DatastoreResolution:
    """Return the SQL warehouse + catalog/schema/table for a Table_ID or layer name."""
    if table_id is None and not datastore_name:
        raise AgentError("Provide either table_id or datastore_name.")

    resolved_source_directory = _normalize_source_directory(source_directory)
    engine = resolve_engine_folder(resolved_source_directory, engine_folder)
    config = load_datastore_config(resolved_source_directory, environment, engine_folder=engine)

    target_layer = datastore_name
    target_entity = None
    if table_id is not None:
        metadata_targets = _get_metadata_target_lookup(engine.metadata_dir)
        target = metadata_targets.get(table_id)
        if target is None:
            raise AgentError(f"Table_ID {table_id} not found in any metadata file under '{engine.metadata_dir}'.")
        target_layer, target_entity = target

    assert target_layer is not None
    layer_key = target_layer.strip().lower()
    layer_def = config.layers.get(layer_key)
    if layer_def is None:
        available = ", ".join(sorted(config.layers))
        raise AgentError(
            f"Medallion layer '{target_layer}' not found in datastore config '{config.source_path}'. "
            f"Available layers: {available}."
        )

    if target_entity and "." not in target_entity:
        target_entity = f"{layer_key}.{target_entity.strip()}"

    return DatastoreResolution(
        endpoint=config.sql_warehouse_id,
        datastore_name=layer_def["catalog"],
        datastore_type="Layer",
        datastore_id=layer_def["catalog"],
        medallion_layer=layer_key,
        workspace_name=config.workspace_url,
        target_entity=target_entity,
        environment=config.environment,
    )


# ---------------------------------------------------------------------------
# Query safety & formatting
# ---------------------------------------------------------------------------
def enforce_read_only_query(query: str) -> None:
    trimmed_query = query.strip()
    if not READ_ONLY_SQL_PATTERN.match(trimmed_query):
        preview = trimmed_query[:50]
        raise AgentError(f"Only SELECT statements are permitted. Received: {preview}...")
    for pattern in FORBIDDEN_SQL_KEYWORDS:
        if pattern.search(trimmed_query):
            raise AgentError(
                f"Query contains forbidden keyword matching pattern '{pattern.pattern}'. Only SELECT statements are permitted."
            )


def format_rows(rows: list[dict[str, Any]], mode: str) -> str:
    if mode == "json":
        return json.dumps(rows, indent=2)
    return json.dumps(rows, separators=(",", ":"))
