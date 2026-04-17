# Datastore configuration

Per-environment declarative config that tells the accelerator which Databricks
workspace and which Unity Catalog catalogs map to each medallion layer, plus
connection details for any external source systems the pipelines read from.

## Files

One JSON file per environment: `datastore_<ENV>.json` (for example
`datastore_DEV.json`, `datastore_TEST.json`, `datastore_PROD.json`).

## Schema

| Key                     | Required | Purpose                                                                     |
|-------------------------|----------|-----------------------------------------------------------------------------|
| `environment`           | yes      | Environment label. Must match the filename suffix.                          |
| `workspace_id`          | yes      | Databricks workspace numeric ID. Verified at runtime against the SDK.       |
| `workspace_url`         | yes      | `https://adb-<id>.<region>.azuredatabricks.net`.                            |
| `sql_warehouse_id`      | yes      | Default SQL warehouse for agent queries. Can be overridden per branch.      |
| `layers.<name>.catalog` | yes      | Unity Catalog name for the medallion layer (e.g. `dev_silver`).             |
| `metadata.catalog`      | yes      | Catalog that holds accelerator metadata tables.                             |
| `metadata.schema`       | yes      | Schema within the metadata catalog (where `Datastore_Configuration` lives). |
| `metadata.sql_warehouse_id` | no   | Warehouse used to query metadata. Falls back to top-level `sql_warehouse_id`. |
| `external_datastores.<name>.kind` | no | Discriminator for non-Databricks sources (`sql_server`, `snowflake`, `rest_api`, `adls_gen2`, ...). |
| `external_datastores.<name>.connection_details` | no | Free-form dict with kind-specific fields (host, database, secret scope, base URL, ...). Serialized into the `Connection_Details` column at sync time. |

> **Where does the schema per layer live?**
> Per-layer schemas are **not** stored here. Each metadata row's `Target_Entity`
> is `"schema.table"`, so one datastore row serves every table in that layer —
> the writer combines `Catalog_Name` (from here) with the schema half of
> `Target_Entity` (from the metadata row) when it builds the fully-qualified
> target table name.

## Deploying to the runtime

`Datastore_Configuration` (in `{metadata.catalog}.{metadata.schema}`) is the
runtime-side mirror of this JSON. It is upserted automatically during
`/fdp-04-commit` via
`automation_scripts/agents/sync_datastore_config.py`, which turns the JSON
into a MERGE statement executed on the metadata SQL warehouse.

## Runtime guardrail

On every agent invocation, the accelerator calls
`WorkspaceClient().get_workspace_id()` and asserts it matches `workspace_id`
in the selected datastore file. This prevents accidental cross-environment
execution (e.g. pointing a DEV branch at the PROD workspace).

## Feature branch overrides

Per-branch patches live in `overrides/<sanitized-branch-name>.json`.
See [overrides/README.md](overrides/README.md) for the supported schema.
