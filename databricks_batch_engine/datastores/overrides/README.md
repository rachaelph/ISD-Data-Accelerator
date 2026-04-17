# Branch overrides for feature workspaces

Drop a JSON file named `<sanitized-branch-name>.json` in this folder to
activate per-branch overrides. The branch name is auto-detected from
`git branch --show-current`; slashes and other special characters are
replaced with `-`.

## Example

Working branch `feature/hschwarz/add-sales` → override file
`feature-hschwarz-add-sales.json`:

```json
{
  "sql_warehouse_id": "wh-feature-hschwarz",
  "layers": {
    "silver": { "catalog": "dev_silver_feature_hschwarz" },
    "gold":   { "catalog": "dev_gold_feature_hschwarz" }
  },
  "metadata": {
    "schema": "feature_hschwarz_meta"
  },
  "external_datastores": {
    "sap_erp": {
      "connection_details": { "database": "ERP_FEATURE" }
    }
  },
  "variables": {
    "orchestration_job_name": "batch_orchestration_hschwarz"
  }
}
```

## Supported keys

| Key                                              | Patches                                                |
|--------------------------------------------------|--------------------------------------------------------|
| `sql_warehouse_id`                               | Default warehouse for medallion layers                 |
| `layers.<name>.catalog`                          | Unity Catalog for that medallion layer                 |
| `metadata.catalog`                               | Metadata tables catalog                                |
| `metadata.schema`                                | Metadata tables schema                                 |
| `metadata.sql_warehouse_id`                      | Warehouse used for metadata queries                    |
| `external_datastores.<name>.kind`                | Discriminator (`sql_server`, `snowflake`, `rest_api`, ...) — required when introducing a new external datastore |
| `external_datastores.<name>.connection_details`  | Shallow-merge onto the base entry's `connection_details` |
| `variables.<name>`                               | Any value defined in `workspace_config.json`           |

Keys that are deliberately **not** override-able:

- `workspace_id` / `workspace_url` — branch isolation stays within the same Databricks workspace.
- `layers.<name>.schema` — schemas live on each metadata row's `Target_Entity`
  (`schema.table`), not in the datastore config. Use a different `catalog` to
  isolate a feature branch.

## Safety guard

When an override file is active for the current branch, commit-time
safety checks raise `HasOverrides=true` and require explicit
confirmation before pushing to shared branches (`main`, `dev`, ...).
