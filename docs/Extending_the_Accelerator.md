# Extending the Accelerator

> Last updated: 2026-04-15

This guide covers all the ways you can customize the Databricks Data Engineering Accelerator to meet your specific needs.

Use the canonical reference docs for metadata and custom-function decisions while working through this guide:

- [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for valid metadata categories, names, attributes, and accepted values
- [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for choosing the correct custom function type
- [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for escalation from built-ins to custom code

---

## Table of Contents

- [Adding Custom Transformation Functions](#adding-custom-transformation-functions)
- [Adding Custom Ingestion Logic](#adding-custom-ingestion-logic)
- [Adding Reusable Helper Functions](#adding-reusable-helper-functions)
- [Modifying Core Modules](#modifying-core-modules)
- [Version Control with Git Tags](#version-control-with-git-tags)
- [Testing Your Changes](#testing-your-changes)

---

## Adding Custom Transformation Functions

The fastest way to add custom logic without touching core code.

### Step 1: Create a Custom Functions File

Create a new `.py` file (e.g., `custom_sales_transforms.py`) in the `custom_functions/` folder:

```python
# custom_functions/custom_sales_transforms.py

def transform_calculate_margin(new_data, metadata, spark):
    """
    Calculate profit margin from revenue and cost columns.

    Parameters:
    - new_data: PySpark DataFrame (the current data being processed)
    - metadata: dict containing orchestration_metadata, primary_config, datastore_config, function_config, event_payload
    - spark: SparkSession

    Returns:
    - new_data: PySpark DataFrame (transformed)
    """
    from pyspark.sql.functions import col, round as spark_round

    # ⭐ RECOMMENDED: Access your custom attributes from function_config
    # function_config contains your transformation step's attributes as simple key-value pairs
    function_config = metadata.get('function_config', {})
    revenue_col = function_config.get('revenue_column', 'revenue')
    cost_col = function_config.get('cost_column', 'cost')

    # ⚠️ AVOID: advanced_config is a list of dicts for ALL transformation steps - difficult to parse
    # Only use if you need to access OTHER transformation steps' configuration

    # Example transformation
    new_data = new_data.withColumn(
        "profit_margin",
        spark_round((col(revenue_col) - col(cost_col)) / col(revenue_col) * 100, 2)
    )

    return new_data
```

### Step 2: Reference in Metadata

```sql
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- Required attributes
(100, 'data_transformation_steps', 'custom_transformation_function', 1, 'functions_to_execute', 'transform_calculate_margin'),
(100, 'data_transformation_steps', 'custom_transformation_function', 1, 'files_to_run', 'custom_sales_transforms'),
-- Custom attributes for your function (optional - accessed via metadata dict)
(100, 'data_transformation_steps', 'custom_transformation_function', 1, 'revenue_column', 'total_revenue'),
(100, 'data_transformation_steps', 'custom_transformation_function', 1, 'cost_column', 'total_cost'),
(100, 'data_transformation_steps', 'custom_transformation_function', 1, 'output_column', 'margin_percent');
```

### Function Signature

See the template file for the required function signature and available metadata:

📄 **[Custom_Transformation_Function.py](resources/Custom_Transformation_Function.py)**

---

## Adding Custom Ingestion Logic

### Option A: Add a New Source Type (Recommended for External Sources)

For REST APIs and other external sources, the accelerator uses configurable source types. You can add new source types directly in the framework.

**Existing source types:** `oracle`, `azure_sql`, `sql_server`, `postgre_sql`, `my_sql`, `db2`, `rest_api`

**To add a new external source type:**

1. **Create a connection configuration** for your source type in the Databricks workspace

2. **Register the source** in `databricks_batch_engine/datastores/datastore_<ENV>.json` under `external_datastores.<name>` with a `kind` discriminator and a `connection_details` object. The entry is synced to `Datastore_Configuration` automatically on `/fdp-04-commit`.

3. **Configure via metadata** using `source_details_source` = your new source type value

This approach:
- ✅ Uses native connectors (better performance, managed authentication)
- ✅ Integrates with watermark tracking and error handling
- ✅ Follows the existing patterns customers are familiar with

### Option A.2: Add a Custom Staging Pipeline

Instead of (or in addition to) adding source types, you can add a **custom staging step**. This is useful when:

- Your external source requires complex extraction logic
- You want to leverage custom connectors during the staging/extraction phase
- You have existing scripts that already extract external data into staging volumes

**How it works:**

1. The custom staging function extracts data from the external source and stages it into volumes
2. The `pipeline_stage_and_batch` processing method picks up the staged files via `batch_processing.py` as usual
3. Data flows through the normal Bronze → Silver → Gold medallion layers from there

This approach:
- ✅ Leverages custom connectors for external sources
- ✅ Maintains the full `pipeline_stage_and_batch` framework benefits (watermarking, schema tracking, quality checks)
- ✅ Can be parameterized dynamically to handle multiple sources

### Option B: Custom Table Ingestion Function (For Complex SQL Logic)

Use `custom_table_ingestion_function` when you need PySpark logic that can't be achieved with standard connectors—for example, calling multiple APIs and joining results, or complex data assembly from multiple tables.

### Step 1: Create Custom Ingestion File

```python
# custom_functions/custom_complex_ingestion.py

def ingest_with_api_joins(metadata, spark):
    """
    Example: Fetch data from multiple endpoints and join in Spark.

    Use this pattern when:
    - You need to call multiple APIs and combine results
    - You need complex transformation during ingestion
    - The source requires custom authentication flows

    For simple REST API ingestion, use `source_details_source = 'rest_api'`
    with a Databricks connection instead.
    """
    import requests
    from pyspark.sql.functions import col

    # Get configuration from metadata - stores Key Vault name and secret NAME (not value)
    # Access custom attributes using primary_config.get() with the EXACT Configuration_Name from metadata
    # e.g., metadata row: (200, 'source_details', 'custom_table_ingestion_function_base_url', 'https://...')
    #       accessed as:  primary_config.get("custom_table_ingestion_function_base_url")
    primary_config = metadata.get('primary_config', {})
    base_url = primary_config.get("custom_table_ingestion_function_base_url")
    keyvault_name = primary_config.get("custom_table_ingestion_function_keyvault_name")
    secret_name = primary_config.get("custom_table_ingestion_function_api_key_secret_name")  # This is the NAME of the secret, not the secret itself

    # Retrieve actual secret at runtime from Databricks secrets
    api_key = dbutils.secrets.get(scope=keyvault_name, key=secret_name)
    headers = {"Authorization": f"Bearer {api_key}"}

    # Fetch from multiple endpoints
    customers = requests.get(f"{base_url}/customers", headers=headers).json()
    orders = requests.get(f"{base_url}/orders", headers=headers).json()

    # Convert to DataFrames and join
    customers_df = spark.createDataFrame(customers["data"])
    orders_df = spark.createDataFrame(orders["data"])

    df = orders_df.join(customers_df, "customer_id", "left")

    return df
```

### Step 2: Configure in Primary Metadata

> ⚠️ **Never store actual secrets in metadata.** Store only the Databricks secret scope and key **name** (not value). The function retrieves the actual secret at runtime.

```sql
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(200, 'source_details', 'custom_table_ingestion_function_file', 'custom_complex_ingestion'),
(200, 'source_details', 'custom_table_ingestion_function', 'ingest_with_api_joins'),
(200, 'source_details', 'custom_table_ingestion_function_base_url', 'https://api.example.com/v1'),
-- Store Databricks secret scope and key names only - never actual secret values!
(200, 'source_details', 'custom_table_ingestion_function_keyvault_name', 'your-secret-scope'),
(200, 'source_details', 'custom_table_ingestion_function_api_key_secret_name', 'example-api-key-secret-name');
```

### Function Signature

See the template file for the required function signature and available metadata:

📄 **[Custom_Table_Ingestion_Function.py](resources/Custom_Table_Ingestion_Function.py)**

---

## Adding Reusable Helper Functions

When multiple custom function files need the same utility code (authentication, API clients, data validation helpers, etc.), put that code in a **shared helper file** rather than duplicating it across custom files.

### Why Not Put Reusable Code in Custom Function Files?

Custom function files (e.g., `custom_osdu_ingestion.py`) are loaded via `load_workspace_module_into_globals()`, which executes their code into the shared session namespace. This means:

- ✅ Custom functions **can already call** all helper functions from `helper_functions_1/2/3.py` (e.g., `log_and_print()`, `_get_datastore_config()`)
- ❌ But code defined in one custom file is **not guaranteed to be available** in another custom file — loading order depends on which entity runs first
- ❌ Duplicating reusable code across custom files creates maintenance burden — a fix in one copy doesn't propagate to others

### The Solution: A Shared Helper Module

Create your own helper functions file that gets imported alongside the core helpers. Because imports execute **before** any custom functions are loaded, your reusable functions are guaranteed to be available everywhere.

#### Step 1: Create helper_functions_4.py

Create a new Python file in the engine directory:

```python
# helper_functions_4.py — Your organization's reusable helpers
# This file is NOT part of the core accelerator and will NOT be overwritten during upgrades.

# =============================================================================
# AUTHENTICATION HELPERS
# =============================================================================

def authenticate_with_keyvault(keyvault_name, secret_name):
    """
    Retrieve a secret from Databricks secrets for API authentication.
    Reusable across any custom file that needs auth.
    """
    token = dbutils.secrets.get(scope=keyvault_name, key=secret_name)
    return {"Authorization": f"Bearer {token}"}


# =============================================================================
# API CLIENT HELPERS  
# =============================================================================

def call_paginated_api(base_url, headers, page_size=100, max_pages=None):
    """
    Generic paginated API client. Returns all records across pages.
    Reusable by any custom notebook that calls REST APIs.
    """
    import requests
    all_records = []
    url = base_url
    page = 0
    while url:
        response = requests.get(url, headers=headers, params={"page_size": page_size})
        response.raise_for_status()
        data = response.json()
        all_records.extend(data.get('results', []))
        url = data.get('next_page_url')
        page += 1
        if max_pages and page >= max_pages:
            break
    log_and_print(f"Fetched {len(all_records)} total records from {page} pages")
    return all_records


# =============================================================================
# DATA VALIDATION HELPERS
# =============================================================================

def validate_required_columns(df, required_columns, context=""):
    """
    Validate that a DataFrame contains expected columns.
    Raises ValueError with details if columns are missing.
    """
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns{' in ' + context if context else ''}: {missing}")
    log_and_print(f"Column validation passed — all {len(required_columns)} required columns present")
```

> 💡 **Note:** Your helpers can call functions from `helper_functions_1/2/3.py` (like `log_and_print()`) because `helper_functions_4.py` is imported **after** the core helpers are loaded.

#### Step 2: Import in batch_processing.py

Add an import for `helper_functions_4` to `batch_processing.py`. Add it **after** the existing `helper_functions_3` import:

**In `batch_processing.py`:**
```python
from helper_functions_1 import *
from helper_functions_2 import *
from helper_functions_3 import *
from helper_functions_4 import *    # ← Add this line
```

#### Step 3: Use in Custom Function Files

Your custom function files can now call these helpers directly — no imports needed:

```python
# custom_functions/custom_osdu_ingestion.py — entity-specific logic only

def ingest_osdu_wells(metadata, spark):
    """Ingest well data from OSDU API."""
    primary_config = metadata.get('primary_config', {})
    kv_name = primary_config.get('custom_table_ingestion_function_keyvault_name')
    secret = primary_config.get('custom_table_ingestion_function_api_secret_name')
    
    # ✅ From helper_functions_4.py — shared across all OSDU custom functions
    headers = authenticate_with_keyvault(kv_name, secret)
    
    base_url = primary_config.get('custom_table_ingestion_function_api_url')
    # ✅ From helper_functions_4.py — reusable paginated API client
    records = call_paginated_api(f"{base_url}/wells", headers)
    
    df = spark.createDataFrame(records)
    
    # ✅ From helper_functions_4.py — shared validation
    validate_required_columns(df, ['well_id', 'well_name', 'status'])
    
    return df
```

### Upgrade Safety

| File | Owned By | Overwritten on Upgrade? |
|------|----------|------------------------|
| `helper_functions_1.py` | Accelerator | ✅ Yes — don't modify |
| `helper_functions_2.py` | Accelerator | ✅ Yes — don't modify |
| `helper_functions_3.py` | Accelerator | ✅ Yes — don't modify |
| `helper_functions_4.py` | **You** | ❌ No — not in the accelerator repo |
| `custom_functions/*.py` | **You** | ❌ No — not in the accelerator repo |

> ⚠️ **The import line you add to `batch_processing.py`** will need to be re-added after an accelerator upgrade that overwrites `batch_processing.py`. Track this in your [upgrade checklist](#managing-updates-from-upstream).
>
> **Note on parameter model:** `batch_processing.py` receives lightweight parameters (SQL warehouse endpoints, table ID, trigger context) instead of pre-built JSON. If you have custom integrations that invoke this module directly, you must update them to pass the new parameters. See [Core Components Reference - Batch Processing](CORE_COMPONENTS_REFERENCE.md#batch-ingestion-notebook-batch_processing.py) for the current parameter list.

### When to Use What

| Code Type | Where to Put It | Example |
|-----------|----------------|--------|
| Entity-specific logic | Custom function file (`custom_functions/*.py`) | OSDU well-specific field mapping |
| Reusable across your custom functions | `helper_functions_4.py` | Auth helpers, API clients, validation |
| Core accelerator functionality | `helper_functions_1/2/3.py` (via PR to accelerator repo) | New built-in transformation type |

---

## Modifying Core Modules

Sometimes you need to modify the core helper functions or batch processing logic. Here's how to do it effectively.

### Understanding the Core Modules

| Module | Purpose | Modification Complexity |
|--------|---------|------------------------|
| `helper_functions_1.py` | Transformations, DQ checks, merging, SCD2 | Moderate—many interdependent functions |
| `helper_functions_2.py` | DDL generation, file ingestion utilities | Lower—more isolated functions |
| `helper_functions_3.py` | Config parsing, orchestration utilities, connections, logging helpers | Higher—affects all processing |
| `batch_processing.py` | Main processing flow with integrated metadata fetching and logging (9 sections) | Highest—orchestrates everything including SQL connections, Started/Processed/Failed logging, and schema change tracking |

### Before You Modify

1. **Check if a custom function can achieve the same result** — It's faster to implement and easier to maintain

2. **Understand the function's callers** — Use `grep_search` or your IDE to find all places a function is called:
   ```
   Search for: function_name(
   ```

3. **Review the test coverage** — Check `integration_tests/` for existing tests:
   ```
   test_transformations_basic.py    — Basic transformation tests
   test_transformations_advanced.py — Complex transformation tests
   test_data_quality.py             — DQ check tests
   test_merge_operations.py         — Merge/write tests
   ```

### Making the Modification

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/my-enhancement
   ```

2. **Make your changes** to the module

3. **Update or add tests** in `integration_tests/`

4. **Run the test suite** in Databricks (see [Running the Test Suite](#running-the-test-suite))

5. **Update documentation** if you're adding new configuration options:
    - Add to [Metadata Reference](METADATA_REFERENCE.md)
    - Update [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) if it affects metadata generation
    - Update [TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md) if adding/changing transformation patterns

6. **Update validation** — If you added new config options, update `.github/skills/metadata-validation/validate_metadata_sql.py`

### Managing Updates from Upstream

If you've modified core modules and want to pull updates from the accelerator:

**Option A: Rebase your changes**
```bash
git fetch upstream
git rebase upstream/main
# Resolve conflicts in notebook files
```

**Option B: Cherry-pick specific updates**
```bash
git fetch upstream
git log upstream/main --oneline  # Find commits you want
git cherry-pick <commit-hash>
```

**Option C: Maintain a patch file**
```bash
# Create patch of your changes
git diff upstream/main -- src/databricks_batch_engine/helper_functions_1.py > my_changes.patch

# After pulling updates, reapply
git apply my_changes.patch
```

### Keeping Track of Your Modifications

Add a comment header to any modified core module:

```python
# =============================================================================
# LOCAL MODIFICATIONS
# =============================================================================
# 2026-01-02: Added support for custom_xyz transformation (John Doe)
# 2026-01-15: Fixed edge case in _merge_data for nullable keys (Jane Smith)
# =============================================================================
```

---

## Version Control with Git Tags

Track your releases and enable rollback with Git tags.

### Creating Release Tags

```bash
# Tag a release when deploying to production
git tag -a v2.1.0 -m "Release 2.1.0: Added entity resolution, fixed SCD2 edge case"
git push origin v2.1.0

# List all tags
git tag -l "v*"

# View tag details
git show v2.1.0
```

### Versioning Strategy

| Environment | Tagging Pattern | Example |
|-------------|-----------------|---------|
| Production releases | `v{major}.{minor}.{patch}` | `v2.1.0` |
| Hotfixes | `v{major}.{minor}.{patch}-hotfix.{n}` | `v2.1.0-hotfix.1` |
| Pre-release testing | `v{major}.{minor}.{patch}-rc.{n}` | `v2.2.0-rc.1` |

### Rolling Back

If a deployment causes issues:

```bash
# Check out the previous stable version
git checkout v2.0.0

# Or create a hotfix branch from the stable version
git checkout -b hotfix/v2.0.1 v2.0.0
```

### Changelog

Maintain a `CHANGELOG.md` to document changes:

```markdown
## [2.1.0] - 2026-01-02
### Added
- Entity resolution functions for MDM scenarios
- validate_pattern DQ check with 13 pattern types

### Fixed
- SCD2 single-version dimension start date handling

### Changed
- Improved error messages for surrogate key failures
```

---

## Testing Your Changes

### Validating Metadata Configuration

Run the validation script before committing:
```bash
python .github/skills/metadata-validation/validate_metadata_sql.py <path_to_metadata_sql_file>
```

### Testing Custom Functions

Test in a scratch notebook before referencing in metadata:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Create test data
test_df = spark.createDataFrame([
    (100.0, 60.0),
    (200.0, 150.0)
], ["revenue", "cost"])

# Build metadata dict (mimics what the accelerator passes)
metadata = {
    "function_config": {
        "revenue_column": "revenue",
        "cost_column": "cost",
        "output_column": "margin"
    }
}

# Test your function
result = transform_calculate_margin(test_df, metadata, spark)
result.show()
```

### Testing with a Single Entity

Run a single Table_ID without affecting other entities:

1. **Execute** `batch_processing.py` with parameters:
   - `trigger_name`: Your trigger (e.g., `TR_Daily_Load`)
   - `table_ids`: The specific Table_ID to test (e.g., `100`)
2. **Monitor** the job run to validate your custom function works correctly

> 💡 The `table_ids` parameter defaults to `0` (all entities). Pass a specific ID to test just one.

### Running the Test Suite

If you've modified core modules, run the unit tests in Databricks:

1. **Open** `integration_tests/` in your Databricks workspace
2. **Run the test suite** against your modified helper functions
3. **Review results** to ensure no regressions

> 💡 Tests are designed to run in Databricks, not locally. They depend on Spark and Databricks-specific APIs.

---

## Troubleshooting

### Adding Debug Logging

Use the built-in `log_and_print()` function for consistent logging that integrates with Databricks diagnostics:

```python
def transform_with_logging(new_data, metadata, spark):
    """Custom function with debug logging using the standard log_and_print function."""

    log_and_print(f"Metadata keys: {metadata.keys()}", "info")
    log_and_print(f"Input schema: {new_data.schema.simpleString()}", "info")
    log_and_print(f"Input row count: {new_data.count()}", "info")

    # Your logic
    result_df = new_data  # ... transformations

    log_and_print(f"Output row count: {result_df.count()}", "info")

    return result_df
```

**Log Levels:**
- `"info"` — General processing steps (default)
- `"warn"` — Non-critical issues that don't stop processing
- `"error"` — Critical failures

> **Note:** `batch_processing.py` uses `log_failure_and_cleanup()` in every `except` block to log "Failed" status to `Data_Pipeline_Logs` before re-raising exceptions. It also calls `log_data_movement()` directly for status tracking (Started/Processed) and `log_new_schema()` for schema change recording. If you modify error handling in the module, ensure these logging calls are preserved to maintain observability.

See [Monitoring and Logging](MONITORING_AND_LOGGING.md#application-logging-framework) for details on viewing logs in Log Analytics.
