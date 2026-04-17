# Custom Source Function Template
#
# Use this template when you need to ingest data from an **external source**
# (API, SDK, non-Databricks database) and return a **DataFrame** for the
# framework to process. The framework handles DQ, transformations, merge,
# and logging. If you want to persist a new watermark, return
# (DataFrame, next_watermark_value).
#
# When to use custom_source_function vs custom_staging_function:
# - Use custom_source_function when your extraction returns tabular data
#   and you want the framework to own persistence.
# - Use custom_staging_function when you need full control over file I/O
#   (file names, formats, folder structure) during staging.


# ⚠️ IMPORTANT: Do NOT put imports at the top level of custom function modules.
# Custom modules are loaded via importlib and all public symbols are injected
# into the caller's globals(). Top-level imports pollute that namespace and
# can shadow critical symbols (e.g., the 'f' alias for pyspark.sql.functions).
# Place all imports INSIDE your function body instead.
# See FAQ: "Why must custom function modules place imports inside functions?"


# all custom SOURCE functions must:

## 1. have below parameters: (metadata, spark)

## 2. return a spark DataFrame
##    — OR a tuple of (DataFrame, next_watermark_value)

## 3. NOT write files — the framework handles persistence

## Note: the function can only reference other functions defined in the same module

def custom_source_function(metadata, spark):
    # =========================================================================
    # AVAILABLE METADATA KEYS
    # =========================================================================
    # The metadata dictionary contains all pipeline parameters bundled by
    # the batch_processing orchestrator.
    primary_config = metadata.get('primary_config', {})
    watermark_value = metadata.get('watermark_value', '')

    # Optional metadata you can access when needed:
    # datastore_config = metadata.get('datastore_config', [])
    # advanced_config = metadata.get('advanced_config', {})
    # event_payload = metadata.get('event_payload', {})
    # orchestration_metadata = metadata.get('orchestration_metadata', {})

    # =========================================================================
    # ACCESSING CUSTOM CONFIG VALUES
    # =========================================================================
    # Custom config values use the FULL key with category prefix:
    # Metadata: (300, 'source_details', 'custom_source_function_api_endpoint', 'https://...')
    # Access:   primary_config.get('source_details_custom_source_function_api_endpoint')
    api_endpoint = primary_config.get('source_details_custom_source_function_api_endpoint', '')
    page_size = primary_config.get('source_details_custom_source_function_page_size', '100')
    return_mode = primary_config.get('source_details_custom_source_function_return_mode', 'dataframe')

    # =========================================================================
    # USING _get_datastore_config() TO LOOK UP DATASTORE PROPERTIES
    # =========================================================================
    # The _get_datastore_config() helper function is available from helper_functions_3.py.
    # Available datastore names: 'bronze', 'silver', 'gold', 'metadata_lakehouse', 'metadata_warehouse'
    # Available properties: 'Datastore_Name', 'Datastore_Type', 'Datastore_ID',
    #                       'Workspace_ID', 'Workspace_Name', 'Medallion_Layer',
    #                       'Endpoint', 'Connection_ID', 'Catalog_Name', 'Schema_Name'
    #
    # source_datastore_name = primary_config.get('source_details_datastore_name', '')
    # source_connection_id = _get_datastore_config(datastore_config, source_datastore_name, 'Connection_ID')

    # =========================================================================
    # YOUR EXTRACTION LOGIC
    # =========================================================================
    # Replace this sample with your actual API/SDK/database extraction logic.
    records = [
        {
            'id': 1,
            'value': 'alpha',
            'updated_at': '2024-06-01T00:00:00',
            'api_endpoint': api_endpoint or 'sample',
            'page_size': page_size
        },
        {
            'id': 2,
            'value': 'beta',
            'updated_at': '2024-06-02T00:00:00',
            'api_endpoint': api_endpoint or 'sample',
            'page_size': page_size
        }
    ]

    df = spark.createDataFrame(records)

    # =========================================================================
    # RETURN OPTIONS
    # =========================================================================

    # OPTION A: Return just the DataFrame.
    # Use this for full-refresh or stateless extraction logic where
    # no new watermark should be persisted.
    if return_mode == 'dataframe':
        return df

    # OPTION B: Return a tuple of (DataFrame, next_watermark_value).
    # Use this when the function knows the exact next watermark
    # (e.g., an API cursor, page token, or explicit timestamp).
    next_wm = records[-1]['updated_at'] if records else watermark_value
    return df, next_wm


# =============================================================================
# KEY DIFFERENCES: Custom Source vs Custom Staging vs Custom Table Ingestion
# =============================================================================
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ CUSTOM SOURCE (THIS PATTERN)                                           │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ Parameters:  (metadata, spark)                                          │
# │ Returns:     DataFrame  — OR —  (DataFrame, next_watermark_value)       │
# │ Orchestrator: batch_processing                                          │
# │ Use case:    External data → DataFrame → framework handles the rest    │
# │ Watermark:   Function receives watermark_value; must return next value │
# │ No file I/O: Framework owns persistence (merge/overwrite to table)     │
# └─────────────────────────────────────────────────────────────────────────┘
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ CUSTOM STAGING                                                          │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ Parameters:  (metadata, spark)                                          │
# │ Returns:     dict {'rows_copied': int, 'next_watermark_value': str}     │
# │ Use case:    Full control — function handles extract AND file write     │
# │ Watermark:   Function owns both extraction progress and watermark       │
# └─────────────────────────────────────────────────────────────────────────┘
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ CUSTOM TABLE INGESTION                                                  │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ Parameters:  (metadata, spark)                                          │
# │ Returns:     DataFrame                                                  │
# │ Use case:    Read from internal tables with custom logic                 │
# │ Watermark:   Framework injects watermark_filter + watermark_column_name │
# └─────────────────────────────────────────────────────────────────────────┘
#
# DECISION GUIDE:
# - Pulling from an API/SDK and want framework to handle everything? → custom_source_function
# - Need to write specific files to a staging folder?                → custom_staging_function
# - Reading from an existing Delta table with custom SQL?            → custom_table_ingestion_function
