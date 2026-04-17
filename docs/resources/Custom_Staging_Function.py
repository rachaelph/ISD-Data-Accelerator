# ⚠️ IMPORTANT: Do NOT put imports at the top level of custom function modules.
# Custom modules are loaded via importlib and all public symbols are injected
# into the caller's globals(). Top-level imports pollute that namespace and
# can shadow critical symbols (e.g., the 'f' alias for pyspark.sql.functions).
# Place all imports INSIDE your function body instead.
# See FAQ: "Why must custom function modules place imports inside functions?"


# all custom STAGING functions must:

## 1. have below parameters: (metadata, spark)

## 2. return a dictionary with keys: 'rows_copied' (int) and 'next_watermark_value' (str)

## 3. write files directly into the provided timestamped staging folder

## 4. do NOT create subfolders under that folder; direct child files only

## Note: the function can only reference other functions defined in the same module

def custom_staging_function(metadata, spark):
    # =========================================================================
    # IMPORTS — must be INSIDE the function (not at top level)
    # =========================================================================
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    # =========================================================================
    # AVAILABLE METADATA KEYS
    # =========================================================================
    # The metadata dictionary contains all pipeline parameters bundled by
    # the batch_processing orchestrator.
    watermark_value = metadata.get('watermark_value')  # Previous high watermark
    primary_config = metadata.get('primary_config')
    staging_datastore_config = metadata['staging_datastore_config']
    orchestration_metadata = metadata.get('orchestration_metadata')
    target_folderpath = metadata.get('target_folderpath_w_ingestion_type')

    # =========================================================================
    # LOOK UP THE TARGET STAGING LOCATION (Unity Catalog Volumes)
    # =========================================================================
    # The staging path is resolved to a Volume path under /Volumes/{catalog}/{schema}/{volume}/...
    # Volume paths are local filesystem paths — no mounting is needed.
    # Write direct child files only. If this folder stays empty, the framework
    # treats the run as no new data.
    table_id = orchestration_metadata.get('Table_ID', 'unknown')

    # resolve_custom_staging_output_abfss_path() builds the full Volume path
    # from staging_datastore_config and target_folderpath
    target_volume_path = resolve_custom_staging_output_abfss_path(
        staging_datastore_config, target_folderpath
    )

    # Replace this sample payload with calls to your API, database, or service.
    records = [
        {'id': 1, 'value': 'a', 'updated_at': '2024-01-01T00:00:00'},
        {'id': 2, 'value': 'b', 'updated_at': '2024-01-02T00:00:00'}
    ]

    # Choose one pattern below in a real implementation. This template keeps the
    # output type consistent by showing JSON for both Spark and Python writes.
    # For Spark single-file writes via _write_spark_file_with_rename(), supported
    # file_format values are: 'csv', 'txt', 'parquet', 'json', 'jsonl', 'ndjson'.

    # =========================================================================
    # EXAMPLE 1: SPARK WRITE TO A SINGLE JSON FILE
    # =========================================================================
    # Raw Spark writes create folders. Use _write_spark_file_with_rename() when
    # you need a single named file that downstream staging logic can discover.
    spark_df = spark.createDataFrame(records)
    spark_output_path = f"{target_volume_path}/api_response_spark.json"
    _write_spark_file_with_rename(
        data_to_merge=spark_df,
        output_path=spark_output_path,
        file_format='json'
    )

    # =========================================================================
    # EXAMPLE 2: STANDARD PYTHON FILE I/O (Volume paths work directly)
    # =========================================================================
    # Volume paths are local filesystem paths — use standard Python I/O.
    payload = {
        'source': primary_config.get('source_details_custom_staging_function_api_endpoint', 'sample'),
        'extracted_at_utc': datetime.now(timezone.utc).isoformat(),
        'record_count': len(records),
        'records': records
    }

    output_path = f"{target_volume_path}/api_response_python.json"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)

    next_watermark_value = max(record['updated_at'] for record in records) if records else watermark_value

    return {
        'rows_copied': len(records),
        'next_watermark_value': next_watermark_value
    }


# =============================================================================
# KEY DIFFERENCES: Custom Staging vs Custom Table Ingestion
# =============================================================================
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ CUSTOM STAGING (THIS PATTERN)                                           │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ Parameters:  (metadata, spark)                                          │
# │ Returns:     dict {'rows_copied': int, 'next_watermark_value': str}     │
# │ Orchestrator: batch_processing                                          │
# │ Trigger:     custom_staging_function metadata with Processing_Method=batch │
# │ Use case:    Full control — function handles both extract AND write     │
# └─────────────────────────────────────────────────────────────────────────┘
#
# =============================================================================
# OUTPUT CONTRACT
# =============================================================================
# - The framework provides one timestamped staging folder per run
# - Write direct child files only in that folder
# - Zip files are allowed
# - Subfolders are not supported
# - An empty folder is treated as no new data
#
# =============================================================================
# WHEN TO USE CUSTOM STAGING vs CUSTOM TABLE INGESTION
# =============================================================================
# Use Custom Staging when you need:
# - Full control over file naming inside the provided staging folder
# - Non-standard extraction logic before files exist
# - Direct file writes as part of the custom logic
#
# Use Custom Table Ingestion when you need:
# - To return a DataFrame and let the framework handle writes
# - To read from an existing source table/query inside batch processing
# - To reuse the standard write path with custom extraction logic
