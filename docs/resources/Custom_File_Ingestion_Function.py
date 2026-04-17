# ⚠️ IMPORTANT: Do NOT put imports at the top level of custom function modules.
# Custom modules are loaded via importlib and all public symbols are injected
# into the caller's globals(). Top-level imports pollute that namespace and
# can shadow critical symbols (e.g., the 'f' alias for pyspark.sql.functions).
# Place all imports INSIDE your function body instead.
# See FAQ: "Why must custom function modules place imports inside functions?"


# all custom FILE ingestion functions must:
## have below parameters: (file_paths, all_metadata, spark)
## return new_data as a spark dataframe
## file_paths: list of file paths to be processed (Volume paths, e.g. /Volumes/catalog/schema/volume/...)
## all_metadata: dictionary with all configuration details
def custom_file_ingestion_function(file_paths, all_metadata, spark):
    # =========================================================================
    # IMPORTS — must be INSIDE the function (not at top level)
    # =========================================================================
    from pyspark.sql import functions as f
    import pandas as pd

    # =========================================================================
    # AVAILABLE METADATA KEYS (via all_metadata parameter)
    # =========================================================================
    # Access Orchestration metadata
    orchestration_metadata = all_metadata.get('orchestration_metadata')

    # Access Primary Configuration metadata
    # Keys include the category prefix, e.g., 'source_details_custom_file_ingestion_function_base_url'
    primary_config = all_metadata.get('primary_config')

    # =========================================================================
    # ACCESSING CUSTOM CONFIG VALUES
    # =========================================================================
    # Custom config values use the FULL key with category prefix:
    # Metadata: (100, 'source_details', 'custom_file_ingestion_function_base_url', 'https://...')
    # Access:   primary_config.get('source_details_custom_file_ingestion_function_base_url')
    # my_custom_value = primary_config.get('source_details_my_custom_setting')

    # Access Advanced Configuration metadata
    advanced_config = all_metadata.get('advanced_config')

    # Access Datastore Configuration
    # This is a list of dicts from the Datastore_Configuration table.
    # Each dict has: Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID,
    #   Workspace_Name, Medallion_Layer, Endpoint, Connection_ID, Catalog_Name, Schema_Name
    datastore_config = all_metadata.get('datastore_config')

    # Access Event Payload (for event-driven scenarios)
    event_payload = all_metadata.get('event_payload')

    # =========================================================================
    # HELPER FUNCTIONS (available from helper_functions_3.py via globals)
    # =========================================================================
    # The following helper functions are available in scope:
    #
    # 1. _get_datastore_config(datastore_config, datastore_name, property_name)
    #    Look up datastore properties by name.
    #    Available names: 'bronze', 'silver', 'gold', 'metadata_lakehouse', 'metadata_warehouse'
    #    Available properties: 'Datastore_ID', 'Workspace_ID', 'Workspace_Name', 'Medallion_Layer',
    #                          'Endpoint', 'Connection_ID', 'Datastore_Type', 'Catalog_Name', 'Schema_Name'
    #
    # 2. _resolve_volume_root(datastore_config, datastore_name)
    #    Get the volume root path for a datastore.
    #    Returns: /Volumes/{catalog}/{schema}/{datastore} (or Endpoint if configured)
    #
    # 3. _resolve_volume_path(volume_root, configured_path)
    #    Construct a full volume path from root + relative path.
    #    Returns: full path string

    # =========================================================================
    # FILE PATHS — Unity Catalog Volumes
    # =========================================================================
    # In Databricks, file_paths are Volume paths (e.g., /Volumes/catalog/schema/volume/...).
    # Volume paths are LOCAL filesystem paths — no mounting is needed.
    # Standard Python open(), pandas, Spark readers, and any library that
    # uses os.path or pathlib all work directly with Volume paths.

    # =========================================================================
    # EXAMPLE IMPLEMENTATION: Reading files from Volume paths
    # =========================================================================
    all_data = []
    for file_path in file_paths:
        # Read file content using standard Python — Volume paths work directly
        with open(file_path, 'r', encoding='utf-8') as file_handle:
            lines = file_handle.readlines()

        # Process into records (customize this for your format)
        for line in lines:
            all_data.append({'value': line.strip()})

    # Convert to Spark DataFrame for return
    if all_data:
        new_data = spark.createDataFrame(all_data)
    else:
        # Return empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField('value', StringType(), True)])
        new_data = spark.createDataFrame([], schema)

    return new_data


# =============================================================================
# ALTERNATIVE: Spark-native approach
# =============================================================================
# Use this when your data format IS supported by Spark but needs custom parsing.
# This is simpler and more performant when Spark can read the files directly.

def custom_file_ingestion_spark_native(file_paths, all_metadata, spark):
    """
    Use Spark's native file reading — works directly with Volume paths.
    Best for: CSV, JSON, Parquet, text files, and formats Spark supports.
    """
    new_data = None
    for file_path in file_paths:
        # Spark reads Volume paths directly
        df = spark.read.text(file_path)
        if new_data is None:
            new_data = df
        else:
            new_data = new_data.unionByName(df, allowMissingColumns=True)

    return new_data


# =============================================================================
# DECISION GUIDE: Volume paths work everywhere
# =============================================================================
#
# In Databricks with Unity Catalog Volumes, ALL libraries can use Volume
# paths directly — no mounting or path conversion is needed:
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ ALL paths work with /Volumes/{catalog}/{schema}/{volume}/...            │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ ✅ Spark readers      - spark.read.csv(), .json(), .parquet(), .text()  │
# │ ✅ Pandas             - pd.read_csv(), pd.read_parquet(), pd.read_json()│
# │ ✅ Python open()      - open(path, 'r')                                 │
# │ ✅ zipfile.ZipFile()  - Works directly with Volume paths                │
# │ ✅ PIL.Image.open()   - Works directly with Volume paths                │
# │ ✅ PyPDF2             - Works directly with Volume paths                │
# │ ✅ openpyxl           - Works directly with Volume paths                │
# │ ✅ xml.etree          - Works directly with Volume paths                │
# │ ✅ os.path / pathlib  - Works directly with Volume paths                │
# │ ✅ dbutils.fs         - .ls(), .head(), .cp(), .rm()                    │
# └─────────────────────────────────────────────────────────────────────────┘
#
# =============================================================================
# AVAILABLE HELPER FUNCTIONS (from helper_functions_3.py)
# =============================================================================
#
# _get_datastore_config(datastore_config, datastore_name, property_name)
#     → Look up datastore properties
#
# _resolve_volume_root(datastore_config, datastore_name)
#     → Returns: /Volumes/{catalog}/{schema}/{datastore}
#
# _resolve_volume_path(volume_root, configured_path)
#     → Returns: full volume path
