# ⚠️ IMPORTANT: Do NOT put imports at the top level of custom function modules.
# Custom modules are loaded via importlib and all public symbols are injected
# into the caller's globals(). Top-level imports pollute that namespace and
# can shadow critical symbols (e.g., the 'f' alias for pyspark.sql.functions).
# Place all imports INSIDE your function body instead.
# See FAQ: "Why must custom function modules place imports inside functions?"


# all custom TABLE ingestion functions must:
## 1. have below parameters: (metadata, spark)
## 2. return a spark dataframe
## Note: the function can only reference other functions defined in the same module
def custom_table_ingestion_function(metadata, spark):
    # =========================================================================
    # IMPORTS — must be INSIDE the function (not at top level)
    # =========================================================================
    from pyspark.sql import functions as f

    # =========================================================================
    # AVAILABLE METADATA KEYS
    # =========================================================================
    # Access Orchestration metadata
    orchestration_metadata = metadata.get('orchestration_metadata')

    # Access Primary Configuration metadata
    # Keys include the category prefix, e.g., 'source_details_custom_table_ingestion_function_base_url'
    primary_config = metadata.get('primary_config')

    # Access Advanced Configuration metadata
    advanced_config = metadata.get('advanced_config')

    # Access Datastore Configuration
    # This is a list of dicts from the Datastore_Configuration table.
    # Each dict has: Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID,
    #   Workspace_Name, Medallion_Layer, Endpoint, Connection_ID, Catalog_Name, Schema_Name
    datastore_config = metadata.get('datastore_config')

    # Access Event Payload (for event-driven scenarios)
    event_payload = metadata.get('event_payload')

    # Watermark keys (added directly to metadata for table ingestion functions)
    watermark_filter = metadata.get('watermark_filter')  # Pre-built filter expression
    watermark_column_name = metadata.get('watermark_column_name')  # Column name for watermark
    watermark_value = metadata.get('watermark_value')  # Last processed watermark value

    # =========================================================================
    # ACCESSING CUSTOM CONFIG VALUES
    # =========================================================================
    # Custom config values use the FULL key with category prefix:
    # Metadata: (100, 'source_details', 'custom_table_ingestion_function_base_url', 'https://...')
    # Access:   primary_config.get('source_details_custom_table_ingestion_function_base_url')
    # my_custom_value = primary_config.get('source_details_my_custom_setting')

    # =========================================================================
    # USING _get_datastore_config() TO LOOK UP DATASTORE PROPERTIES
    # =========================================================================
    # The _get_datastore_config() helper function is available from helper_functions_3.py.
    # Use it to look up properties by datastore name.
    #
    # Signature: _get_datastore_config(datastore_config, datastore_name, property_name)
    #
    # Available datastore names: 'bronze', 'silver', 'gold', 'metadata_lakehouse', 'metadata_warehouse'
    # Available properties: 'Datastore_Name', 'Datastore_Type', 'Datastore_ID', 'Workspace_ID',
    #                       'Workspace_Name', 'Medallion_Layer', 'Endpoint', 'Connection_ID',
    #                       'Catalog_Name', 'Schema_Name'
    #
    # Examples:
    silver_catalog = _get_datastore_config(datastore_config, 'silver', 'Catalog_Name')
    silver_schema = _get_datastore_config(datastore_config, 'silver', 'Schema_Name')

    # execute custom spark sql and/or pyspark ingestion logic
    df = spark.sql(f"SELECT * FROM `{silver_catalog}`.`{silver_schema}`.Oracle_Sales")

    #### OPTION A ###
    ## USING ALREADY CREATED WATERMARK FILTER
    applying_watermark_filter_log_info = f"Applying watermark filter: {watermark_filter}"
    logger.info(applying_watermark_filter_log_info)
    print(applying_watermark_filter_log_info)

    df = df.filter(watermark_filter)

    #### OPTION B ###
    ## USING WATERMARK COLUMN NAME AND WATERMARK VALUE
    df = df.filter(f"{watermark_column_name} > TIMESTAMP '{watermark_value}'")

    # return data
    return df
