# ⚠️ IMPORTANT: Do NOT put imports at the top level of custom function modules.
# Custom modules are loaded via importlib and all public symbols are injected
# into the caller's globals(). Top-level imports pollute that namespace and
# can shadow critical symbols (e.g., the 'f' alias for pyspark.sql.functions).
# Place all imports INSIDE your function body instead.
# See FAQ: "Why must custom function modules place imports inside functions?"


def example_support_transformation_function(new_data):
    new_data['deleted'] = 1

    return new_data


# all custom transformation functions that are called directly from the metadata driven pipeline must:
## have below parameters even if they don't need all of them
## return new_data as a spark dataframe
# the main function can only reference other functions defined in the same module
def example_transformation_function(new_data, metadata, spark):
    # =========================================================================
    # IMPORTS — must be INSIDE the function (not at top level)
    # =========================================================================
    from pyspark.sql import functions as f
    import pandas as pd

    # convert incoming spark dataframe to pandas dataframe
    new_data = new_data.toPandas()

    # insert custom pandas logic
    # example below to add a new column 'deleted' with value 1
    new_data = example_support_transformation_function(new_data)

    # =========================================================================
    # AVAILABLE METADATA KEYS
    # =========================================================================
    # Access Orchestration metadata
    orchestration_metadata = metadata.get('orchestration_metadata')

    # Access Primary Configuration metadata
    primary_config = metadata.get('primary_config')

    # Access Datastore Configuration
    # This is a list of dicts from the Datastore_Configuration table.
    # Each dict has: Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID,
    #   Workspace_Name, Medallion_Layer, Endpoint, Connection_ID, Catalog_Name, Schema_Name
    datastore_config = metadata.get('datastore_config')

    # Access Event Payload (for event-driven scenarios)
    event_payload = metadata.get('event_payload')

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
    gold_catalog = _get_datastore_config(datastore_config, 'gold', 'Catalog_Name')

    # =========================================================================
    # ⭐ RECOMMENDED: Use function_config for your custom attributes
    # =========================================================================
    # function_config contains the attributes from YOUR transformation step as
    # simple key-value pairs. This is the easiest way to access custom config.
    #
    # EXAMPLE SQL METADATA (in your metadata SQL file):
    # -----------------------------------------------------------------------
    # INSERT INTO metadata.Data_Pipeline_Metadata_Advanced_Configuration
    # (Table_ID, Configuration_Category, Configuration_Name, Configuration_Order, Attribute_Name, Attribute_Value)
    # VALUES
    # (101, 'data_transformation_steps', 'custom_transformation_function', 1, 'function_name', 'example_transformation_function'),
    # (101, 'data_transformation_steps', 'custom_transformation_function', 1, 'module_name', 'custom_my_transform'),
    # (101, 'data_transformation_steps', 'custom_transformation_function', 1, 'my_custom_attribute', 'some_value'),
    # (101, 'data_transformation_steps', 'custom_transformation_function', 1, 'another_param', '100')
    # -----------------------------------------------------------------------
    # The above SQL creates function_config = {
    #     'function_name': 'example_transformation_function',
    #     'module_name': 'custom_my_transform',
    #     'my_custom_attribute': 'some_value',
    #     'another_param': '100'
    # }
    #
    function_config = metadata.get('function_config')
    my_custom_param = function_config.get('my_custom_attribute')  # Returns 'some_value'
    another_param = function_config.get('another_param')  # Returns '100' (as string)

    # =========================================================================
    # ⚠️ AVOID: advanced_config is difficult to parse
    # =========================================================================
    # advanced_config is a LIST of dictionaries containing ALL transformation steps.
    # It requires complex iteration and filtering to find your step's config.
    # Only use if you need to access OTHER transformation steps' configuration.
    # advanced_config = metadata.get('advanced_config')  # Not recommended

    if orchestration_metadata.get('Target_Entity') == 'dbo.sales':
        new_data['Sales'] = 1

    if primary_config.get('target_details_mark_as_deleted') == 1:
        new_data['deleted'] = 1

    # convert pandas dataframe back to spark dataframe and return
    new_data = spark.createDataFrame(new_data)

    return new_data
