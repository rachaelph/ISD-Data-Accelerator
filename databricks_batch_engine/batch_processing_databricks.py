# Databricks notebook source

# COMMAND ----------

# MAGIC %run ./runtime_databricks

# COMMAND ----------

# MAGIC %run ./helper_functions_3

# COMMAND ----------

# MAGIC %run ./helper_functions_2

# COMMAND ----------

# MAGIC %run ./helper_functions_1

# COMMAND ----------

from dataclasses import asdict

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook Parameters

# COMMAND ----------

dbutils.widgets.text("metadata_catalog_name", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("trigger_name", "")
dbutils.widgets.text("trigger_step", "")
dbutils.widgets.text("trigger_execution_id", "")
dbutils.widgets.text("trigger_execution_start_time", "")
dbutils.widgets.text("target_datastore", "")
dbutils.widgets.text("target_entity", "")

from datetime import datetime, timezone

metadata_catalog_name = dbutils.widgets.get("metadata_catalog_name")
table_id = dbutils.widgets.get("table_id")
trigger_name = dbutils.widgets.get("trigger_name")
trigger_step = dbutils.widgets.get("trigger_step")
trigger_execution_id = dbutils.widgets.get("trigger_execution_id")
trigger_execution_time = dbutils.widgets.get("trigger_execution_start_time")
start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
target_datastore = dbutils.widgets.get("target_datastore")
target_entity = dbutils.widgets.get("target_entity")
table_ddl = "[]"
event_payload = ""
folder_path_from_trigger = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to Metadata SQL, Fetch Metadata, and Log "Started"

# COMMAND ----------

clear_run_log_buffer()

metadata_conn = None
logging_conn = None
log_entry = None

try:
    set_current_step(2, "Connect & Fetch Metadata")

    # Cast pipeline string parameters to correct types
    table_id = int(table_id)
    trigger_step = int(trigger_step) if trigger_step else None

    # ===========================================================================================
    # WAREHOUSE CONNECTIONS
    # ===========================================================================================
    metadata_conn, logging_conn = open_startup_warehouse_connections(
        metadata_catalog_name = metadata_catalog_name
    )

    # ===========================================================================================
    # BUILD DATABRICKS RUN URL
    # ===========================================================================================
    run_monitor_url = build_databricks_run_url()

    # ===========================================================================================
    # BUILD LOG ENTRY AND LOG "STARTED"
    # ===========================================================================================
    log_entry = DataMovementLogEntry(
        log_id=str(uuid.uuid4()),
        table_id=table_id,
        target_datastore=target_datastore,
        target_entity=target_entity,
        event_start_time=start_time,
        run_monitor_url=run_monitor_url,
        trigger_name=trigger_name,
        trigger_step=trigger_step,
        trigger_id=trigger_execution_id,
        trigger_time=trigger_execution_time,
        logging_catalog_name=metadata_catalog_name,
    )
    log_data_movement(logging_conn, log_entry, status='Started')

    # ===========================================================================================
    # FETCH METADATA FROM WAREHOUSE (replaces pipeline Metadata Script activity)
    # ===========================================================================================
    raw_metadata = fetch_metadata_from_warehouse(
        conn=metadata_conn,
        table_id=table_id,
        target_datastore=target_datastore
    )

    # ===========================================================================================
    # FETCH LOGGING DETAILS FROM WAREHOUSE (replaces pipeline Logging Script activity)
    # ===========================================================================================
    raw_logging = fetch_logging_details(
        conn=logging_conn,
        table_id=table_id,
        target_datastore=target_datastore
    )

    # Extract fetched data into working variables
    orchestration_metadata = raw_metadata["orchestration_metadata"]
    primary_config = raw_metadata["primary_config"]
    advanced_config = raw_metadata["advanced_config"]
    datastore_config = raw_metadata["all_datastore_config"]
    latest_schema_details = raw_logging["schema_details"]
    watermark_value = raw_logging["watermark_details"].get("watermark_value", "")
    full_reload = raw_logging["watermark_details"].get("full_reload", "")

    # table_ddl is still a JSON string from pipeline parameter
    table_ddl = json.loads(table_ddl)

    log_and_print(f"Metadata fetched for Table_ID={table_id}, watermark_value={watermark_value}, full_reload={full_reload}")

except Exception as e:
    handle_startup_failure(logging_conn, log_entry, e, step="Step 2: Connect & Fetch Metadata")
    raise

finally:
    close_startup_warehouse_connections(metadata_conn, logging_conn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse Configuration

# COMMAND ----------

try:
    set_current_step(3, "Parse Configuration")
    # ===========================================================================================
    # PARSE CONFIGURATION (using dicts fetched from warehouse — no JSON parsing needed)
    # ===========================================================================================

    # Clean configuration data
    advanced_config = clean_advanced_config(advanced_config)
    latest_schema_details = normalize_schema_details(latest_schema_details)

    # Extract schema tracking information for change detection
    schema_tracking = extract_schema_tracking_info(latest_schema_details)

    # Parse source configuration
    source_config = parse_source_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config,
        datastore_config = datastore_config,
        folder_path_from_trigger = folder_path_from_trigger
    )

    # Parse target configuration
    target_config = parse_target_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config,
        datastore_config = datastore_config
    )

    # Parse file ingestion paths
    custom_paths = parse_file_ingestion_paths(
        datastore_config = datastore_config,
        source_datastore_name = source_config.source_datastore_name,
        target_catalog_name = target_config.target_catalog_name,
        target_schema_name = target_config.target_schema_name,
        table_id = source_config.table_id,
        wildcard_folder_path = source_config.wildcard_folder_path,
        primary_config = primary_config
    )

    # Parse watermark configuration
    watermark_config = parse_watermark_configuration(
        primary_config = primary_config,
        default_merge_type = target_config.default_merge_type,
        default_watermark_column_name = target_config.default_watermark_column_name,
        staging_folder_path = source_config.staging_folder_path,
        using_source_folder_path = source_config.using_source_folder_path,
        watermark_value = watermark_value
    )

    # Local alias — modified by determine_first_run_and_table_existence()
    watermark_value = watermark_config.watermark_value

    # Extract lineage information for logging
    lineage_info = extract_lineage_information(
        source_config = source_config,
        target_config = target_config,
        datastore_config = datastore_config,
        merge_type = watermark_config.merge_type
    )

    # Populate lineage fields on the log entry now that we have them
    log_entry.source_medallion_layer = lineage_info.source_medallion_layer
    log_entry.source_type = lineage_info.source_type
    log_entry.target_medallion_layer = lineage_info.target_medallion_layer
    log_entry.target_type = lineage_info.target_type

    # Construct all_metadata dictionary
    all_metadata = {
        "orchestration_metadata": orchestration_metadata,
        "primary_config": primary_config,
        "advanced_config": advanced_config,
        "datastore_config": datastore_config,
        "event_payload": event_payload
    }

    # Parse advanced processing configuration
    advanced_processing_config = parse_advanced_processing_configuration(
        primary_config = primary_config
    )

    # Local alias — modified by cleanse_column_names()
    liquid_clustering_columns = advanced_processing_config.liquid_clustering_columns

    # Parse primary key configuration
    primary_key_config = parse_primary_key_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config
    )

    # Local alias — modified by cleanse_column_names()
    primary_keys = primary_key_config.primary_keys

    # Parse advanced configuration steps
    advanced_steps_config = parse_advanced_configuration_steps(
        advanced_config = advanced_config
    )

    # Parse dimension table configuration
    dimension_config = parse_dimension_table_configuration(
        primary_config = primary_config,
        advanced_config = advanced_config
    )

    file_config = extract_file_configuration(
        primary_config = primary_config,
        fail_on_new_schema = advanced_processing_config.fail_on_new_schema
    )

    source_config_dict = asdict(source_config)
    target_config_dict = asdict(target_config)
    watermark_config_dict = asdict(watermark_config)
    custom_paths_dict = asdict(custom_paths)
    file_config_dict = asdict(file_config)
    advanced_processing_config_dict = asdict(advanced_processing_config)
    dimension_config_dict = asdict(dimension_config)
    lineage_info_dict = asdict(lineage_info)

    log_and_print("Configuration parsed successfully.")

except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 3: Parse Configuration")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Configure Spark Session & Initialize Tables

# COMMAND ----------

try:
    set_current_step(4, "Spark Configuration")
    performance_config = parse_performance_configuration(
        primary_config = primary_config,
        target_datastore_medallion_name = target_config.target_datastore_medallion_name
    )

    spark_config = parse_spark_configuration(
        primary_config = primary_config
    )

    layer_config = apply_spark_configurations(
        spark_timestamp_rebase_mode_write = spark_config.spark_timestamp_rebase_mode_write,
        spark_timestamp_rebase_mode_read = spark_config.spark_timestamp_rebase_mode_read,
        use_spark_config_for_lakehouse = performance_config.use_spark_config_for_lakehouse
    )

    create_date_dimension(
        fact_table_data_load = dimension_config.fact_table_data_load,
        target_catalog_name = target_config.target_catalog_name,
        target_schema_name = target_config.target_schema_name,
        date_table_name = 'dim_date',
        date_dimension_table_key_column_name = "date_sk"
    )

except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 4: Spark Configuration")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Full Reload Check & Data Ingestion

# COMMAND ----------

try:
    set_current_step(5, "Data Ingestion")
    handle_custom_staging(
        logging_conn = logging_conn,
        batch_log_entry = log_entry,
        source_config = source_config,
        primary_config = primary_config,
        datastore_config = datastore_config,
        orchestration_metadata = orchestration_metadata,
        spark_session = spark,
        session_globals = globals()
    )

    first_run, target_table_exists, watermark_value = determine_first_run_and_table_existence(
        target_table_name = target_config.target_table_name,
        full_reload = full_reload,
        watermark_value = watermark_value,
        target_destination = target_config.target_destination,
        unity_catalog_table_output = target_config.unity_catalog_table_output
    )

    new_data, new_watermark_value, source_details = route_to_ingestion_method(
        source_config = source_config_dict,
        watermark_config = watermark_config_dict,
        custom_paths = custom_paths_dict,
        file_config = file_config_dict,
        all_metadata = all_metadata,
        folder_path_from_trigger = folder_path_from_trigger,
        first_run = first_run,
        lineage_info = lineage_info_dict
    )

except NoDataFoundError as e:
    log_no_data_and_cleanup(logging_conn, log_entry, e)
    dbutils.notebook.exit("")
except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 5: Data Ingestion")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Column Standardization & Schema Alignment

# COMMAND ----------

try:
    set_current_step(6, "Schema Alignment")
    new_data, primary_keys, liquid_clustering_columns = cleanse_column_names(
        df = new_data,
        primary_keys = primary_keys,
        liquid_clustering_columns = liquid_clustering_columns,
        advanced_processing_config = advanced_processing_config_dict
    )

    new_data = add_timestamp_metadata_columns(
        df = new_data,
        target_table_name = target_config.target_table_name
    )

    create_schema_if_not_exists(
        target_destination = target_config.target_destination,
        unity_catalog_table_output = target_config.unity_catalog_table_output
    )

    column_definitions = build_source_column_definitions(
        table_ddl = table_ddl,
        enforce_not_null = target_config.enforce_not_null,
        source_category = source_config.source_category,
        advanced_processing_config = advanced_processing_config_dict
    )

    new_data = apply_source_type_casts(
        df = new_data,
        column_definitions = column_definitions
    )

except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 6: Schema Alignment")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Cleansing, Transformations & Data Quality

# COMMAND ----------

try:
    set_current_step(7, "Data Quality & Transformations")
    new_data = data_cleansing(
        df = new_data,
        trim_data_in_string_columns = advanced_processing_config.trim_data_in_string_columns,
        replace_blank_with_null_in_string_columns = advanced_processing_config.replace_blank_with_null_in_string_columns
    )

    new_data = transformation_functions(
        df = new_data,
        data_transformation_steps = advanced_steps_config.data_transformation_steps,
        first_run = first_run,
        target_table_name = target_config.target_table_name,
        primary_keys = primary_keys,
        dimension_config = dimension_config_dict,
        watermark_config = watermark_config_dict,
        merge_type = watermark_config.merge_type,
        all_metadata = all_metadata,
        lineage_info = lineage_info_dict
    )

    new_data = reorder_columns_for_output(new_data = new_data)

    dq_warnings, dq_force_failures, new_data, quarantined_data = execute_data_quality_checks(
        new_data = new_data,
        data_quality_steps = advanced_steps_config.data_quality_steps,
        if_duplicate_primary_keys = advanced_processing_config.if_duplicate_primary_keys,
        primary_keys = primary_keys,
        target_table_name = target_config.target_table_name,
        datastore_config = datastore_config,
        merge_in_batches_with_columns = watermark_config.merge_in_batches_with_columns
    )

    new_schema, new_schema_details, new_data_schema_hash, schema_change_summary, schema_type_updates_summary, column_types_that_changed = analyze_schema_changes(
        new_data = new_data,
        last_schema_id = schema_tracking.last_schema_id,
        last_schema_details = schema_tracking.last_schema_details
    )

    dq_warnings, dq_force_failures = handle_schema_change_failures(
        new_schema = new_schema,
        fail_on_new_schema = advanced_processing_config.fail_on_new_schema,
        first_run = first_run,
        last_schema_id = schema_tracking.last_schema_id,
        schema_change_summary = schema_change_summary,
        fail_on_column_data_type_change = advanced_processing_config.fail_on_column_data_type_change,
        column_types_that_changed = column_types_that_changed,
        schema_type_updates_summary = schema_type_updates_summary,
        dq_warnings = dq_warnings,
        dq_force_failures = dq_force_failures
    )

    # Finalize processing — raises DataQualityFailureError if dq_force_failures exist
    new_data = finalize_processing(
        dq_force_failures = dq_force_failures,
        source_details = source_details,
        dq_warnings = dq_warnings,
        new_schema = new_schema,
        new_data = new_data,
        target_datastore_medallion_name = target_config.target_datastore_medallion_name,
        new_data_schema_hash = new_data_schema_hash,
        lineage_info = lineage_info_dict
    )

except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 7: Data Quality & Transformations")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Data & Post-Processing

# COMMAND ----------

try:
    set_current_step(8, "Write Data")
    new_data = add_scd2_columns_for_dimensions(
        new_data = new_data,
        column_to_mark_source_data_deletion = watermark_config.column_to_mark_source_data_deletion,
        delete_rows_with_value = watermark_config.delete_rows_with_value,
        source_timestamp_column_name = dimension_config.source_timestamp_column_name,
        unity_catalog_table_output = target_config.unity_catalog_table_output,
        first_run = first_run,
        merge_type = watermark_config.merge_type,
    )

    stats_properties = build_statistics_columns_config(
        df = new_data,
        compute_statistics_on_columns = performance_config.compute_statistics_on_columns,
        compute_statistics_on_first_n_columns = performance_config.compute_statistics_on_first_n_columns,
        unity_catalog_table_output = target_config.unity_catalog_table_output
    )

    table_properties = build_table_properties(
        spark_config = spark_config,
        layer_config = layer_config,
        stats_properties = stats_properties
    )

    create_delta_table(
        df = new_data,
        liquid_clustering_columns = liquid_clustering_columns,
        target_table_name = target_config.target_table_name,
        output_external_location = target_config.output_external_location,
        unity_catalog_table_output = target_config.unity_catalog_table_output,
        target_table_exists = target_table_exists,
        table_properties = table_properties,
    )

    total_records_processed = write_data_orchestrator(
        df = new_data,
        first_run = first_run,
        target_config = target_config_dict,
        watermark_config = watermark_config_dict,
        primary_keys = primary_keys,
        dimension_config = dimension_config_dict
    )

    quarantined_records = write_quarantined_data(
        quarantined_df = quarantined_data,
        target_quarantined_target = target_config.target_quarantined_target
    )

    post_processing_scd2_update(
        primary_keys = primary_keys,
        target_destination = target_config.target_destination,
        unity_catalog_table_output = target_config.unity_catalog_table_output,
        merge_type = watermark_config.merge_type,
        total_records_processed = total_records_processed
    )

    drop_external_table_for_shortcut(
        target_table_name = target_config.target_table_name,
        first_run = first_run,
        output_external_location = target_config.output_external_location,
        unity_catalog_table_output = target_config.unity_catalog_table_output
    )

    execute_vacuum_if_needed(
        unity_catalog_table_output = target_config.unity_catalog_table_output,
        target_table_name = target_config.target_table_name,
        target_destination = target_config.target_destination,
        min_operations_threshold = 50
    )

    cleanup_temporary_files(
        file_staging_path = custom_paths.file_staging_path,
        clean_up_temporary_path = custom_paths.clean_up_temporary_path
    )

except Exception as e:
    log_failure_and_cleanup(logging_conn, log_entry, e, step="Step 8: Write Data")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Success Logging & Cleanup

# COMMAND ----------

try:
    log_success_and_cleanup(
        logging_conn = logging_conn,
        log_entry = log_entry,
        source_details = source_details,
        new_watermark_value = new_watermark_value,
        total_records_processed = total_records_processed,
        quarantined_records = quarantined_records,
        dq_warnings = dq_warnings,
        new_schema = new_schema,
        table_id = table_id,
        target_datastore = target_datastore,
        target_entity = target_entity,
        new_data_schema_hash = new_data_schema_hash,
        new_schema_details = new_schema_details,
        run_monitor_url = run_monitor_url
    )
finally:
    close_warehouse_connection(logging_conn)
