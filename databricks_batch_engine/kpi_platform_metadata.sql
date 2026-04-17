-- If your catalog is NOT named "kpi_platform", find-replace throughout:
--   kpi_platform  →  your_catalog_name
USE CATALOG kpi_platform;


-- Databricks metadata/control-plane deployment script.
-- Object names use catalog.schema.object in Unity Catalog.
-- Target catalog: kpi_platform
-- Target schema:  metadata

CREATE SCHEMA IF NOT EXISTS metadata;
-- -----------------------------------------------------------------------------
-- Tables
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Pipeline_Metadata_Orchestration (
  Trigger_Name STRING,
  Order_Of_Operations INT,
  Table_ID INT,
  Target_Datastore STRING,
  Target_Entity STRING,
  Primary_Keys STRING,
  Processing_Method STRING,
  Ingestion_Active INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration (
  Table_ID INT,
  Configuration_Category STRING,
  Configuration_Name STRING,
  Configuration_Value STRING
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Pipeline_Metadata_Advanced_Configuration (
  Table_ID INT,
  Configuration_Category STRING,
  Configuration_Name STRING,
  Configuration_Name_Instance_Number INT,
  Configuration_Attribute_Name STRING,
  Configuration_Attribute_Value STRING
)
USING DELTA;




-- Datastore_Configuration is the runtime-side mirror of
-- databricks_batch_engine/datastores/datastore_<ENV>.json. It is upserted by
-- automation_scripts/agents/sync_datastore_config.py during /fdp-04-commit.
--
-- Rows describe either a Databricks medallion layer or an external source
-- system (SAP, SQL Server, Snowflake, REST API, ADLS, ...). Target table
-- schemas are NOT stored here; schemas live in Target_Entity ("schema.table")
-- on Data_Pipeline_Metadata_Orchestration, so a single datastore row serves
-- every table in that layer.
CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Datastore_Configuration (
  Datastore_Name     STRING NOT NULL,   -- Logical name ('bronze', 'silver', 'metadata', 'sap_erp', ...)
  Datastore_Kind     STRING NOT NULL,   -- 'databricks' | 'sql_server' | 'snowflake' | 'rest_api' | 'adls_gen2' | ...
  Medallion_Layer    STRING,            -- Layer name for databricks rows; NULL for external systems
  Workspace_ID       STRING,            -- Databricks workspace ID (databricks rows only)
  Workspace_URL      STRING,            -- https://adb-xxx.azuredatabricks.net (databricks rows only)
  SQL_Warehouse_ID   STRING,            -- Databricks SQL warehouse used for this datastore (databricks rows only)
  Catalog_Name       STRING,            -- Unity Catalog catalog for databricks rows; NULL for external
  Connection_Details STRING             -- JSON blob with kind-specific fields (host, database, secret scope, ...) for external datastores
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Pipeline_Logs (
  Log_ID STRING,
  Table_ID INT,
  Source_Medallion_Layer STRING,
  Source_Type STRING,
  Data_Source_Details STRING,
  Target_Medallion_Layer STRING,
  Target_Type STRING,
  Target_Datastore STRING,
  Target_Entity STRING,
  Ingestion_Start_Time TIMESTAMP,
  Ingestion_End_Time TIMESTAMP,
  Ingestion_Status STRING,
  Processing_Phase STRING,
  Watermark_Value STRING,
  Records_Processed INT,
  Quarantined_Records INT,
  Job_Run_URL STRING,
  Trigger_Name STRING,
  Trigger_Step INT,
  Trigger_Execution_ID STRING,
  Trigger_Execution_Start_Time TIMESTAMP,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Quality_Notifications (
  Log_ID STRING,
  Datastore_Name STRING,
  Table_ID INT,
  Table_Name STRING,
  Data_Quality_Category STRING,
  Data_Quality_Result STRING,
  Data_Quality_Message STRING,
  Rows_Impacted INT,
  Data_Quarantined STRING,
  Rows_Quarantined INT,
  Ingestion_Start_Time TIMESTAMP,
  Ingestion_End_Time TIMESTAMP,
  Job_Run_URL STRING,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Schema_Logs (
  Table_ID INT,
  Datastore_Name STRING,
  Table_Name STRING,
  Schema_ID STRING,
  Schema_Details STRING,
  Schema_Arrival_Time TIMESTAMP,
  Job_Run_URL STRING,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Schema_Changes (
  Table_ID INT,
  Datastore_Name STRING,
  Table_Name STRING,
  Change_Type STRING,
  Column_Name STRING,
  Data_Type_Details STRING,
  Schema_Arrival_Time TIMESTAMP,
  Job_Run_URL STRING,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Data_Pipeline_Lineage (
  Lineage_ID STRING NOT NULL,
  Trigger_Name STRING,
  Source_Table_ID INT,
  Target_Table_ID INT,
  Source_Entity STRING,
  Source_Datastore STRING,
  Source_Medallion_Layer STRING,
  Source_Type STRING,
  Source_Workspace_Name STRING,
  Source_ABFSS_Path STRING,
  Source_Connection_ID STRING,
  Target_Entity STRING,
  Target_Datastore STRING,
  Target_Medallion_Layer STRING,
  Target_Type STRING,
  Target_Workspace_Name STRING,
  Target_ABFSS_Path STRING,
  Relationship_Type STRING,
  Transformation_Applied STRING,
  Lineage_Depth INT,
  Lineage_Path STRING,
  Is_Cross_Trigger BOOLEAN,
  Cross_Trigger_Dependencies STRING,
  Lineage_Group_ID STRING,
  Lineage_Group_Table_Count INT,
  Lineage_Group_Narrative STRING,
  Lineage_Summary STRING,
  Source_Lineage_Narrative STRING,
  Target_Lineage_Narrative STRING,
  Lineage_Generated_At TIMESTAMP,
  Lineage_Version INT,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Exploratory_Data_Analysis_Results (
  Table_ID INT,
  Datastore_Name STRING,
  Target_Type STRING,
  Target_Medallion_Layer STRING,
  Table_Name STRING,
  Table_Last_Modified_Time TIMESTAMP,
  Column_Name STRING,
  Data_Type STRING,
  Total_Rows INT,
  Total_Columns INT,
  Approx_Distinct_Values INT,
  Null_Count INT,
  Null_Percent DECIMAL(20, 4),
  Mean DECIMAL(20, 4),
  Std_Dev DECIMAL(20, 4),
  Min STRING,
  Max STRING,
  Data_Profile_Execution_Time TIMESTAMP NOT NULL,
  Date_Key INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Date_Dimension (
  Date_Key INT,
  `Date` DATE,
  Date_Text STRING,
  Year INT,
  Quarter INT,
  Month INT,
  Month_Name STRING,
  Month_Name_Abbrev STRING,
  Day INT,
  Day_Name STRING,
  Day_Of_Week INT,
  Week_Of_Year INT,
  Is_Weekend BOOLEAN,
  Month_Year STRING,
  Sort_Year INT,
  Sort_Quarter INT,
  Sort_Month INT,
  Sort_Day INT,
  Sort_Day_Of_Week INT,
  Sort_Week_Of_Year INT,
  Sort_Year_Month INT
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Activity_Run_Logs (
  Table_ID INT,
  Log_ID STRING NOT NULL,
  Sequence_Number INT NOT NULL,
  Log_Timestamp TIMESTAMP,
  Log_Level STRING NOT NULL,
  Step_Name STRING,
  Step_Number INT,
  Message STRING NOT NULL,
  Source_Type STRING
)
USING DELTA;




CREATE TABLE IF NOT EXISTS kpi_platform.metadata.Metadata_Files (
  File_Name STRING,
  Last_Modified TIMESTAMP
)
USING DELTA;




-- -- -----------------------------------------------------------------------------
-- -- Stored procedures
-- -- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Create_Date_Dimension()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  IF NOT EXISTS (SELECT 1 FROM kpi_platform.metadata.Date_Dimension LIMIT 1) THEN
    INSERT INTO kpi_platform.metadata.Date_Dimension
    SELECT
      CAST(date_format(dt, 'yyyyMMdd') AS INT) AS Date_Key,
      dt AS `Date`,
      date_format(dt, 'yyyy-MM-dd') AS Date_Text,
      year(dt) AS Year,
      quarter(dt) AS Quarter,
      month(dt) AS Month,
      date_format(dt, 'MMMM') AS Month_Name,
      date_format(dt, 'MMM') AS Month_Name_Abbrev,
      day(dt) AS Day,
      date_format(dt, 'EEEE') AS Day_Name,
      dayofweek(dt) AS Day_Of_Week,
      weekofyear(dt) AS Week_Of_Year,
      CASE WHEN dayofweek(dt) IN (1, 7) THEN TRUE ELSE FALSE END AS Is_Weekend,
      concat(date_format(dt, 'MMM'), ', ', year(dt)) AS Month_Year,
      year(dt) * -1 AS Sort_Year,
      quarter(dt) * -1 AS Sort_Quarter,
      month(dt) * -1 AS Sort_Month,
      CAST(date_format(dt, 'yyyyMMdd') AS INT) * -1 AS Sort_Day,
      dayofweek(dt) * -1 AS Sort_Day_Of_Week,
      weekofyear(dt) * -1 AS Sort_Week_Of_Year,
      CAST(concat(year(dt), month(dt)) AS INT) * -1 AS Sort_Year_Month
    FROM (
      SELECT explode(sequence(to_date('2025-03-26'), to_date('2035-12-31'), interval 1 day)) AS dt
    ) dates;
  END IF;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.FIFO_Status(
  p_table_id INT,
  p_target_datastore STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  IF EXISTS (
    SELECT 1
    FROM kpi_platform.metadata.Data_Pipeline_Logs log_rows
    WHERE log_rows.Table_ID = p_table_id
      AND log_rows.Log_ID IN (
        SELECT Log_ID
        FROM kpi_platform.metadata.Data_Pipeline_Logs
        WHERE Table_ID = p_table_id
          AND Processing_Phase = 'Staging'
        GROUP BY Log_ID
        HAVING min(Ingestion_Status) = 'Started'
      )
      AND log_rows.Ingestion_Start_Time > current_timestamp() - INTERVAL 24 HOURS
  ) THEN
    SELECT raise_error(
      concat(
        'The pipeline failed because another run associated with Table_ID = ',
        CAST(p_table_id AS STRING),
        ' is either currently in progress or was recently interrupted. Blocking Log_ID values: ',
        coalesce(
          (
            SELECT array_join(sort_array(collect_set(Log_ID)), ', ')
            FROM kpi_platform.metadata.Data_Pipeline_Logs log_rows
            WHERE log_rows.Table_ID = p_table_id
              AND log_rows.Log_ID IN (
                SELECT Log_ID
                FROM kpi_platform.metadata.Data_Pipeline_Logs
                WHERE Table_ID = p_table_id
                  AND Processing_Phase = 'Staging'
                GROUP BY Log_ID
                HAVING min(Ingestion_Status) = 'Started'
              )
              AND log_rows.Ingestion_Start_Time > current_timestamp() - INTERVAL 24 HOURS
          ),
          '(none)'
        )
      )
    ) AS Error_Message;
  ELSE
    SELECT 'Yes' AS Proceed;
  END IF;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Advanced_Metadata(
  p_table_id INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  WITH grouped AS (
    SELECT
      Configuration_Category,
      Configuration_Name_Instance_Number,
      to_json(
        map_concat(
          map('Category', max(Configuration_Name)),
          map_from_arrays(
            collect_list(Configuration_Attribute_Name),
            collect_list(Configuration_Attribute_Value)
          )
        )
      ) AS advanced_settings
    FROM kpi_platform.metadata.Data_Pipeline_Metadata_Advanced_Configuration
    WHERE Table_ID = p_table_id
    GROUP BY Configuration_Category, Configuration_Name_Instance_Number
  )
  SELECT Configuration_Category, Configuration_Name_Instance_Number, advanced_settings
  FROM grouped
  UNION ALL
  SELECT 'NoData', 0, 'NoData'
  WHERE NOT EXISTS (SELECT 1 FROM grouped)
  ORDER BY Configuration_Name_Instance_Number;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Datastore_Details(
  p_datastore_name STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM kpi_platform.metadata.Datastore_Configuration
    WHERE lower(Datastore_Name) = lower(p_datastore_name)
  ) THEN
    SELECT raise_error(
      concat(
        'Datastore ''',
        lower(p_datastore_name),
        ''' not found in Datastore_Configuration. Available datastores: ',
        coalesce(
          (
            SELECT array_join(sort_array(collect_list(Datastore_Name)), ', ')
            FROM kpi_platform.metadata.Datastore_Configuration
          ),
          '(none)'
        )
      )
    ) AS Error_Message;
  END IF;

  SELECT
    Datastore_Name,
    Datastore_Kind,
    Medallion_Layer,
    Workspace_ID,
    Workspace_URL,
    SQL_Warehouse_ID,
    Catalog_Name,
    Connection_Details
  FROM kpi_platform.metadata.Datastore_Configuration
  WHERE lower(Datastore_Name) = lower(p_datastore_name);
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Exploratory_Analysis_Input(
  p_trigger_name STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  WITH profiling_config AS (
    SELECT
      o.Table_ID,
      o.Target_Datastore,
      o.Target_Entity,
      trim(lower(coalesce(c.Configuration_Value, 'weekly'))) AS Data_Profiling_Frequency,
      max(e.Data_Profile_Execution_Time) AS Last_Run_Datetime,
      datediff(current_timestamp(), max(e.Data_Profile_Execution_Time)) AS Days_Since_Last_Run
    FROM kpi_platform.metadata.Data_Pipeline_Metadata_Orchestration o
    LEFT JOIN kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration c
      ON o.Table_ID = c.Table_ID
     AND c.Configuration_Name = 'data_profiling_frequency'
    LEFT JOIN kpi_platform.metadata.Exploratory_Data_Analysis_Results e
      ON o.Table_ID = e.Table_ID
    WHERE o.Target_Entity NOT LIKE '%/%'
      AND o.Ingestion_Active = 1
      AND o.Trigger_Name = p_trigger_name
      AND trim(lower(coalesce(c.Configuration_Value, 'weekly'))) <> 'never'
    GROUP BY o.Table_ID, o.Target_Datastore, o.Target_Entity, c.Configuration_Value
  )
  SELECT
    Table_ID,
    Target_Datastore,
    Target_Entity,
    Data_Profiling_Frequency,
    Last_Run_Datetime,
    Days_Since_Last_Run
  FROM profiling_config
  WHERE (Data_Profiling_Frequency = 'monthly' AND Days_Since_Last_Run >= 30)
     OR (Data_Profiling_Frequency = 'weekly' AND Days_Since_Last_Run >= 7)
     OR (Data_Profiling_Frequency = 'daily' AND Days_Since_Last_Run >= 1)
     OR Last_Run_Datetime IS NULL;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Last_Run_Status(
  p_table_id INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  SELECT coalesce(min(Ingestion_Status), 'Failed') AS Last_Ingestion_Status
  FROM kpi_platform.metadata.Data_Pipeline_Logs
  WHERE Trigger_Execution_Start_Time = (
      SELECT max(Trigger_Execution_Start_Time)
      FROM kpi_platform.metadata.Data_Pipeline_Logs
      WHERE Table_ID = p_table_id
    )
    AND Table_ID = p_table_id
    AND Processing_Phase = 'Batch';
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Pipeline_Tables(
  p_trigger_name STRING,
  p_start_with_step INT DEFAULT 0,
  p_table_ids STRING DEFAULT '0'
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  WITH requested_tables AS (
    SELECT explode(transform(split(p_table_ids, ','), x -> try_cast(trim(x) AS INT))) AS Table_ID
  )
  SELECT
    o.Trigger_Name,
    o.Order_Of_Operations,
    concat_ws(
      ',',
      collect_list(
        concat(
          '''',
          cast(o.Table_ID AS STRING), ':',
          o.Processing_Method, ':',
          o.Target_Datastore, ':',
          o.Target_Entity, ':',
          coalesce(pc.Configuration_Value, ''), ':',
          coalesce(sl.Configuration_Value, ''), ':',
          coalesce(se.Configuration_Value, ''), ':',
          CASE WHEN o.Processing_Method <> 'batch' THEN lower(uuid()) ELSE '' END,
          ''''
        )
      )
    ) AS Pipeline_Table_Details
  FROM kpi_platform.metadata.Data_Pipeline_Metadata_Orchestration o
  LEFT JOIN kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration pc
    ON pc.Table_ID = o.Table_ID
   AND pc.Configuration_Category = 'source_details'
   AND pc.Configuration_Name = 'datastore_name'
  LEFT JOIN kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration sl
    ON sl.Table_ID = o.Table_ID
   AND sl.Configuration_Category = 'source_details'
   AND sl.Configuration_Name = 'staging_lakehouse_name'
  LEFT JOIN kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration se
    ON se.Table_ID = o.Table_ID
   AND se.Configuration_Category = 'other_settings'
   AND se.Configuration_Name = 'spark_environment_id'
  WHERE o.Trigger_Name = p_trigger_name
    AND o.Order_Of_Operations >= p_start_with_step
    AND (p_table_ids = '0' OR o.Table_ID IN (SELECT Table_ID FROM requested_tables))
    AND o.Ingestion_Active = 1
  GROUP BY o.Trigger_Name, o.Order_Of_Operations
  ORDER BY o.Order_Of_Operations;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Schema_Details(
  p_table_id INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  WITH latest_schema AS (
    SELECT Schema_ID, Schema_Details, Schema_Arrival_Time
    FROM kpi_platform.metadata.Schema_Logs
    WHERE Table_ID = p_table_id
    ORDER BY Schema_Arrival_Time DESC
    LIMIT 1
  )
  SELECT Schema_ID, Schema_Details, Schema_Arrival_Time
  FROM latest_schema
  UNION ALL
  SELECT 'NoData', 'NoData', CAST(NULL AS TIMESTAMP)
  WHERE NOT EXISTS (
    SELECT 1
    FROM kpi_platform.metadata.Schema_Logs
    WHERE Table_ID = p_table_id
  );
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Get_Watermark_Value(
  p_table_id INT DEFAULT NULL,
  p_target_datastore STRING DEFAULT NULL,
  p_processing_phase STRING DEFAULT 'Batch'
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  WITH watermark_config AS (
    SELECT coalesce(Configuration_Value, 'datetime') AS watermark_data_type
    FROM kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration
    WHERE Table_ID = p_table_id
      AND Configuration_Category = 'watermark_details'
      AND Configuration_Name = 'data_type'
    LIMIT 1
  ),
  processed_logs AS (
    SELECT *
    FROM kpi_platform.metadata.Data_Pipeline_Logs
    WHERE Table_ID = p_table_id
      AND lower(Target_Datastore) = lower(p_target_datastore)
      AND Processing_Phase = p_processing_phase
      AND Ingestion_Status = 'Processed'
  ),
  latest_processed_log AS (
    SELECT Watermark_Value
    FROM processed_logs
    ORDER BY Ingestion_End_Time DESC NULLS LAST
    LIMIT 1
  ),
  watermark_state AS (
    SELECT
      coalesce((SELECT count(*) FROM processed_logs), 0) AS matched_records,
      coalesce((SELECT watermark_data_type FROM watermark_config), 'datetime') AS watermark_data_type,
      CASE
        WHEN coalesce((SELECT watermark_data_type FROM watermark_config), 'datetime') = 'datetime' THEN
          (
            SELECT CASE
              WHEN Watermark_Value = 'FULL_RELOAD' THEN '1900-01-01 14:44:44'
              WHEN Watermark_Value RLIKE '^[+-]?[0-9]+(\\.[0-9]+)?$' THEN Watermark_Value
              ELSE CAST(try_cast(Watermark_Value AS TIMESTAMP) AS STRING)
            END
            FROM latest_processed_log
          )
        ELSE
          (
            SELECT CASE
              WHEN Watermark_Value = 'FULL_RELOAD' THEN '-2'
              ELSE Watermark_Value
            END
            FROM latest_processed_log
          )
      END AS latest_watermark_value
  )
  SELECT
    CASE
      WHEN matched_records = 0 THEN CASE WHEN watermark_data_type = 'datetime' THEN '1900-01-01 00:00:00' ELSE '-1' END
      WHEN coalesce(latest_watermark_value, '') = '' THEN CASE WHEN watermark_data_type = 'datetime' THEN '1900-01-01 00:00:00' ELSE '-1' END
      ELSE latest_watermark_value
    END AS watermark_value,
    matched_records AS matched_records,
    CASE
      WHEN latest_watermark_value IN ('1900-01-01 14:44:44', '-2') THEN 'Yes'
      ELSE NULL
    END AS full_reload
  FROM watermark_state;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Insert_Activity_Log_Entry(
  p_log_id STRING,
  p_table_id INT,
  p_log_level STRING,
  p_step_name STRING,
  p_step_number INT,
  p_message STRING,
  p_source_type STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  INSERT INTO kpi_platform.metadata.Activity_Run_Logs (
    Table_ID,
    Log_ID,
    Sequence_Number,
    Log_Timestamp,
    Log_Level,
    Step_Name,
    Step_Number,
    Message,
    Source_Type
  )
  VALUES (
    p_table_id,
    p_log_id,
    1,
    current_timestamp(),
    p_log_level,
    p_step_name,
    p_step_number,
    p_message,
    p_source_type
  );
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Insert_Activity_Log_Entries(
  p_log_id STRING,
  p_table_id INT,
  p_entries STRING,
  p_source_type STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  INSERT INTO kpi_platform.metadata.Activity_Run_Logs (
    Table_ID,
    Log_ID,
    Sequence_Number,
    Log_Timestamp,
    Log_Level,
    Step_Name,
    Step_Number,
    Message,
    Source_Type
  )
  SELECT
    p_table_id,
    p_log_id,
    entry.seq,
    try_cast(entry.ts AS TIMESTAMP),
    entry.level,
    entry.step_name,
    entry.step_number,
    entry.message,
    p_source_type
  FROM (
    SELECT explode(
      from_json(
        p_entries,
        'array<struct<seq:int,ts:string,level:string,step_name:string,step_number:int,message:string>>'
      )
    ) AS entry
  ) parsed_entries;

  SELECT count(*) AS Rows_Inserted
  FROM (
    SELECT explode(
      from_json(
        p_entries,
        'array<struct<seq:int,ts:string,level:string,step_name:string,step_number:int,message:string>>'
      )
    ) AS entry
  ) parsed_entries;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Log_Data_Movement(
  p_log_id STRING DEFAULT NULL,
  p_table_id INT DEFAULT NULL,
  p_source_details STRING DEFAULT NULL,
  p_target_datastore STRING DEFAULT NULL,
  p_target_entity STRING DEFAULT NULL,
  p_event_start_time TIMESTAMP DEFAULT NULL,
  p_event_end_time TIMESTAMP DEFAULT NULL,
  p_status STRING DEFAULT NULL,
  p_watermark_value STRING DEFAULT NULL,
  p_records_processed STRING DEFAULT NULL,
  p_quarantined_records STRING DEFAULT NULL,
  p_data_quality_warnings STRING DEFAULT NULL,
  p_data_quality_failures STRING DEFAULT NULL,
  p_job_run_url STRING DEFAULT NULL,
  p_trigger_name STRING DEFAULT NULL,
  p_trigger_step INT DEFAULT NULL,
  p_trigger_id STRING DEFAULT NULL,
  p_trigger_time TIMESTAMP DEFAULT NULL,
  p_source_medallion_layer STRING DEFAULT NULL,
  p_source_type STRING DEFAULT NULL,
  p_target_medallion_layer STRING DEFAULT NULL,
  p_target_type STRING DEFAULT NULL,
  p_processing_phase STRING DEFAULT 'Batch'
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  IF p_status IN ('Started', 'Processed', 'Failed') THEN
    INSERT INTO kpi_platform.metadata.Data_Pipeline_Logs (
      Log_ID,
      Table_ID,
      Source_Medallion_Layer,
      Source_Type,
      Data_Source_Details,
      Target_Medallion_Layer,
      Target_Type,
      Target_Datastore,
      Target_Entity,
      Ingestion_Start_Time,
      Ingestion_End_Time,
      Ingestion_Status,
      Processing_Phase,
      Watermark_Value,
      Records_Processed,
      Quarantined_Records,
      Job_Run_URL,
      Trigger_Name,
      Trigger_Step,
      Trigger_Execution_ID,
      Trigger_Execution_Start_Time,
      Date_Key
    )
    VALUES (
      p_log_id,
      p_table_id,
      p_source_medallion_layer,
      p_source_type,
      p_source_details,
      p_target_medallion_layer,
      p_target_type,
      lower(p_target_datastore),
      p_target_entity,
      p_event_start_time,
      p_event_end_time,
      p_status,
      p_processing_phase,
      p_watermark_value,
      CASE
        WHEN p_status = 'Failed' THEN coalesce(try_cast(p_records_processed AS INT), 0)
        ELSE try_cast(p_records_processed AS INT)
      END,
      try_cast(p_quarantined_records AS INT),
      p_job_run_url,
      p_trigger_name,
      p_trigger_step,
      p_trigger_id,
      p_trigger_time,
      CAST(
        date_format(
          coalesce(
            CASE WHEN p_status = 'Started' THEN p_event_start_time ELSE p_event_end_time END,
            p_event_end_time,
            p_event_start_time,
            current_timestamp()
          ),
          'yyyyMMdd'
        ) AS INT
      )
    );
  END IF;

  IF p_data_quality_warnings IS NOT NULL
     AND trim(p_data_quality_warnings) <> ''
     AND trim(p_data_quality_warnings) <> '[]' THEN
    INSERT INTO kpi_platform.metadata.Data_Quality_Notifications (
      Log_ID,
      Datastore_Name,
      Table_ID,
      Table_Name,
      Data_Quality_Category,
      Data_Quality_Result,
      Data_Quality_Message,
      Rows_Impacted,
      Data_Quarantined,
      Rows_Quarantined,
      Ingestion_Start_Time,
      Ingestion_End_Time,
      Job_Run_URL,
      Date_Key
    )
    SELECT
      p_log_id,
      lower(p_target_datastore),
      p_table_id,
      p_target_entity,
      warning.category,
      warning.outcome,
      warning.message,
      coalesce(try_cast(warning.rows_quarantined AS INT), try_cast(warning.rows_impacted AS INT), 0),
      coalesce(warning.quarantined, 'No'),
      coalesce(try_cast(warning.rows_quarantined AS INT), 0),
      p_event_start_time,
      p_event_end_time,
      p_job_run_url,
      CAST(date_format(coalesce(p_event_end_time, p_event_start_time, current_timestamp()), 'yyyyMMdd') AS INT)
    FROM (
      SELECT explode(
        from_json(
          p_data_quality_warnings,
          'array<struct<category:string,message:string,quarantined:string,rows_quarantined:string,rows_impacted:string,outcome:string>>'
        )
      ) AS warning
    ) parsed_warnings;
  END IF;

  IF p_data_quality_failures IS NOT NULL
     AND trim(p_data_quality_failures) <> ''
     AND trim(p_data_quality_failures) <> '[]' THEN
    INSERT INTO kpi_platform.metadata.Data_Quality_Notifications (
      Log_ID,
      Datastore_Name,
      Table_ID,
      Table_Name,
      Data_Quality_Category,
      Data_Quality_Result,
      Data_Quality_Message,
      Rows_Impacted,
      Data_Quarantined,
      Rows_Quarantined,
      Ingestion_Start_Time,
      Ingestion_End_Time,
      Job_Run_URL,
      Date_Key
    )
    SELECT
      p_log_id,
      lower(p_target_datastore),
      p_table_id,
      p_target_entity,
      failure.category,
      'Failure',
      failure.message,
      coalesce(try_cast(failure.rows_impacted AS INT), 0),
      'No',
      0,
      p_event_start_time,
      p_event_end_time,
      p_job_run_url,
      CAST(date_format(coalesce(p_event_end_time, p_event_start_time, current_timestamp()), 'yyyyMMdd') AS INT)
    FROM (
      SELECT explode(
        from_json(
          p_data_quality_failures,
          'array<struct<category:string,message:string,rows_impacted:string>>'
        )
      ) AS failure
    ) parsed_failures;
  END IF;

  SELECT 1 AS Status;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Log_New_Schema(
  p_table_id INT DEFAULT NULL,
  p_datastore_name STRING DEFAULT NULL,
  p_table_name STRING DEFAULT NULL,
  p_schema_id STRING DEFAULT NULL,
  p_schema_details STRING DEFAULT NULL,
  p_schema_updates STRING DEFAULT NULL,
  p_job_run_url STRING DEFAULT NULL,
  p_end_time TIMESTAMP DEFAULT NULL
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  INSERT INTO kpi_platform.metadata.Schema_Logs (
    Table_ID,
    Datastore_Name,
    Table_Name,
    Schema_ID,
    Schema_Details,
    Schema_Arrival_Time,
    Job_Run_URL,
    Date_Key
  )
  VALUES (
    p_table_id,
    p_datastore_name,
    p_table_name,
    p_schema_id,
    p_schema_details,
    p_end_time,
    p_job_run_url,
    CAST(date_format(CAST(p_end_time AS DATE), 'yyyyMMdd') AS INT)
  );

  IF p_schema_updates IS NOT NULL
     AND trim(p_schema_updates) <> ''
     AND trim(p_schema_updates) <> '[]' THEN
    INSERT INTO metadata.Schema_Changes (
      Table_ID,
      Datastore_Name,
      Table_Name,
      Change_Type,
      Column_Name,
      Data_Type_Details,
      Schema_Arrival_Time,
      Job_Run_URL,
      Date_Key
    )
    SELECT
      p_table_id,
      p_datastore_name,
      p_table_name,
      schema_change.change_type,
      schema_change.column,
      schema_change.data_type,
      p_end_time,
      p_job_run_url,
      CAST(date_format(CAST(p_end_time AS DATE), 'yyyyMMdd') AS INT)
    FROM (
      SELECT explode(
        from_json(
          p_schema_updates,
          'array<struct<column:string,data_type:string,change_type:string>>'
        )
      ) AS schema_change
    ) parsed_changes;
  END IF;

  SELECT 1 AS Status;
END;

CREATE OR REPLACE PROCEDURE kpi_platform.metadata.Pivot_Primary_Config(
  p_table_id INT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  -- Databricks-friendly version: return raw rows and let Python build the dynamic pivot.
  SELECT
    Configuration_Category,
    Configuration_Name,
    Configuration_Value
  FROM kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration
  WHERE Table_ID = p_table_id
  ORDER BY Configuration_Category, Configuration_Name;
END;


-- -----------------------------------------------------------------------------
-- Seed Data: Datastore Configuration
-- -----------------------------------------------------------------------------
-- NOTE: In production this table is populated by
--       automation_scripts/agents/sync_datastore_config.py during /fdp-04-commit
--       from databricks_batch_engine/datastores/datastore_<ENV>.json.
--       The rows below are just a default seed so the SQL can be run standalone.
--       Per-layer schemas live on Target_Entity ("schema.table") on the
--       orchestration rows, not here.

MERGE INTO kpi_platform.metadata.Datastore_Configuration AS tgt
USING (
  SELECT * FROM VALUES
    ('bronze',   'databricks', 'bronze',   NULL, NULL, NULL, 'kpi_platform', CAST(NULL AS STRING)),
    ('silver',   'databricks', 'silver',   NULL, NULL, NULL, 'kpi_platform', CAST(NULL AS STRING)),
    ('gold',     'databricks', 'gold',     NULL, NULL, NULL, 'kpi_platform', CAST(NULL AS STRING)),
    ('metadata', 'databricks', 'metadata', NULL, NULL, NULL, 'kpi_platform', CAST(NULL AS STRING))
  AS src(Datastore_Name, Datastore_Kind, Medallion_Layer, Workspace_ID, Workspace_URL, SQL_Warehouse_ID, Catalog_Name, Connection_Details)
) AS src
ON tgt.Datastore_Name = src.Datastore_Name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- -----------------------------------------------------------------------------
-- Seed Data: Orchestration (daily_load trigger)
-- -----------------------------------------------------------------------------

MERGE INTO kpi_platform.metadata.Data_Pipeline_Metadata_Orchestration AS tgt
USING (
  SELECT * FROM VALUES
    ('daily_load', 1, 1001, 'silver', 'dbo.housing_price', 'MedInc',                                  'batch', 1),
    ('daily_load', 2, 1002, 'silver', 'dbo.london_taxi',   'distance,pickup_latitude,pickup_longitude', 'batch', 1),
    ('daily_load', 3, 1003, 'silver', 'dbo.nyc_taxi',      'distance,pickup_latitude,pickup_longitude', 'batch', 1)
  AS src(Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
) AS src
ON tgt.Table_ID = src.Table_ID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- -----------------------------------------------------------------------------
-- Seed Data: Primary Configuration (daily_load tables)
-- -----------------------------------------------------------------------------
-- IMPORTANT: Configuration_Name must match what the Python framework reads.
-- The code builds keys as: {Configuration_Category}_{Configuration_Name}
-- e.g. ('source_details', 'datastore_name') -> primary_config["source_details_datastore_name"]
--
-- Common mistakes (from original metadata):
--   'source_category'    -> should be 'source'         (code reads source_details_source)
--   'default_merge_type' -> should be 'merge_type'     (code reads target_details_merge_type)
--   'source_entity'      -> not read by the framework  (target_entity comes from Orchestration)
--   'target_catalog_name'-> not read by the framework  (comes from Datastore_Configuration)
--   'target_schema_name' -> not read by the framework  (comes from Datastore_Configuration)
-- -----------------------------------------------------------------------------

-- Clear any existing primary config for these Table_IDs to avoid stale/conflicting rows
DELETE FROM kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (1001, 1002, 1003);

INSERT INTO kpi_platform.metadata.Data_Pipeline_Metadata_Primary_Configuration
  (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
  -- =====================================================================
  -- Table_ID 1001: housing_price.csv -> silver.dbo.housing_price
  -- =====================================================================
  (1001, 'source_details', 'wildcard_folder_path', 'housing_price.csv'),
  (1001, 'source_details', 'datastore_name',       'bronze'),
  (1001, 'source_details', 'source',               'file'),
  (1001, 'source_details', 'file_extension',        'csv'),
  (1001, 'target_details', 'merge_type',            'overwrite'),
  (1001, 'watermark_details', 'data_type',          'datetime'),

  -- =====================================================================
  -- Table_ID 1002: london_taxi.csv -> silver.dbo.london_taxi
  -- =====================================================================
  (1002, 'source_details', 'wildcard_folder_path', 'london_taxi.csv'),
  (1002, 'source_details', 'datastore_name',       'bronze'),
  (1002, 'source_details', 'source',               'file'),
  (1002, 'source_details', 'file_extension',        'csv'),
  (1002, 'target_details', 'merge_type',            'overwrite'),
  (1002, 'watermark_details', 'data_type',          'datetime'),

  -- =====================================================================
  -- Table_ID 1003: nyc_taxi.csv -> silver.dbo.nyc_taxi
  -- =====================================================================
  (1003, 'source_details', 'wildcard_folder_path', 'nyc_taxi.csv'),
  (1003, 'source_details', 'datastore_name',       'bronze'),
  (1003, 'source_details', 'source',               'file'),
  (1003, 'source_details', 'file_extension',        'csv'),
  (1003, 'target_details', 'merge_type',            'overwrite'),
  (1003, 'watermark_details', 'data_type',          'datetime');