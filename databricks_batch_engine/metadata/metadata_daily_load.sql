-- =====================================================================
-- Metadata Configuration: Daily Load Sample Pipelines
-- Trigger Name: daily_load
-- Generated: 2026-04-17
-- =====================================================================
--
-- Purpose:
--   Sample daily_load pipeline that ingests three CSV files from the
--   bronze landing area into curated silver Delta tables:
--     - Table_ID 1001: housing_price.csv -> silver.silver.housing_price
--     - Table_ID 1002: london_taxi.csv   -> silver.silver.london_taxi
--     - Table_ID 1003: nyc_taxi.csv      -> silver.silver.nyc_taxi
--
-- Source: bronze datastore (CSV files in landing volume)
-- Target: silver.silver.housing_price, silver.silver.london_taxi, silver.silver.nyc_taxi
--
-- Prerequisites:
--   - bronze, silver, and metadata datastores registered in
--     Datastore_Configuration (synced from datastore_DEV.json)
--   - Source CSV files staged at the wildcard_folder_path locations
--
-- Notes:
--   - Metadata table names are intentionally UNQUALIFIED. The deploy
--     session (commit_pipeline.py) issues USE CATALOG/USE SCHEMA from
--     datastore_<ENV>.json (metadata.catalog / metadata.schema) before
--     executing this file, so bare table names resolve automatically.
--   - All three tables use overwrite merge semantics (full reload each run)
--   - watermark_details.data_type = datetime is set for framework defaults
--     even though merge_type=overwrite does not consume a watermark
-- =====================================================================

-- =====================================================================
-- STEP 1: DELETE existing records for this trigger
-- =====================================================================
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'daily_load');

DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'daily_load');

DELETE FROM Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'daily_load';

-- =====================================================================
-- STEP 2: Orchestration Metadata
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Orchestration (Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
VALUES
('daily_load', 1, 1001, 'silver', 'housing_price', 'MedInc', 'batch', 1),
('daily_load', 2, 1002, 'silver', 'london_taxi', 'distance,pickup_latitude,pickup_longitude', 'batch', 1),
('daily_load', 3, 1003, 'silver', 'nyc_taxi', 'distance,pickup_latitude,pickup_longitude', 'batch', 1);

-- =====================================================================
-- STEP 3: Primary Configuration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
-- Table_ID 1001: housing_price.csv -> silver.silver.housing_price
(1001, 'source_details', 'wildcard_folder_path', 'raw_files/housing_price.csv'),
(1001, 'source_details', 'datastore_name', 'bronze'),
(1001, 'source_details', 'source', 'file'),
(1001, 'source_details', 'file_extension', 'csv'),
(1001, 'target_details', 'merge_type', 'overwrite'),
(1001, 'watermark_details', 'data_type', 'datetime'),
-- Table_ID 1002: london_taxi.csv -> silver.silver.london_taxi
(1002, 'source_details', 'wildcard_folder_path', 'raw_files/london_taxi.csv'),
(1002, 'source_details', 'datastore_name', 'bronze'),
(1002, 'source_details', 'source', 'file'),
(1002, 'source_details', 'file_extension', 'csv'),
(1002, 'target_details', 'merge_type', 'overwrite'),
(1002, 'watermark_details', 'data_type', 'datetime'),
-- Table_ID 1003: nyc_taxi.csv -> silver.silver.nyc_taxi
(1003, 'source_details', 'wildcard_folder_path', 'raw_files/nyc_taxi.csv'),
(1003, 'source_details', 'datastore_name', 'bronze'),
(1003, 'source_details', 'source', 'file'),
(1003, 'source_details', 'file_extension', 'csv'),
(1003, 'target_details', 'merge_type', 'overwrite'),
(1003, 'watermark_details', 'data_type', 'datetime');

-- =====================================================================
-- STEP 4: Advanced Configuration - Data Transformation Steps
-- Added for Table_ID 1003 (nyc_taxi):
--   - Dedup on trip_id (requested)
--   - Mask hack_license as PII (requested)
-- NOTE: the current sample_data/nyc_taxi.csv does NOT include trip_id or
-- hack_license columns. These transformations assume an upstream CSV
-- schema that contains both. Update the sample or source feed before run.
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
(1003, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'trip_id'),
(1003, 'data_transformation_steps', 'mask_sensitive_data', 2, 'column_name', 'hack_license'),
(1003, 'data_transformation_steps', 'mask_sensitive_data', 2, 'upper_char', 'X'),
(1003, 'data_transformation_steps', 'mask_sensitive_data', 2, 'lower_char', 'X'),
(1003, 'data_transformation_steps', 'mask_sensitive_data', 2, 'digit_char', 'X'),
(1003, 'data_transformation_steps', 'mask_sensitive_data', 2, 'other_char', 'X');
