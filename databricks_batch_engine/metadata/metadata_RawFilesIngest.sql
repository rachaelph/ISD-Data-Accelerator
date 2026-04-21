-- =====================================================================
-- Metadata Configuration: Raw CSV Files (Volume) Ingestion + Medallion
-- Trigger Name: RawFilesIngest
-- Generated: 2026-04-21
-- =====================================================================
--
-- Purpose:
--   Full Bronze -> Silver -> Gold pipeline for three CSV files landed in
--   the Unity Catalog volume /Volumes/kpi_platform/bronze/raw_files/:
--     - housing_price.csv  (California Housing dataset)
--     - london_taxi.csv    (London taxi trips, same schema as nyc_taxi)
--     - nyc_taxi.csv       (NYC taxi trips)
--
--   Bronze (1001-1003): Raw ingestion from volume to Delta with column
--                       name cleansing (snake_case, trim, replace
--                       non-alphanumeric).
--   Silver (1101-1103): Typed, deduplicated, DQ-checked tables.
--   Gold   (1201-1202): Aggregates - taxi_trip_metrics (union of london
--                       + nyc trips by vendor & pickup_month) and
--                       housing_summary (avg target by HouseAge bucket).
--
-- Source: UC Volume - kpi_platform.bronze.raw_files
-- Target: kpi_platform.bronze / silver / gold (per layer datastore)
--
-- Prerequisites:
--   - Datastores 'bronze', 'silver', 'gold' exist in Datastore_Configuration
--     and resolve to catalog 'kpi_platform' (per datastore_DEV.json).
--   - The three CSV files are present at the listed Volume paths.
--
-- Notes:
--   - Bronze writes use merge_type='overwrite' (only append/overwrite
--     are valid for the bronze layer).
--   - Silver/Gold also use 'overwrite' to keep the demo flow stateless.
--   - Auto-watermarking (file mtime) is enabled by default for file
--     ingestion - no explicit watermark_details required.
-- =====================================================================

-- =====================================================================
-- STEP 1: DELETE existing records for this trigger
-- =====================================================================
-- Remove existing metadata for 'RawFilesIngest' to allow clean re-deploy.
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'RawFilesIngest');
DELETE FROM Data_Pipeline_Metadata_Primary_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'RawFilesIngest');
DELETE FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'RawFilesIngest';

-- =====================================================================
-- STEP 2: Orchestration Metadata
-- =====================================================================
-- Order_Of_Operations: 1=Bronze, 2=Silver, 3=Gold (later layers depend on earlier).
-- Processing_Method='batch' for both file ingestion and Delta-to-Delta hops.
-- Primary_Keys left empty for overwrite flows where no merge is performed.
INSERT INTO Data_Pipeline_Metadata_Orchestration (Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
VALUES
('RawFilesIngest', 1, 1001, 'bronze', 'housing_raw', '', 'batch', 1),
('RawFilesIngest', 1, 1002, 'bronze', 'london_taxi_raw', '', 'batch', 1),
('RawFilesIngest', 1, 1003, 'bronze', 'nyc_taxi_raw', '', 'batch', 1),
('RawFilesIngest', 1, 1004, 'bronze', 'titanic_raw', '', 'batch', 1),
('RawFilesIngest', 2, 1101, 'silver', 'housing', '', 'batch', 1),
('RawFilesIngest', 2, 1102, 'silver', 'london_taxi', '', 'batch', 1),
('RawFilesIngest', 2, 1103, 'silver', 'nyc_taxi', '', 'batch', 1),
('RawFilesIngest', 2, 1104, 'silver', 'titanic', '', 'batch', 1),
('RawFilesIngest', 3, 1201, 'gold', 'taxi_trip_metrics', '', 'batch', 1),
('RawFilesIngest', 3, 1202, 'gold', 'housing_summary', '', 'batch', 1),
('RawFilesIngest', 3, 1203, 'gold', 'titanic_survival_summary', '', 'batch', 1);

-- =====================================================================
-- STEP 3: Primary Configuration
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
-- ---------------------------------------------------------------------
-- Table_ID 1001: Bronze ingestion of housing_price.csv from UC Volume
-- ---------------------------------------------------------------------
-- Absolute Volume path - framework accepts paths starting with '/'.
(1001, 'source_details', 'wildcard_folder_path', 'raw_files/housing_price.csv'),
(1001, 'source_details', 'datastore_name', 'bronze'),
(1001, 'source_details', 'file_has_header_row', 'true'),
(1001, 'source_details', 'delimiter', ','),
-- Quarantine rows that fail to parse rather than silently dropping or failing the pipeline.
(1001, 'source_details', 'on_bad_records', 'quarantine'),
-- Standardize column names: trim, lower-case, replace non-alphanumeric (the leading unnamed index column becomes a clean snake_case name).
(1001, 'column_cleansing', 'trim', 'true'),
(1001, 'column_cleansing', 'apply_case', 'lower'),
(1001, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
(1001, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
(1001, 'data_cleansing', 'trim_data_in_string_columns', '*'),
-- Bronze layer only allows append or overwrite -- using overwrite for repeatable demo runs.
(1001, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1002: Bronze ingestion of london_taxi.csv from UC Volume
-- ---------------------------------------------------------------------
(1002, 'source_details', 'wildcard_folder_path', 'raw_files/london_taxi.csv'),
(1002, 'source_details', 'datastore_name', 'bronze'),
(1002, 'source_details', 'file_has_header_row', 'true'),
(1002, 'source_details', 'delimiter', ','),
(1002, 'source_details', 'on_bad_records', 'quarantine'),
(1002, 'column_cleansing', 'trim', 'true'),
(1002, 'column_cleansing', 'apply_case', 'lower'),
(1002, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
(1002, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
(1002, 'data_cleansing', 'trim_data_in_string_columns', '*'),
(1002, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1003: Bronze ingestion of nyc_taxi.csv from UC Volume
-- ---------------------------------------------------------------------
(1003, 'source_details', 'wildcard_folder_path', 'raw_files/nyc_taxi.csv'),
(1003, 'source_details', 'datastore_name', 'bronze'),
(1003, 'source_details', 'file_has_header_row', 'true'),
(1003, 'source_details', 'delimiter', ','),
(1003, 'source_details', 'on_bad_records', 'quarantine'),
(1003, 'column_cleansing', 'trim', 'true'),
(1003, 'column_cleansing', 'apply_case', 'lower'),
(1003, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
(1003, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
(1003, 'data_cleansing', 'trim_data_in_string_columns', '*'),
(1003, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1004: Bronze ingestion of titanic.csv from UC Volume
-- ---------------------------------------------------------------------
(1004, 'source_details', 'wildcard_folder_path', 'raw_files/titanic.csv'),
(1004, 'source_details', 'datastore_name', 'bronze'),
(1004, 'source_details', 'file_has_header_row', 'true'),
(1004, 'source_details', 'delimiter', ','),
(1004, 'source_details', 'on_bad_records', 'quarantine'),
(1004, 'column_cleansing', 'trim', 'true'),
(1004, 'column_cleansing', 'apply_case', 'lower'),
(1004, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
(1004, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
(1004, 'data_cleansing', 'trim_data_in_string_columns', '*'),
(1004, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1101: Silver housing - typed + deduplicated from bronze
-- ---------------------------------------------------------------------
-- Source is a Delta table - only table_name is required (no source attribute).
(1101, 'source_details', 'table_name', 'bronze.housing_raw'),
(1101, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1102: Silver london_taxi - typed + DQ checked from bronze
-- ---------------------------------------------------------------------
(1102, 'source_details', 'table_name', 'bronze.london_taxi_raw'),
(1102, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1103: Silver nyc_taxi - typed + DQ checked from bronze
-- ---------------------------------------------------------------------
(1103, 'source_details', 'table_name', 'bronze.nyc_taxi_raw'),
(1103, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1104: Silver titanic - typed + deduplicated + DQ from bronze
-- ---------------------------------------------------------------------
(1104, 'source_details', 'table_name', 'bronze.titanic_raw'),
(1104, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1201: Gold taxi_trip_metrics - union london + nyc, aggregate
-- ---------------------------------------------------------------------
-- Primary source is london_taxi -- nyc is unioned via union_data step below.
(1201, 'source_details', 'table_name', 'silver.london_taxi'),
(1201, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1202: Gold housing_summary - aggregate by house_age bucket
-- ---------------------------------------------------------------------
(1202, 'source_details', 'table_name', 'silver.housing'),
(1202, 'target_details', 'merge_type', 'overwrite'),
-- ---------------------------------------------------------------------
-- Table_ID 1203: Gold titanic_survival_summary - survival rate by class/sex
-- ---------------------------------------------------------------------
(1203, 'source_details', 'table_name', 'silver.titanic'),
(1203, 'target_details', 'merge_type', 'overwrite');

-- =====================================================================
-- STEP 4: Advanced Configuration (Transformations and Data Quality)
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- ---------------------------------------------------------------------
-- Table_ID 1001 Bronze housing_raw - DQ on key columns
-- ---------------------------------------------------------------------
-- After cleansing, the unnamed index column becomes '_c0' which is then
-- replaced to '_c0' (alphanumeric+underscore preserved). Key business
-- columns we expect to always be present:
(1001, 'data_quality', 'validate_not_null', 1, 'column_name', 'medinc'),
(1001, 'data_quality', 'validate_not_null', 1, 'message', 'medinc must not be null in housing_raw'),
(1001, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1001, 'data_quality', 'validate_not_null', 2, 'column_name', 'target'),
(1001, 'data_quality', 'validate_not_null', 2, 'message', 'target must not be null in housing_raw'),
(1001, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1002 Bronze london_taxi_raw - DQ on key columns
-- ---------------------------------------------------------------------
(1002, 'data_quality', 'validate_not_null', 1, 'column_name', 'vendor'),
(1002, 'data_quality', 'validate_not_null', 1, 'message', 'vendor must not be null in london_taxi_raw'),
(1002, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1002, 'data_quality', 'validate_not_null', 2, 'column_name', 'distance'),
(1002, 'data_quality', 'validate_not_null', 2, 'message', 'distance must not be null in london_taxi_raw'),
(1002, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1003 Bronze nyc_taxi_raw - DQ on key columns
-- ---------------------------------------------------------------------
(1003, 'data_quality', 'validate_not_null', 1, 'column_name', 'vendor'),
(1003, 'data_quality', 'validate_not_null', 1, 'message', 'vendor must not be null in nyc_taxi_raw'),
(1003, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1003, 'data_quality', 'validate_not_null', 2, 'column_name', 'distance'),
(1003, 'data_quality', 'validate_not_null', 2, 'message', 'distance must not be null in nyc_taxi_raw'),
(1003, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1004 Bronze titanic_raw - DQ on key columns
-- ---------------------------------------------------------------------
(1004, 'data_quality', 'validate_not_null', 1, 'column_name', 'passengerid'),
(1004, 'data_quality', 'validate_not_null', 1, 'message', 'passengerid must not be null in titanic_raw'),
(1004, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1004, 'data_quality', 'validate_not_null', 2, 'column_name', 'survived'),
(1004, 'data_quality', 'validate_not_null', 2, 'message', 'survived must not be null in titanic_raw'),
(1004, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1101 Silver housing - cast types, drop dupes, DQ
-- ---------------------------------------------------------------------
-- Drop the unnamed index column carried over from CSV (cleansed name '_c0').
(1101, 'data_transformation_steps', 'remove_columns', 1, 'column_name', '_c0'),
-- Cast all numeric columns to double in one instance (single target type).
(1101, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'medinc,houseage,averooms,avebedrms,population,aveoccup,latitude,longitude,target'),
(1101, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'double'),
-- Filter out clearly invalid rows (negative ages or zero population).
(1101, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'houseage >= 0 AND population > 0'),
-- Deduplicate on the geographic key (latitude+longitude defines the block).
(1101, 'data_transformation_steps', 'drop_duplicates', 4, 'column_name', 'latitude,longitude'),
-- DQ: target (median house value) must be positive.
(1101, 'data_quality', 'validate_range', 1, 'column_name', 'target'),
(1101, 'data_quality', 'validate_range', 1, 'min_value', '0'),
(1101, 'data_quality', 'validate_range', 1, 'message', 'housing target must be positive'),
(1101, 'data_quality', 'validate_range', 1, 'if_not_compliant', 'quarantine'),
(1101, 'data_quality', 'validate_not_null', 2, 'column_name', 'medinc,target'),
(1101, 'data_quality', 'validate_not_null', 2, 'message', 'medinc and target must not be null in silver.housing'),
(1101, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1102 Silver london_taxi - cast types, derive datetime, DQ
-- ---------------------------------------------------------------------
-- Cast geo / distance columns to double.
(1102, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'distance,dropoff_latitude,dropoff_longitude,pickup_latitude,pickup_longitude'),
(1102, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'double'),
-- Cast integer trip attributes.
(1102, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'passengers,store_forward,vendor'),
(1102, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'int'),
-- Filter physically impossible trips.
(1102, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'distance >= 0 AND passengers > 0'),
-- Tag city for later union in Gold layer.
(1102, 'data_transformation_steps', 'derived_column', 4, 'column_name', 'city'),
(1102, 'data_transformation_steps', 'derived_column', 4, 'expression', '"london"'),
-- DQ on key columns.
(1102, 'data_quality', 'validate_not_null', 1, 'column_name', 'vendor,distance'),
(1102, 'data_quality', 'validate_not_null', 1, 'message', 'vendor and distance required in silver.london_taxi'),
(1102, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1102, 'data_quality', 'validate_range', 2, 'column_name', 'passengers'),
(1102, 'data_quality', 'validate_range', 2, 'min_value', '1'),
(1102, 'data_quality', 'validate_range', 2, 'max_value', '8'),
(1102, 'data_quality', 'validate_range', 2, 'message', 'passengers must be between 1 and 8'),
(1102, 'data_quality', 'validate_range', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1103 Silver nyc_taxi - mirrors london_taxi pipeline
-- ---------------------------------------------------------------------
(1103, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'distance,dropoff_latitude,dropoff_longitude,pickup_latitude,pickup_longitude'),
(1103, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'double'),
(1103, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'passengers,store_forward,vendor'),
(1103, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'int'),
(1103, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'distance >= 0 AND passengers > 0'),
(1103, 'data_transformation_steps', 'derived_column', 4, 'column_name', 'city'),
(1103, 'data_transformation_steps', 'derived_column', 4, 'expression', '"nyc"'),
(1103, 'data_quality', 'validate_not_null', 1, 'column_name', 'vendor,distance'),
(1103, 'data_quality', 'validate_not_null', 1, 'message', 'vendor and distance required in silver.nyc_taxi'),
(1103, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(1103, 'data_quality', 'validate_range', 2, 'column_name', 'passengers'),
(1103, 'data_quality', 'validate_range', 2, 'min_value', '1'),
(1103, 'data_quality', 'validate_range', 2, 'max_value', '8'),
(1103, 'data_quality', 'validate_range', 2, 'message', 'passengers must be between 1 and 8'),
(1103, 'data_quality', 'validate_range', 2, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1104 Silver titanic - cast types, dedupe, DQ
-- ---------------------------------------------------------------------
-- Cast integer columns.
(1104, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'passengerid,survived,pclass,sibsp,parch'),
(1104, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'int'),
-- Cast continuous numeric columns.
(1104, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'age,fare'),
(1104, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'double'),
-- Filter out rows with negative fare (invalid).
(1104, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'fare IS NULL OR fare >= 0'),
-- Deduplicate on passengerid (the natural key).
(1104, 'data_transformation_steps', 'drop_duplicates', 4, 'column_name', 'passengerid'),
-- DQ: passengerid + survived must always be present after typing.
(1104, 'data_quality', 'validate_not_null', 1, 'column_name', 'passengerid,survived'),
(1104, 'data_quality', 'validate_not_null', 1, 'message', 'passengerid and survived required in silver.titanic'),
(1104, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- DQ: survived flag must be 0 or 1.
(1104, 'data_quality', 'validate_range', 2, 'column_name', 'survived'),
(1104, 'data_quality', 'validate_range', 2, 'min_value', '0'),
(1104, 'data_quality', 'validate_range', 2, 'max_value', '1'),
(1104, 'data_quality', 'validate_range', 2, 'message', 'survived must be 0 or 1'),
(1104, 'data_quality', 'validate_range', 2, 'if_not_compliant', 'quarantine'),
-- DQ: pclass must be 1, 2, or 3.
(1104, 'data_quality', 'validate_range', 3, 'column_name', 'pclass'),
(1104, 'data_quality', 'validate_range', 3, 'min_value', '1'),
(1104, 'data_quality', 'validate_range', 3, 'max_value', '3'),
(1104, 'data_quality', 'validate_range', 3, 'message', 'pclass must be 1, 2, or 3'),
(1104, 'data_quality', 'validate_range', 3, 'if_not_compliant', 'quarantine'),
-- ---------------------------------------------------------------------
-- Table_ID 1201 Gold taxi_trip_metrics - union london + nyc, aggregate
-- ---------------------------------------------------------------------
-- Step 1: union london (primary source) with nyc on shared columns.
(1201, 'data_transformation_steps', 'union_data', 1, 'union_tables', 'silver.nyc_taxi'),
(1201, 'data_transformation_steps', 'union_data', 1, 'union_type', 'by_name'),
(1201, 'data_transformation_steps', 'union_data', 1, 'allow_missing_columns', 'false'),
-- Step 2: aggregate trip metrics by city + vendor + pickup_month.
(1201, 'data_transformation_steps', 'aggregate_data', 2, 'group_by_columns', 'city,vendor,pickup_month'),
(1201, 'data_transformation_steps', 'aggregate_data', 2, 'column_name', 'distance,passengers,city'),
(1201, 'data_transformation_steps', 'aggregate_data', 2, 'aggregation', 'avg,sum,count'),
(1201, 'data_transformation_steps', 'aggregate_data', 2, 'output_column_name', 'avg_distance,total_passengers,total_trips'),
-- DQ: every aggregated row must have at least one trip.
(1201, 'data_quality', 'validate_range', 1, 'column_name', 'total_trips'),
(1201, 'data_quality', 'validate_range', 1, 'min_value', '1'),
(1201, 'data_quality', 'validate_range', 1, 'message', 'gold taxi_trip_metrics must aggregate at least one trip per group'),
(1201, 'data_quality', 'validate_range', 1, 'if_not_compliant', 'warn'),
-- ---------------------------------------------------------------------
-- Table_ID 1202 Gold housing_summary - bucket by house age, aggregate
-- ---------------------------------------------------------------------
-- Step 1: bucket houseage into readable categories.
(1202, 'data_transformation_steps', 'conditional_column', 1, 'column_name', 'house_age_bucket'),
(1202, 'data_transformation_steps', 'conditional_column', 1, 'conditions', 'houseage < 10|houseage < 25|houseage < 40|houseage >= 40'),
(1202, 'data_transformation_steps', 'conditional_column', 1, 'values', 'new|recent|established|old'),
(1202, 'data_transformation_steps', 'conditional_column', 1, 'values_delimiter', '|'),
(1202, 'data_transformation_steps', 'conditional_column', 1, 'default_value', 'unknown'),
-- Step 2: aggregate target (median house value) and income per bucket.
(1202, 'data_transformation_steps', 'aggregate_data', 2, 'group_by_columns', 'house_age_bucket'),
(1202, 'data_transformation_steps', 'aggregate_data', 2, 'column_name', 'target,medinc,latitude'),
(1202, 'data_transformation_steps', 'aggregate_data', 2, 'aggregation', 'avg,avg,count'),
(1202, 'data_transformation_steps', 'aggregate_data', 2, 'output_column_name', 'avg_target,avg_medinc,n_blocks'),
-- DQ: avg_target should be positive in every bucket.
(1202, 'data_quality', 'validate_range', 1, 'column_name', 'avg_target'),
(1202, 'data_quality', 'validate_range', 1, 'min_value', '0'),
(1202, 'data_quality', 'validate_range', 1, 'message', 'gold housing_summary avg_target must be positive'),
(1202, 'data_quality', 'validate_range', 1, 'if_not_compliant', 'warn'),
-- ---------------------------------------------------------------------
-- Table_ID 1203 Gold titanic_survival_summary - survival rate by class/sex
-- ---------------------------------------------------------------------
-- Aggregate: rows per (pclass, sex) plus mean survival rate and mean fare.
(1203, 'data_transformation_steps', 'aggregate_data', 1, 'group_by_columns', 'pclass,sex'),
(1203, 'data_transformation_steps', 'aggregate_data', 1, 'column_name', 'passengerid,survived,fare'),
(1203, 'data_transformation_steps', 'aggregate_data', 1, 'aggregation', 'count,avg,avg'),
(1203, 'data_transformation_steps', 'aggregate_data', 1, 'output_column_name', 'passenger_count,survival_rate,avg_fare'),
-- DQ: every (pclass, sex) group must have at least one passenger.
(1203, 'data_quality', 'validate_range', 1, 'column_name', 'passenger_count'),
(1203, 'data_quality', 'validate_range', 1, 'min_value', '1'),
(1203, 'data_quality', 'validate_range', 1, 'message', 'gold titanic_survival_summary must aggregate at least one passenger per group'),
(1203, 'data_quality', 'validate_range', 1, 'if_not_compliant', 'warn'),
-- DQ: survival_rate must be a probability between 0 and 1.
(1203, 'data_quality', 'validate_range', 2, 'column_name', 'survival_rate'),
(1203, 'data_quality', 'validate_range', 2, 'min_value', '0'),
(1203, 'data_quality', 'validate_range', 2, 'max_value', '1'),
(1203, 'data_quality', 'validate_range', 2, 'message', 'survival_rate must be between 0 and 1'),
(1203, 'data_quality', 'validate_range', 2, 'if_not_compliant', 'warn');
