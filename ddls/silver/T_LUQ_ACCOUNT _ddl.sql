CREATE TABLE IF NOT EXISTS $$unity_catalog_name.$$schema_name.$$delta_table_name (
  ID STRING COMMENT 'ID',
  ACCOUNT STRING COMMENT 'ACCOUNT',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  INSERT_TS TIMESTAMP COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  UPDATE_TS TIMESTAMP COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION '$$delta_table_location' 
TBLPROPERTIES (
 'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);