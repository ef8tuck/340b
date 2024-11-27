CREATE TABLE IF NOT EXISTS t_phs_accounts (
CUST_ACCT_ID BIGINT,
CUST_NAME STRING,
STORE_NUM STRING,
DEA_FAMILY STRING,
DELIVERY_ROUTE_NUM STRING,
DELIVERY_ROUTE_STOP_NUM STRING,
MARKETING_CAMPAIGN STRING,
HOME_DC_ID BIGINT,
DEA_NUM STRING,
HIN_BASE_CD STRING,
HIN_DEPT_CD STRING,
HIN_LOCATION_CD STRING,
HIN_NUM STRING,
ID_340B STRING,
ATTENTION_NAME_DELY STRING,
ADDR_LINE_1_DELY STRING,
ADDR_LINE_2_DELY STRING,
CITY_NAME_DELY STRING,
STATE_CD_DELY STRING,
ZIP_CD_DELY BIGINT,
ATTENTION_NAME_INV STRING,
ADDR_LINE_1_INV STRING,
ADDR_LINE_2_INV STRING,
CITY_NAME_INV STRING,
STATE_CD_INV STRING,
ZIP_CD_INV BIGINT,
CUST_CHAIN_ID BIGINT,
CUST_CHAIN_NAME STRING,
NATIONAL_GROUP_CD BIGINT,
NATIONAL_GROUP_NAME STRING,
NATIONAL_SUB_GROUP_CD BIGINT,
NATIONAL_SUB_GROUP_NAME STRING,
REGION_NUM BIGINT,
REGION_NAME STRING,
DISTRICT_NUM BIGINT,
DISTRICT_NAME STRING,
RX_BILL_PLAN_CD BIGINT,
BUSINESS_TYPE_CD BIGINT,
DISTRIBUTION_CHANNEL BIGINT,
SALES_TERRITORY_ID BIGINT,
PRIMARY_CUST_ID BIGINT,
PROMO_SPEC_PRC_1 STRING,
PROMO_SPEC_PRC_2 STRING,
PROMO_SPEC_PRC_3 STRING,
PROMO_SPEC_PRC_4 STRING,
PROMO_SPEC_PRC_5 STRING,
PROMO_SPEC_PRC_6 STRING,
ACCOUNT_CLASSIFICATION BIGINT,
ACCOUNT_CLASSIFICATION_DESCRIPTION STRING,
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

