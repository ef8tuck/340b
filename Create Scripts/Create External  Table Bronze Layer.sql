-- Databricks notebook source
CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_mnc_phs (
  CUSTOMER STRING COMMENT 'CUSTOMER',
  MEM STRING COMMENT 'MEM',
  IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_MNC_PHS' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_nat_grp (
RUN_DATE DATE,
	NAT_GRP STRING,
	NAT_SUB_GRP STRING,
	CUST_DSTRCT STRING,
	CUST_RGN STRING,
	ACM STRING,
	DC STRING,
	CAN STRING,
	CAN_NAME STRING,
	CUSID STRING,
	CUS_NAME STRING,
	TERMS STRING,
	TERMS_DESC STRING,
	CHAIN_ STRING,
	PREPAY_DAYS STRING,
	PREPAY STRING,
	PAY_METHOD STRING,
	PAY_METHOD_DESC STRING,
	ADDRESS STRING,
	CITY STRING,
	STATE STRING,
	ZIP STRING,
	DEA STRING,
	DSOP STRING,
IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
_rescued_data STRING COMMENT 'Rescued Data',
ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_NAT_GRP' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_phs_accounts (
CUST_ACCT_ID STRING,
	CUST_NAME STRING,
	STORE_NUM STRING,
	DEA_FAMILY STRING,
	DELIVERY_ROUTE_NUM STRING,
	DELIVERY_ROUTE_STOP_NUM STRING,
	MARKETING_CAMPAIGN STRING,
	HOME_DC_ID STRING,
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
	ZIP_CD_DELY STRING,
	ATTENTION_NAME_INV STRING,
	ADDR_LINE_1_INV STRING,
	ADDR_LINE_2_INV STRING,
	CITY_NAME_INV STRING,
	STATE_CD_INV STRING,
	ZIP_CD_INV STRING,
	CUST_CHAIN_ID STRING,
	CUST_CHAIN_NAME STRING,
	NATIONAL_GROUP_CD STRING,
	NATIONAL_GROUP_NAME STRING,
	NATIONAL_SUB_GROUP_CD STRING,
	NATIONAL_SUB_GROUP_NAME STRING,
	REGION_NUM STRING,
	REGION_NAME STRING,
	DISTRICT_NUM STRING,
	DISTRICT_NAME STRING,
	RX_BILL_PLAN_CD STRING,
	BUSINESS_TYPE_CD STRING,
	DISTRIBUTION_CHANNEL STRING,
	SALES_TERRITORY_ID STRING,
	PRIMARY_CUST_ID STRING,
	PROMO_SPEC_PRC_1 STRING,
	PROMO_SPEC_PRC_2 STRING,
	PROMO_SPEC_PRC_3 STRING,
	PROMO_SPEC_PRC_4 STRING,
	PROMO_SPEC_PRC_5 STRING,
	PROMO_SPEC_PRC_6 STRING,
	ACCOUNT_CLASSIFICATION STRING,
	ACCOUNT_CLASSIFICATION_DESCRIPTION STRING,
IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_PHS_ACCOUNTS' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_phs_accounts_generics (
  ACCOUNTS STRING COMMENT 'ACCOUNTS',
  IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_PHS_ACCOUNTS_GENERICS' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_pvp_report (
PVP_PARTICIPANT_ID STRING,
PVP_PARTICIPATION_FLAG STRING,
COMMITTED_LOP_LOCS STRING,
PVP_ENTITY_TYPE STRING,
PVP_340B_ID STRING,
PVP_PARENT_340B_ID STRING,
HRSA_TERM_CODE STRING,
HRSA_TERM_DATE STRING,
PVP_MEMBER_NAME STRING,
PVP_ADDRESS_HEADER STRING,
PVP_ADDRESS_LINE_1 STRING,
PVP_ADDRESS_LINE_2 STRING,
PVP_CITY STRING,
PVP_STATE STRING,
PVP_ZIP_CODE STRING,
PVP_ZIP_CODE_EXT STRING,
HRSA_MEMBER_DATE STRING,
PVP_EFFECTIVE_DATE STRING,
PVP_EXPIRATION_DATE STRING,
HIN_NUMBER STRING,
COVERED_ENTITY_DEA_NUMBER STRING,
PVP_BILLING_PARTICIPANT_NAME STRING,
PVP_BILLING_ADDRESS1 STRING,
PVP_BILLING_ADDRESS2 STRING,
PVP_BILLING_CITY STRING,
PVP_BILLING_STATE STRING,
PVP_BILLING_ZIP STRING,
PVP_BILLING_ZIP2 STRING,
PVP_SHIPPING_PARTICIPANT_NAME STRING,
PVP_SHIPPING_ADDRESS1 STRING,
PVP_SHIPPING_ADDRESS2 STRING,
PVP_SHIPPING_CITY STRING,
PVP_SHIPPING_STATE STRING,
PVP_SHIPPING_ZIP STRING,
PVP_SHIPPING_ZIP2 STRING,
PRIME_VENDOR_CONTACT1_NAME STRING,
PRIME_VENDOR_CONTACT1_EMAIL STRING,
PRIME_VENDOR_CONTACT1_TITLE STRING,
PRIME_VENDOR_CONTACT1_TELEPHONE_NUMBER STRING,
PRIME_VENDOR_CONTACT2_NAME STRING,
PRIME_VENDOR_CONTACT2_EMAIL STRING,
PRIME_VENDOR_CONTACT2_TITLE STRING,
PRIME_VENDOR_CONTACT2_TELEPHONE STRING,
PRIME_VENDOR_CONTACT3_NAME STRING,
PRIME_VENDOR_CONTACT3_EMAIL STRING,
PRIME_VENDOR_CONTACT3_TITLE STRING,
PRIME_VENDOR_CONTACT3_TELEPHONE STRING,
ENTITY_MEDICAID_NUMBER STRING,
ENTITY_GRANT_NUMBER STRING,
ALT_METHODS STRING,
PVP_SHIPPING_HIN STRING,
ORIGINAL_PVP_PARTICIPANT_ID STRING,
IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_PVP_REPORT' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);



-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.t_apexus_code_key (
  LEAD_NO STRING,
  CODE_KEY STRING,
  ORG_ID_SEQ STRING,
  CONTRACT_NAME STRING,
  VENDOR_NO STRING,
  VENDOR_NAME STRING,
  MATERIAL STRING,
  EAN_UPC STRING,
  MATERIAL_DESCRIPTION STRING,
  ORIGINAL_VALID_FROM STRING,
  ORIGINAL_VALID_TO STRING,
  BID_PRICE STRING,
  VARIANCE STRING,
  WAC STRING,
  NO_CHARGEBACK_INDICATOR STRING,
  GPO_CHBK_REF_NO STRING,
  GENERIC_FLAG STRING,
  ITEM_STATUS STRING,
  IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING
  COMMENT 'Rescued Data',
  ADF_RUN_ID STRING
  COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING
  COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp
  COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING
  COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING
  COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING
  COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME))
  COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME))
  COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_APEXUS_CODE_KEY'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles' = true
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.T_APEXUS_PHS_ACCT_BY_LEAD (
    LEAD_NO STRING,
    CODE_KEY STRING,
    ORG_ID_SEQ STRING,
    CONTRACT_NAME STRING,
    VENDOR_NO STRING,
    VENDOR_NAME STRING,
    MATERIAL STRING,
    EAN_UPC STRING,
    MATERIAL_DESCRIPTION STRING,
    ORIGINAL_VALID_FROM STRING,
    ORIGINAL_VALID_TO STRING,
    BID_PRICE STRING,
    VARIANCE STRING,
    WAC STRING,
    NO_CHARGEBACK_INDICATOR STRING,
    GPO_CHBK_REF_NO STRING,
    GENERIC_FLAG STRING,
    ITEM_STATUS STRING,
    IS_ACTIVE STRING DEFAULT 'Y' COMMENT 'Indicates if the record is active, default is Y',
  _rescued_data STRING COMMENT 'Rescued Data',
  ADF_RUN_ID STRING COMMENT 'ID for specific pipeline run loaded from landing. This run_id gets generated when the records gets loaded from source to landing',
  ADF_JOB_ID STRING COMMENT 'ID of the trigger that invokes the pipeline. This job_id gets generated when the records gets loaded from source to landing',
  RECORD_LOAD_TIME TIMESTAMP DEFAULT current_timestamp COMMENT 'This is the default generated column using current timestamp when the record is loaded in the table',
  INPUT_FILE_NAME STRING COMMENT 'This is extracted from the input file name of source',
  DATABRICKS_RUN_ID STRING COMMENT 'Run ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATABRICKS_JOB_ID STRING COMMENT 'Job ID of the Databricks job run. This gets generated from the bronze notebook run',
  DATE_PART DATE GENERATED ALWAYS AS (DATE(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from date part of the RECORD_LOAD_TIME column',
  HOUR_PART INT GENERATED ALWAYS AS (HOUR(RECORD_LOAD_TIME)) COMMENT 'This is autogenerated from hour part of the RECORD_LOAD_TIME column'
) 
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_APEXUS_PHS_ACCT_BY_LEAD' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);

