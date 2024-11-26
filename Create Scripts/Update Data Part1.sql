-- Databricks notebook source
CREATE TABLE IF NOT EXISTS PSAS_DI_DEV.340B_GOLD.T_PHS_AUDIT_PART2
USING DELTA
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported')
AS SELECT * FROM PSAS_DI_DEV.340B_slvr.T_PHS_AUDIT
;

-- ALTER TABLE PSAS_DI_DEV.340B_GOLD.T_PHS_AUDIT_PART2
-- SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');

-- COMMAND ----------

 CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_PHS_AUDIT_PART3 (
  CUST_ACCT_ID STRING COMMENT 'CUST_ACCT_ID',
  CUST_ACCT_NAME STRING COMMENT 'CUST_ACCT_NAME',
  ACCT_CLASSIFICATION STRING COMMENT 'ACCT_CLASSIFICATION',
  STATUS STRING COMMENT 'STATUS',
  PHS_340B_ID STRING COMMENT 'PHS_340B_ID',
  CUST_CHN_ID STRING COMMENT 'CUST_CHN_ID',
  CHAIN_NAM STRING COMMENT 'CHAIN_NAM',
  NATIONAL_GRP_CD STRING COMMENT 'NATIONAL_GRP_CD',
  NATIONAL_GRP_NAME STRING COMMENT 'NATIONAL_GRP_NAME',
  NATIONAL_SUBGRP_CD STRING COMMENT 'NATIONAL_SUBGRP_CD',
  NATIONAL_SUBGRP_NAME STRING COMMENT 'NATIONAL_SUBGRP_NAME',
  REGION_CD STRING COMMENT 'REGION_CD',
  REGION_NAME STRING COMMENT 'REGION_NAME',
  BUS_TYP_CD STRING COMMENT 'BUS_TYP_CD',
  DELIVERY_DOC STRING COMMENT 'DELIVERY_DOC',
  DIST_CHANNEL STRING COMMENT 'DIST_CHANNEL',
  HOME_DC_ID STRING COMMENT 'HOME_DC_ID',
  DC_NAME STRING COMMENT 'DC_NAME',
  SALES_ADMIN STRING COMMENT 'SALES_ADMIN',
  MHS_SALES_REP STRING COMMENT 'MHS_SALES_REP',
  SALES_REP STRING COMMENT 'SALES_REP',
  PHS_340B_ID_NOSUFFIX STRING COMMENT 'PHS_340B_ID_NOSUFFIX',
  CE_CITY STRING COMMENT 'CE_CITY',
  CE_STATE STRING COMMENT 'CE_STATE',
  DEA_FAMILY STRING COMMENT 'DEA_FAMILY',
  UPDATE_SAP STRING COMMENT 'UPDATE_SAP',
  COVERED_ENTITY_NAME STRING COMMENT 'COVERED_ENTITY_NAME',
  COVERED_ENTITY_PRIMARY STRING COMMENT 'COVERED_ENTITY_PRIMARY',
  CONTRACT_PHARMACY_NAME STRING COMMENT 'CONTRACT_PHARMACY_NAME',
  CONTRACT_PHARMACY_PRIMARY STRING COMMENT 'CONTRACT_PHARMACY_PRIMARY',
  CONTRACT_PHARMACY_RETAIL STRING COMMENT 'CONTRACT_PHARMACY_RETAIL',
  DSCSA_RECEIVED STRING COMMENT 'DSCSA_RECEIVED',
  UPDATE_TRACKER STRING COMMENT 'UPDATE_TRACKER',
  PVP_MEMBER_NAME STRING COMMENT 'PVP_MEMBER_NAME',
  PVP_PARTICIPANT_ID STRING COMMENT 'PVP_PARTICIPANT_ID',
  PVP_PARTICIPATION_FLAG STRING COMMENT 'PVP_PARTICIPATION_FLAG',
  PVP_ELIGIBILITY_DATE STRING COMMENT 'PVP_ELIGIBILITY_DATE',
  PVP_EXPIRATION_DATE STRING COMMENT 'PVP_EXPIRATION_DATE',
  HRSA_START_DATE STRING COMMENT 'HRSA_START_DATE',
  HRSA_TERM_DATE STRING COMMENT 'HRSA_TERM_DATE',
  ACTIVATION_DATE STRING COMMENT 'ACTIVATION_DATE',
  LEADS_PHS STRING COMMENT 'LEADS_PHS',
  LEADS_WAC STRING COMMENT 'LEADS_WAC',
  MNC STRING COMMENT 'MNC',
  ACCT_NAME_A34 STRING COMMENT 'ACCT_NAME_A34',
  ACCT_NAME_C34 STRING COMMENT 'ACCT_NAME_C34',
  ACCT_NAME_PHS STRING COMMENT 'ACCT_NAME_PHS',
  ENTITY_TYPE STRING COMMENT 'ENTITY_TYPE',
  ENTITY_TYPE_NAME STRING COMMENT 'ENTITY_TYPE_NAME',
  AUDIT_GPO STRING COMMENT 'AUDIT_GPO',
  AUDIT_GROUP_NAME STRING COMMENT 'AUDIT_GROUP_NAME',
  AUDIT_COMMENTS STRING COMMENT 'AUDIT_COMMENTS',
  AUDIT_STATUS STRING COMMENT 'AUDIT_STATUS',
  TOTAL_SALES DOUBLE COMMENT 'TOTAL_SALES',
  MPB_EXTENDED STRING COMMENT 'MPB_EXTENDED',
  AGREEMENT STRING COMMENT 'AGREEMENT',
  AGREEMENT_EXPIRATION STRING COMMENT 'AGREEMENT_EXPIRATION',
  PAYMENT_TERM STRING COMMENT 'PAYMENT_TERM',
  RETURNS_MATRIX STRING COMMENT 'RETURNS_MATRIX',
  CAN_NAM STRING COMMENT 'CAN_NAM',
  COMMENTS STRING COMMENT 'COMMENTS',
  RX_COGS STRING COMMENT 'RX_COGS',
  OTC_COGS STRING COMMENT 'OTC_COGS',
  ADDR_DELIVERY STRING COMMENT 'ADDR_DELIVERY',
  ROUTE STRING COMMENT 'ROUTE',
  STOP STRING COMMENT 'STOP',
  ADDR_INVOICE STRING COMMENT 'ADDR_INVOICE',
  DEA_NUM STRING COMMENT 'DEA_NUM',
  RX_BILL_PLAN STRING COMMENT 'RX_BILL_PLAN',
  SALES_LSTQTR STRING COMMENT 'SALES_LSTQTR',
  SALES_LSTMTH STRING COMMENT 'SALES_LSTMTH',
  SALES_CURMTH STRING COMMENT 'SALES_CURMTH',
  GPO_ID STRING COMMENT 'GPO_ID',
  GPO_ACCT STRING COMMENT 'GPO_ACCT',
  PVP_CODE STRING COMMENT 'PVP_CODE',
  PVP_CODING STRING COMMENT 'PVP_CODING',
  THIRD_PARTY_VENDOR STRING COMMENT 'THIRD_PARTY_VENDOR',
  ZX_BLOCK STRING COMMENT 'ZX_BLOCK',
  OPENED_FOR_RETURNS STRING COMMENT 'OPENED_FOR_RETURNS',
  ACM_NAME STRING COMMENT 'ACM_NAME',
  MAIN_LEAD STRING COMMENT 'MAIN_LEAD',
  UPDATE_ACTIVESTATUS STRING COMMENT 'UPDATE_ACTIVESTATUS',
  UPDATE_340BID STRING COMMENT 'UPDATE_340BID',
  FIRST_ORDER STRING COMMENT 'FIRST_ORDER',
  LAST_ORDER STRING COMMENT 'LAST_ORDER',
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
) USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_AUDIT_PART3'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true,
  'spark.sql.ansi.enabled'=false
);

-- COMMAND ----------

select * from PSAS_DI_DEV.340B_GOLD.T_PHS_AUDIT_PART2;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.T_MT_PVP_REPORT(
PVP_340B_ID STRING,
PVP_MEMBER_NAME STRING,
PVP_PARTICIPANT_ID BIGINT,
PVP_PARTICIPATION_FLAG STRING,
PVP_EFFECTIVE_DATE DATE,
PVP_EXPIRATION_DATE DATE,
HRSA_MEMBER_DATE DATE,
PVP_CITY STRING,
PVP_STATE STRING,
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
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_MT_PVP_REPORT' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS psas_di_dev.340b_brnz.T_HRSA(
PHS_340B_ID STRING ,
STATE STRING ,
City STRING ,
DELETE_COL STRING ,
COVERED_ENTITY STRING ,
Zip STRING ,
Address STRING ,
HRSA_TERM_DATE TIMESTAMP ,
HRSA_START_DATE TIMESTAMP,
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
USING DELTA PARTITIONED BY (DATE_PART, HOUR_PART) LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/brnz/T_HRSA' 
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'spark.sql.files.ignoreMissingFiles'=true
);

-- COMMAND ----------

--This query has all the columns to be updated in PHS_AUDIT but can't be executed as two tables are missing

INSERT INTO psas_di_dev.340b_gold.T_PHS_AUDIT_PART2(
ENTITY_TYPE,
    ENTITY_TYPE_NAME,
    ACCT_NAME_PHS,
    PHS_340B_ID_NOSUFFIX,
    PVP_CODE,
    PVP_CODING,
    MNC,
    ACM_NAME,
    HRSA_START_DATE,
    HRSA_TERM_DATE,
    COVERED_ENTITY_NAME,
    PVP_MEMBER_NAME,
    PVP_PARTICIPANT_ID,
    PVP_PARTICIPATION_FLAG,
    PVP_ELIGIBILITY_DATE,
    PVP_EXPIRATION_DATE,
    CE_CITY,
    CE_STATE
)
SELECT 
    -- For ENTITY_TYPE: Based on PHS_340B_ID's first 2 or 3 characters
    CASE 
        WHEN LEFT(T_PHS_AUDIT.PHS_340B_ID, 3) IN ('CH0','CH1') OR LEFT(T_PHS_AUDIT.PHS_340B_ID, 2) IN ('FP','HM','HV','NH','TB','UI','RW')
        THEN LEFT(T_PHS_AUDIT.PHS_340B_ID, 2)
        ELSE t_PHS_AUDIT.ENTITY_TYPE
    END AS ENTITY_TYPE,

    -- For ENTITY_TYPE_NAME: Join with LUTL_ENTITY based on ENTITY_TYPE
    CASE 
        WHEN T_PHS_AUDIT.ENTITY_TYPE = LUTL_ENTITY.ENTITY_TYPE
        THEN LUTL_ENTITY.ENTITY_NAME
        ELSE ENTITY_TYPE_NAME
    END AS ENTITY_TYPE_NAME,

    -- For ACCT_NAME_PHS: Based on CUST_ACCT_NAME conditions
    CASE 
        WHEN (CHARINDEX(' PHS', CUST_ACCT_NAME) > 0) OR 
             ((CHARINDEX('PHS', CUST_ACCT_NAME) > 0) AND (LEFT(CUST_ACCT_NAME, 5) <> 'USPHS') AND (CHARINDEX('JOSEPHS', CUST_ACCT_NAME) = 0))
        THEN 'Y'
        ELSE ACCT_NAME_PHS
    END AS ACCT_NAME_PHS,

    -- For PHS_340B_ID_NOSUFFIX: Trim to left of hyphen if exists
    CASE 
        WHEN (PHS_340B_ID_NOSUFFIX IS NOT NULL AND PHS_340B_ID_NOSUFFIX <> '') AND CHARINDEX('-', PHS_340B_ID_NOSUFFIX) > 0
        THEN LEFT(PHS_340B_ID_NOSUFFIX, CHARINDEX('-', PHS_340B_ID_NOSUFFIX) - 1)
        WHEN (PHS_340B_ID_NOSUFFIX IS NOT NULL AND PHS_340B_ID_NOSUFFIX <> '') AND REGEXP_LIKE(RIGHT(PHS_340B_ID_NOSUFFIX, 1), '[0-9]') = FALSE
        THEN LEFT(PHS_340B_ID_NOSUFFIX, LEN(PHS_340B_ID_NOSUFFIX) - 1)
        ELSE PHS_340B_ID_NOSUFFIX
    END AS PHS_340B_ID_NOSUFFIX,

    -- For PVP_CODE: Concatenate CUST_CHN_ID with NATIONAL_SUBGRP_CD (or '000000' if empty)
    CASE 
        WHEN (ACCT_CLASSIFICATION = '003' OR ACCT_CLASSIFICATION IN ('004', '005')) 
             OR (NATIONAL_GRP_CD = '0240' AND UPPER(STATUS) = 'ACTIVE')
        THEN CUST_CHN_ID || CASE WHEN TRIM(NATIONAL_SUBGRP_CD) <> '' THEN NATIONAL_SUBGRP_CD ELSE '000000' END
        ELSE PVP_CODE
    END AS PVP_CODE,

    -- For PVP_CODING: Join with LUTL_PVP_CODING table to set the value based on PVP_CODE
    CASE 
        WHEN (ACCT_CLASSIFICATION = '003' OR ACCT_CLASSIFICATION IN ('004', '005')) 
             OR (NATIONAL_GRP_CD = '0240')
        THEN CASE WHEN LUTL_PVP_CODING.PVP_CODING = 'Y' THEN 'Y' ELSE 'N' END
        ELSE T_PHS_AUDIT.PVP_CODING
    END AS PVP_CODING,

    -- For MNC: Based on CUST_ACCT_ID and matching records in T_MNC_PHS
    CASE 
        WHEN MNC_PHS.MEM = 'A34' THEN 'A34'
        WHEN MNC_PHS.MEM = 'C34' THEN T_PHS_AUDIT.MNC || 'C34'
        ELSE MNC
    END AS MNC,
    

    -- For ACM_NAME: Based on matching CUST_ACCT_ID in T_Nat_Grp
    CASE 
        WHEN T_PHS_AUDIT.CUST_ACCT_ID = Nat_Grp.CUSID THEN Nat_Grp.ACM
        ELSE ACM_NAME
    END AS ACM_NAME,

    -- -- For HRSA fields: Based on matching PHS_340B_ID in T_HRSA
    CASE 
        WHEN T_PHS_AUDIT.PHS_340B_ID = T_HRSA.PHS_340B_ID THEN T_HRSA.HRSA_START_DATE
        ELSE T_PHS_AUDIT.HRSA_START_DATE
    END AS HRSA_START_DATE,
    
    CASE 
        WHEN T_PHS_AUDIT.PHS_340B_ID = T_HRSA.PHS_340B_ID THEN T_HRSA.HRSA_TERM_DATE
        ELSE T_PHS_AUDIT.HRSA_TERM_DATE
    END AS HRSA_TERM_DATE,

    CASE 
        WHEN T_PHS_AUDIT.PHS_340B_ID = T_HRSA.PHS_340B_ID THEN T_HRSA.COVERED_ENTITY
        ELSE T_PHS_AUDIT.COVERED_ENTITY_NAME
    END AS COVERED_ENTITY_NAME,

    -- For PVP fields: Based on ACCT_CLASSIFICATION and matching with PVP reports
    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE T_PHS_AUDIT.PVP_MEMBER_NAME
    END AS PVP_MEMBER_NAME,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE T_PHS_AUDIT.PVP_PARTICIPANT_ID
    END AS PVP_PARTICIPANT_ID,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN 'N'
        ELSE T_PHS_AUDIT.PVP_PARTICIPATION_FLAG
    END AS PVP_PARTICIPATION_FLAG,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN NULL
        ELSE T_PHS_AUDIT.PVP_ELIGIBILITY_DATE
    END AS PVP_ELIGIBILITY_DATE,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN NULL
        ELSE T_PHS_AUDIT.PVP_EXPIRATION_DATE
    END AS PVP_EXPIRATION_DATE,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE T_PHS_AUDIT.CE_CITY
    END AS CE_CITY,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE T_PHS_AUDIT.CE_STATE
    END AS CE_STATE

FROM 
    psas_di_dev.340b_brnz.T_PHS_AUDIT AS T_PHS_AUDIT
LEFT JOIN 
    psas_di_dev.340b_brnz.T_LUTL_ENTITY AS LUTL_ENTITY ON T_PHS_AUDIT.ENTITY_TYPE = LUTL_ENTITY.ENTITY_TYPE
LEFT JOIN 
    psas_di_dev.340b_brnz.T_LUTL_PVP_CODING AS LUTL_PVP_CODING ON T_PHS_AUDIT.PVP_CODE = LUTL_PVP_CODING.Coding
LEFT JOIN 
    psas_di_dev.340b_brnz.T_MNC_PHS AS MNC_PHS ON MNC_PHS.CUSTOMER = T_PHS_AUDIT.CUST_ACCT_ID
LEFT JOIN 
    psas_di_dev.340b_brnz.T_Nat_Grp AS Nat_Grp ON T_PHS_AUDIT.CUST_ACCT_ID = Nat_Grp.CUSID
LEFT JOIN 
    psas_di_dev.340b_brnz.t_hrsa AS T_HRSA ON T_PHS_AUDIT.PHS_340B_ID = T_HRSA.PHS_340B_ID
LEFT JOIN 
    psas_di_dev.340b_brnz.t_mt_pvp_report AS MT_PVP_REPORT ON T_PHS_AUDIT.PHS_340B_ID = MT_PVP_REPORT.PVP_340B_ID
LEFT JOIN 
    psas_di_dev.340b_brnz.t_mt_pvp_report_parentmatch AS MT_PVP_REPORT_PARENTMATCH ON T_PHS_AUDIT.PHS_340B_ID = MT_PVP_REPORT_PARENTMATCH.PVP_340B_ID

-- COMMAND ----------

--query without T_HRSA and T_MT_PVP_REPORT

INSERT INTO psas_di_dev.340b_gold.T_PHS_AUDIT_PART2 (
    ENTITY_TYPE,
    ENTITY_TYPE_NAME,
    ACCT_NAME_PHS,
    PHS_340B_ID_NOSUFFIX,
    PVP_CODE,
    PVP_CODING,
    MNC,
    ACM_NAME,
    HRSA_START_DATE,
    HRSA_TERM_DATE,
    COVERED_ENTITY_NAME,
    PVP_MEMBER_NAME,
    PVP_PARTICIPANT_ID,
    PVP_PARTICIPATION_FLAG,
    PVP_ELIGIBILITY_DATE,
    PVP_EXPIRATION_DATE,
    CE_CITY,
    CE_STATE
)
SELECT 
    CASE 
        WHEN LEFT(PHS_340B_ID, 3) IN ('CH0','CH1') OR LEFT(PHS_340B_ID, 2) IN ('FP','HM','HV','NH','TB','UI','RW')
        THEN LEFT(PHS_340B_ID, 2)
        ELSE T_PHS_AUDIT.ENTITY_TYPE
    END AS ENTITY_TYPE,

    CASE 
        WHEN T_PHS_AUDIT.ENTITY_TYPE = LUTL_ENTITY.ENTITY_TYPE
        THEN LUTL_ENTITY.ENTITY_NAME
        ELSE ENTITY_TYPE_NAME
    END AS ENTITY_TYPE_NAME,

    CASE 
        WHEN (INSTR(CUST_ACCT_NAME, ' PHS') > 0) OR 
             ((INSTR(CUST_ACCT_NAME, 'PHS') > 0) AND (LEFT(CUST_ACCT_NAME, 5) <> 'USPHS') AND (INSTR(CUST_ACCT_NAME, 'JOSEPHS') = 0))
        THEN 'Y'
        ELSE ACCT_NAME_PHS
    END AS ACCT_NAME_PHS,

    CASE 
        WHEN (PHS_340B_ID_NOSUFFIX IS NOT NULL AND PHS_340B_ID_NOSUFFIX <> '') AND INSTR(PHS_340B_ID_NOSUFFIX, '-') > 0
        THEN LEFT(PHS_340B_ID_NOSUFFIX, INSTR(PHS_340B_ID_NOSUFFIX, '-') - 1)
        WHEN (PHS_340B_ID_NOSUFFIX IS NOT NULL AND PHS_340B_ID_NOSUFFIX <> '') AND REGEXP_LIKE(RIGHT(PHS_340B_ID_NOSUFFIX, 1), '[0-9]') = FALSE
        THEN LEFT(PHS_340B_ID_NOSUFFIX, LENGTH(PHS_340B_ID_NOSUFFIX) - 1)
        ELSE PHS_340B_ID_NOSUFFIX
    END AS PHS_340B_ID_NOSUFFIX,

    CASE 
        WHEN (ACCT_CLASSIFICATION = '003' OR ACCT_CLASSIFICATION IN ('004', '005')) 
             OR (NATIONAL_GRP_CD = '0240' AND UPPER(STATUS) = 'ACTIVE')
        THEN CONCAT(CUST_CHN_ID, CASE WHEN TRIM(NATIONAL_SUBGRP_CD) <> '' THEN NATIONAL_SUBGRP_CD ELSE '000000' END)
        ELSE PVP_CODE
    END AS PVP_CODE,

    CASE 
        WHEN (ACCT_CLASSIFICATION = '003' OR ACCT_CLASSIFICATION IN ('004', '005')) 
             OR (NATIONAL_GRP_CD = '0240')
        THEN (CASE WHEN LUTL_PVP_CODING.PVP_CODING = 'Y' THEN 'Y' ELSE 'N' END)
        ELSE T_PHS_AUDIT.PVP_CODING
    END AS PVP_CODING,

    CASE 
        WHEN MNC_PHS.MEM = 'A34' THEN 'A34'
        WHEN MNC_PHS.MEM = 'C34' THEN CONCAT(T_PHS_AUDIT.MNC, 'C34')
        ELSE MNC
    END AS MNC,

    CASE 
        WHEN T_PHS_AUDIT.CUST_ACCT_ID = Nat_Grp.CUSID THEN Nat_Grp.ACM
        ELSE ACM_NAME
    END AS ACM_NAME,

    NULL AS HRSA_START_DATE,
    NULL AS HRSA_TERM_DATE,
    NULL AS COVERED_ENTITY_NAME,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE PVP_MEMBER_NAME
    END AS PVP_MEMBER_NAME,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE try_cast(PVP_PARTICIPANT_ID AS STRING)
    END AS PVP_PARTICIPANT_ID,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN 'N'
        ELSE PVP_PARTICIPATION_FLAG
    END AS PVP_PARTICIPATION_FLAG,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN NULL
        ELSE PVP_ELIGIBILITY_DATE
    END AS PVP_ELIGIBILITY_DATE,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN NULL
        ELSE PVP_EXPIRATION_DATE
    END AS PVP_EXPIRATION_DATE,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE CE_CITY
    END AS CE_CITY,

    CASE 
        WHEN (T_PHS_AUDIT.ACCT_CLASSIFICATION = '003' OR T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('004', '005')) THEN ''
        ELSE CE_STATE
    END AS CE_STATE

FROM 
    psas_di_dev.340b_brnz.T_PHS_AUDIT AS T_PHS_AUDIT
LEFT JOIN 
    psas_di_dev.340b_brnz.t_lutl_entity AS LUTL_ENTITY ON T_PHS_AUDIT.ENTITY_TYPE = LUTL_ENTITY.ENTITY_TYPE
LEFT JOIN 
    psas_di_dev.340b_brnz.t_lutl_pvp_coding AS LUTL_PVP_CODING ON T_PHS_AUDIT.PVP_CODE = LUTL_PVP_CODING.Coding
LEFT JOIN 
    psas_di_dev.340b_slvr.T_MNC_PHS AS MNC_PHS ON MNC_PHS.CUSTOMER = T_PHS_AUDIT.CUST_ACCT_ID
LEFT JOIN 
    psas_di_dev.340b_slvr.T_Nat_Grp AS Nat_Grp ON T_PHS_AUDIT.CUST_ACCT_ID = Nat_Grp.CUSID;

-- COMMAND ----------


