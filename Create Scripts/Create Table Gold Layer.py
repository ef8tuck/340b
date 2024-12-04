# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.`340b_gold`.t_phs_audit_part1 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   DIST_CHANNEL STRING,
# MAGIC   HOME_DC_ID STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   DEA_FAMILY STRING,
# MAGIC   ACTIVE_CUST_IND STRING,
# MAGIC   UPDATE_SAP DATE,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   ENTITY_TYPE STRING,
# MAGIC   COGS_RX DOUBLE,
# MAGIC   COGS_OTC DOUBLE,
# MAGIC   ADDR_DELIVERY STRING,
# MAGIC   ADDR_INVOICE STRING,
# MAGIC   DEA_NUM STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   RX_BILL_PLAN STRING,
# MAGIC   SALES_LSTMTH DOUBLE,
# MAGIC   SALES_CURMTH DOUBLE,
# MAGIC   GPO_ID STRING,
# MAGIC   GPO_ACCT STRING,
# MAGIC   DLVRY_RTE_NUM STRING,
# MAGIC   DLVRY_RTE_STOP_NUM STRING,
# MAGIC   REGION_CD STRING,
# MAGIC   DELIVERY_DOC STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   FIRST_ORDER DATE,
# MAGIC   UPDATE_ACTIVESTATUS DATE,
# MAGIC   UPDATE_340BID DATE,
# MAGIC   LAST_ORDER DATE,
# MAGIC   SALES_REP STRING,
# MAGIC   REGION_NAME STRING
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_AUDIT_PART1'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.`340b_gold`.t_phs_audit_2 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   STATUS STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   REGION_CD STRING,
# MAGIC   REGION_NAME STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   DELIVERY_DOC STRING,
# MAGIC   DIST_CHANNEL STRING,
# MAGIC   HOME_DC_ID STRING,
# MAGIC   DC_NAME STRING,
# MAGIC   SALES_ADMIN STRING,
# MAGIC   MHS_SALES_REP STRING,
# MAGIC   SALES_REP STRING,
# MAGIC   PHS_340B_ID_NOSUFFIX STRING,
# MAGIC   CE_CITY STRING,
# MAGIC   CE_STATE STRING,
# MAGIC   DEA_FAMILY STRING,
# MAGIC   UPDATE_SAP DATE,
# MAGIC   COVERED_ENTITY_NAME STRING,
# MAGIC   COVERED_ENTITY_PRIMARY STRING,
# MAGIC   CONTRACT_PHARMACY_NAME STRING,
# MAGIC   CONTRACT_PHARMACY_PRIMARY STRING,
# MAGIC   CONTRACT_PHARMACY_RETAIL STRING,
# MAGIC   DSCSA_RECEIVED STRING,
# MAGIC   UPDATE_TRACKER DATE,
# MAGIC   PVP_MEMBER_NAME STRING,
# MAGIC   PVP_PARTICIPANT_ID STRING,
# MAGIC   PVP_PARTICIPATION_FLAG STRING,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   PVP_EXPIRATION_DATE DATE,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   LEADS_PHS STRING,
# MAGIC   LEADS_WAC STRING,
# MAGIC   MNC STRING,
# MAGIC   ACCT_NAME_A34 STRING,
# MAGIC   ACCT_NAME_C34 STRING,
# MAGIC   ACCT_NAME_PHS STRING,
# MAGIC   ENTITY_TYPE STRING,
# MAGIC   ENTITY_TYPE_NAME STRING,
# MAGIC   AUDIT_GPO STRING,
# MAGIC   AUDIT_GROUP_NAME STRING,
# MAGIC   AUDIT_COMMENTS STRING,
# MAGIC   AUDIT_STATUS STRING,
# MAGIC   TOTAL_SALES DOUBLE,
# MAGIC   MPB_EXTENDED STRING,
# MAGIC   AGREEMENT STRING,
# MAGIC   AGREEMENT_EXPIRATION DATE,
# MAGIC   PAYMENT_TERM STRING,
# MAGIC   RETURNS_MATRIX STRING,
# MAGIC   CAN_NAM STRING,
# MAGIC   COMMENTS STRING,
# MAGIC   RX_COGS DOUBLE,
# MAGIC   OTC_COGS DOUBLE,
# MAGIC   ADDR_DELIVERY STRING,
# MAGIC   ROUTE STRING,
# MAGIC   STOP STRING,
# MAGIC   ADDR_INVOICE STRING,
# MAGIC   DEA_NUM STRING,
# MAGIC   RX_BILL_PLAN STRING,
# MAGIC   SALES_LSTQTR DOUBLE,
# MAGIC   SALES_LSTMTH DOUBLE,
# MAGIC   SALES_CURMTH DOUBLE,
# MAGIC   GPO_ID STRING,
# MAGIC   GPO_ACCT STRING,
# MAGIC   PVP_CODE STRING,
# MAGIC   PVP_CODING STRING,
# MAGIC   THIRD_PARTY_VENDOR STRING,
# MAGIC   ZX_BLOCK DATE,
# MAGIC   OPENED_FOR_RETURNS DATE,
# MAGIC   ACM_NAME STRING,
# MAGIC   MAIN_LEAD STRING,
# MAGIC   UPDATE_ACTIVESTATUS DATE,
# MAGIC   UPDATE_340BID DATE,
# MAGIC   FIRST_ORDER DATE,
# MAGIC   LAST_ORDER DATE
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/t_phs_audit_2' 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE psas_di_dev.340b_gold.MHS_RNA_CUST_ACCT (
# MAGIC     CUST_ACCT_ID STRING,
# MAGIC     CUST_ACCT_NAM STRING,
# MAGIC     NATL_GRP_CD STRING,
# MAGIC     NATL_GRP_NAM STRING,
# MAGIC     NATL_SUB_GRP_CD STRING,
# MAGIC     NATL_SUB_GRP_NAM STRING,
# MAGIC     ACCT_CHN_ID STRING,
# MAGIC     CHAIN_NAM STRING,
# MAGIC     CUST_RGN_NUM STRING,
# MAGIC     CUST_RGN_NAM STRING,
# MAGIC     CUST_DSTRCT_NUM STRING,
# MAGIC     CUST_DSTRCT_NAM STRING,
# MAGIC     MCK_KU_DSCSA STRING,
# MAGIC     MCK_KU_ACCT_CLAS STRING,
# MAGIC     MCK_KU_ACCT_CLAS_DESC STRING,
# MAGIC     MCK_KU_GPOS STRING,
# MAGIC     MCK_KU_GPOS_DESC STRING,
# MAGIC     MCK_KU_GPO_ACCT STRING
# MAGIC )USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/MHS_RNA_CUST_ACCT' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_NON_ORDERING_ACCOUNTS (
# MAGIC   Cust_Acct_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   STATUS STRING
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_NON_ORDERING_ACCOUNTS' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE psas_di_dev.340b_gold.T_ACCOUNT_CLASSIFICATION_02 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ROUTE STRING,
# MAGIC   STOP STRING,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   UPDATE_SAP TIMESTAMP,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   STATUS STRING,
# MAGIC   PVP_CODING STRING,
# MAGIC   COMMENTS STRING
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_ACCOUNT_CLASSIFICATION_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_Accounts_Generics_Duplicates_01 (
# MAGIC   ACCOUNTS STRING COMMENT 'Account Number'
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_Accounts_Generics_Duplicates_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_Accounts_Generics_Coding_02 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   PVP_PARTICIPANT_ID STRING,
# MAGIC   PVP_PARTICIPATION_FLAG STRING,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   STATUS STRING,
# MAGIC   PVP_CODING STRING,
# MAGIC   Duplicate STRING
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_Accounts_Generics_Coding_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_ACCOUNTCONFIRMATION_LOOKUP_01 (
# MAGIC   SHIP_TO_ACCT_NO STRING,
# MAGIC   ENTITY_NAME STRING,
# MAGIC   SHIP_TO_ACCT_NAME STRING,
# MAGIC   APEXUS_340B_ID STRING,
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   STATUS STRING
# MAGIC )
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_ACCOUNTCONFIRMATION_LOOKUP_01' 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true,
# MAGIC   'delta.autoOptimize.optimizeWrite' = true);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.`340b_gold`.T_APEXUS_CODE_KEY_01 (
# MAGIC     Lead_No BIGINT,
# MAGIC     Org_ID_Seq STRING,
# MAGIC     Contract_Name STRING,
# MAGIC     Material BIGINT,
# MAGIC     EAN_UPC STRING,
# MAGIC     Material_Description STRING,
# MAGIC     Original_Valid_From INT,
# MAGIC     Original_Valid_To INT,
# MAGIC     Bid_Price DOUBLE,
# MAGIC     GPO_Chbk_Ref_No STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_CODE_KEY_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_WAC_ACCT_BY_LEAD_04 (
# MAGIC   Lead_No BIGINT,
# MAGIC   Org_ID_Seq STRING,
# MAGIC   Contract_Name STRING,
# MAGIC   Material BIGINT,
# MAGIC   EAN_UPC STRING,
# MAGIC   Material_Description STRING,
# MAGIC   Original_Valid_From INT,
# MAGIC   Original_Valid_To INT,
# MAGIC   Bid_Price DOUBLE,
# MAGIC   GPO_Chbk_Ref_No STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_WAC_ACCT_BY_LEAD_04' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_PHS_ACCT_BY_LEAD_02 (
# MAGIC   Lead_No BIGINT,
# MAGIC   Org_ID_Seq STRING,
# MAGIC   Contract_Name STRING,
# MAGIC   Material BIGINT,
# MAGIC   EAN_UPC STRING,
# MAGIC   Material_Description STRING,
# MAGIC   Original_Valid_From INT,
# MAGIC   Original_Valid_To INT,
# MAGIC   Bid_Price DOUBLE,
# MAGIC   GPO_Chbk_Ref_No STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_PHS_ACCT_BY_LEAD_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_WAC_ACCOUNTS_05 (
# MAGIC   Lead_No BIGINT,
# MAGIC   Org_ID_Seq STRING,
# MAGIC   Contract_Name STRING,
# MAGIC   Material bigint,
# MAGIC   EAN_UPC STRING,
# MAGIC   Material_Description STRING,
# MAGIC   Original_Valid_From int,
# MAGIC   Original_Valid_To INT,
# MAGIC   Bid_Price DOUBLE,
# MAGIC   GPO_Chbk_Ref_No STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_WAC_ACCOUNTS_05' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_ACCOUNTS_03 (
# MAGIC   Lead_No STRING,
# MAGIC   Org_ID_Seq STRING,
# MAGIC   Contract_Name STRING,
# MAGIC   Material STRING,
# MAGIC   EAN_UPC STRING,
# MAGIC   Material_Description STRING,
# MAGIC   Original_Valid_From DATE,
# MAGIC   Original_Valid_To DATE,
# MAGIC   Bid_Price DECIMAL(10, 2),
# MAGIC   GPO_Chbk_Ref_No STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_ACCOUNTS_03' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_WAC_ACCT_BY_LEAD_CHECK_04B (
# MAGIC   Lead_No BIGINT,
# MAGIC   LEAD_Unformatted STRING,
# MAGIC   WAC STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_WAC_ACCT_BY_LEAD_CHECK_04B' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table psas_di_dev.340b_gold.T_PHS_ACCT_BY_LEAD_CHECK_02B (
# MAGIC   Lead_No string,
# MAGIC   LEAD_Unformatted string,
# MAGIC   PHS String
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_ACCT_BY_LEAD_CHECK_02B' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_BUSTYPE_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   UPDATE_SAP DATE,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   DEA_NUM STRING,
# MAGIC   DELIVERY_DOC STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   PVP_Flag STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   COMMENTS STRING,
# MAGIC   SALES_LSTMTH STRING,
# MAGIC   SALES_CURMTH STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_BUSTYPE_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_ACCOUNTS (
# MAGIC   `CUST_ACCT_ID` bigint,
# MAGIC   `CUST_NAME` string,
# MAGIC   `STORE_NUM` string,
# MAGIC   `DEA_FAMILY` string,
# MAGIC   `MARKETING_CAMPAIGN` string,
# MAGIC   `HOME_DC_ID` bigint,
# MAGIC   `DEA_NUM` string,
# MAGIC   `HIN_BASE_CD` string,
# MAGIC   `HIN_DEPT_CD` string,
# MAGIC   `HIN_LOCATION_CD` string,
# MAGIC   `HIN_NUM` string,
# MAGIC   `340B_ID` string,
# MAGIC   `ATTENTION_NAME_DELY` string,
# MAGIC   `ADDR_LINE_1_DELY` string,
# MAGIC   `ADDR_LINE_2_DELY` string,
# MAGIC   `CITY_NAME_DELY` string,
# MAGIC   `STATE_CD_DELY` string,
# MAGIC   `ZIP_CD_DELY` bigint,
# MAGIC   `ATTENTION_NAME_INV` string,
# MAGIC   `ADDR_LINE_1_INV` string,
# MAGIC   `ADDR_LINE_2_INV` string,
# MAGIC   `CITY_NAME_INV` string,
# MAGIC   `STATE_CD_INV` string,
# MAGIC   `ZIP_CD_INV` bigint,
# MAGIC   `CUST_CHAIN_ID` bigint,
# MAGIC   `CUST_CHAIN_NAME` string,
# MAGIC   `NATIONAL_GROUP_CD` bigint,
# MAGIC   `NATIONAL_GROUP_NAME` string,
# MAGIC   `NATIONAL_SUB_GROUP_CD` bigint,
# MAGIC   `NATIONAL_SUB_GROUP_NAME` string,
# MAGIC   `REGION_NUM` bigint,
# MAGIC   `REGION_NAME` string,
# MAGIC   `DISTRICT_NUM` bigint,
# MAGIC   `DISTRICT_NAME` string,
# MAGIC   `RX_BILL_PLAN_CD` bigint,
# MAGIC   `BUSINESS_TYPE_CD` bigint,
# MAGIC   `DISTRIBUTION_CHANNEL` bigint,
# MAGIC   `SALES_TERRITORY_ID` bigint,
# MAGIC   `PRIMARY_CUST_ID` bigint,
# MAGIC   `PROMO1` string,
# MAGIC   `PROMO2` string,
# MAGIC   `PROMO3` string,
# MAGIC   `PROMO4` string,
# MAGIC   `PROMO5` string,
# MAGIC   `PROMO6` string,
# MAGIC   `ACCOUNT_CLASSIFICATION` bigint,
# MAGIC   `ACCOUNT_CLASSIFICATION_DESCRIPTION` string
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_ACCOUNTS' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Create Roster tables left
# MAGIC dmem submission

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE psas_di_dev.340b_gold.T_DELIVERY_DOCUMENT_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   UPDATE_SAP STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   DELIVERY_DOC STRING,
# MAGIC   CONTRACT_PHARMACY_RETAIL STRING,
# MAGIC   COMMENTS STRING
# MAGIC )
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_DELIVERY_DOCUMENT_01' 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true,
# MAGIC   'delta.autoOptimize.optimizeWrite' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_DELIVERY_DOCUMENT_CP_PAYS_BILL_02 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   DELIVERY_DOC STRING,
# MAGIC   CONTRACT_PHARMACY_RETAIL STRING
# MAGIC )
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_DELIVERY_DOCUMENT_CP_PAYS_BILL_02' 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true,
# MAGIC   'delta.autoOptimize.optimizeWrite' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  psas_di_dev.340b_gold.t_contractpharmacy_exceptions(
# MAGIC   `ID` STRING,
# MAGIC   RETAIL_CHAIN STRING
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/t_contractpharmacy_exceptions' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_HRSA_TERMED_200 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   COMMENTS STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   COVERED_ENTITY_NAME STRING,
# MAGIC   DC_NAME STRING,
# MAGIC   SALES_ADMIN STRING,
# MAGIC   MHS_SALES_REP STRING,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   OPENED_FOR_RETURNS STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_HRSA_TERMED_200' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE psas_di_dev.340b_gold.T_APEXUS_ELIGIBILITY_WAC_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   MNC STRING,
# MAGIC   ENTITY_TYPE STRING,
# MAGIC   ROUTE STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   PVP_MEMBER_NAME STRING,
# MAGIC   PVP_Flag STRING,
# MAGIC   Acct_Type STRING,
# MAGIC   Apexus_Eligible DATE,
# MAGIC   PVP_EXPIRATION_DATE DATE,
# MAGIC   PVP_PARTICIPANT_ID STRING,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   COMMENTS STRING,
# MAGIC   STATUS STRING,
# MAGIC   NATIONAL_GRP_CD STRING
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_ELIGIBILITY_WAC_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_ELIGIBILITY_PHS_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   SALES_ADMIN STRING,
# MAGIC   MHS_SALES_REP STRING,
# MAGIC   SALES_REP STRING,
# MAGIC   Chain STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   REGION_CD STRING,
# MAGIC   OPENED_FOR_RETURNS STRING,
# MAGIC   Combine STRING,
# MAGIC   PVP_Flag STRING,
# MAGIC   PVP_Coding STRING,
# MAGIC   PVP_Eligible DATE,
# MAGIC   PVP_Expiration DATE,
# MAGIC   HRSA_Start DATE,
# MAGIC   HRSA_Term DATE,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_ELIGIBILITY_PHS_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_HRSA_TERMED_WITH_APEXUS_LEADS_02 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATL_GROUP STRING,
# MAGIC   SUB_GROUP STRING,
# MAGIC   SUB_GROUP_NAME STRING,
# MAGIC   SALES_LSTMTH STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_HRSA_TERMED_WITH_APEXUS_LEADS_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_OVERRIDE_LEADS_03 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CNTRCT_LEAD_ID STRING,
# MAGIC   CNTRCT_LEAD_NAME STRING,
# MAGIC   STATUS STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_OVERRIDE_LEADS_03' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_C34_APEXUS_LEADS_04 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CNTRCT_LEAD_ID STRING,
# MAGIC   MNC STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_C34_APEXUS_LEADS_04' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_MCK_PROGRAMS_05A (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   Acct_Class STRING,
# MAGIC   NATL_GRP STRING,
# MAGIC   Chain STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   PVP_Flag STRING,
# MAGIC   Lead STRING,
# MAGIC   CNTRCT_LEAD_NAME STRING,
# MAGIC   PRTY_CONT STRING,
# MAGIC   CNTRCT_LEAD_TYPE STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_MCK_PROGRAMS_05A' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_APEXUS_MCK_PROGRAMS_05B (
# MAGIC   Account STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   Acct_Class STRING,
# MAGIC   Natl_Grp STRING,
# MAGIC   Chain STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   Lead STRING,
# MAGIC   Lead_Name STRING,
# MAGIC   Comments STRING,
# MAGIC   `Type` STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_MCK_PROGRAMS_05B' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_APEXUS_MCK_PROGRAMS_SUMMARY_05C (
# MAGIC   Lead STRING,
# MAGIC   Lead_Name STRING,
# MAGIC   CountOfAccount BIGINT
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_MCK_PROGRAMS_SUMMARY_05C' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_APEXUS_MCK_PROGRAMS_06 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAM STRING,
# MAGIC   XFD_KU_340B_ID STRING,
# MAGIC   Acct_Class STRING,
# MAGIC   NATL_GRP_CD STRING,
# MAGIC   ACCT_CHN_ID STRING,
# MAGIC   LEAD_ID DECIMAL(10,0),
# MAGIC   LEAD_NAME STRING,
# MAGIC   LEAD_TYPE STRING,
# MAGIC   LST_UPDT_DT DATE
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_APEXUS_MCK_PROGRAMS_06' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_PHS_LEADS_07 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   LEAD STRING,
# MAGIC   LEAD_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   Expansion_Lead STRING,
# MAGIC   Expansion_Account STRING
# MAGIC ) 
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_LEADS_07' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_ISMC_MHS_AUDIT (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAM STRING,
# MAGIC   ACCT_CHN_ID STRING,
# MAGIC   MCK_KU_CHNL_TYP STRING,
# MAGIC   MCK_KU_ACCT_CLAS STRING,
# MAGIC   NATL_GRP_CD STRING,
# MAGIC   NATL_GRP_NAM STRING
# MAGIC   )
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_ISMC_MHS_AUDIT' 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true,
# MAGIC   'delta.autoOptimize.optimizeWrite' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_CHAIN_DT_04 (
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_CHAIN_DT_04' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_CHAIN_CT_05 (
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CountOfCUST_CHN_ID BIGINT
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_CHAIN_CT_05' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_CHAIN_06 (
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   BUS_TYP_CD STRING,
# MAGIC   ROUTE STRING,
# MAGIC   UPDATE_SAP STRING,
# MAGIC   SALES_CURMTH STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_CHAIN_06' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_NATIONALGRP_DT_01 (
# MAGIC   PHS_340B_ID STRING,
# MAGIC   NATIONAL_GROUP STRING
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_NATIONALGRP_DT_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_NATIONALGRP_CT_02 (
# MAGIC   PHS_340B_ID STRING,
# MAGIC   COUNTOFNATIONAL_GROUP BIGINT
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_NATIONALGRP_CT_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_NATIONALGRP_03 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   RX_COGS DOUBLE,
# MAGIC   OTC_COGS DOUBLE,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_NATIONALGRP_03' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_PVP_CODING_AUDIT (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   PVP_Flag STRING,
# MAGIC   PVP_CODE STRING,
# MAGIC   PVP_Coding STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   NATIONAL_SUBGRP_CD STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATIONAL_SUBGRP_NAME STRING,
# MAGIC   NATIONAL_GRP_NAME STRING,
# MAGIC   PVP_EXPIRATION_DATE DATE,
# MAGIC   HRSA_Start DATE,
# MAGIC   COMMENTS STRING,
# MAGIC   PVP_Start DATE,
# MAGIC   HRSA_TERM_DATE DATE
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PVP_CODING_AUDIT' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_ACTIVATED_PHSWAC_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   Acct_Class STRING,
# MAGIC   STATUS STRING,
# MAGIC   UPDATE_SAP TIMESTAMP,
# MAGIC   OPENED_FOR_RETURNS DATE,
# MAGIC   Active_Status_Update TIMESTAMP,
# MAGIC   SALES_CURMTH DECIMAL(10, 2),
# MAGIC   SALES_LSTMTH DECIMAL(10, 2),
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_ACTIVATED_PHSWAC_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_340B_EXCEPTIONS_02 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   UPDATE_340BID STRING,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_340B_EXCEPTIONS_02' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.340b_gold.T_NON_APEXUS_CODING_WITH_APEXUS_LEADS_01 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   DMEM_SUBMITTED STRING,
# MAGIC   ACCT_CLASSIFICATION STRING,
# MAGIC   PVP_CODING STRING,
# MAGIC   PVP_Flag STRING,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   PVP_EXPIRATION_DATE DATE,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   CUST_CHN_ID STRING,
# MAGIC   CHAIN_NAM STRING,
# MAGIC   NATL_GROUP STRING,
# MAGIC   Sub_Group STRING,
# MAGIC   SUB_GROUP_NAME STRING,
# MAGIC   ACTIVATION_DATE DATE,
# MAGIC   SALES_LSTMTH STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   COMMENTS STRING
# MAGIC ) 
# MAGIC USING DELTA LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_NON_APEXUS_CODING_WITH_APEXUS_LEADS_01' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC create roster
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.`340b_gold`.T_PHS_DMEM_SUBMISSION_07 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   LEAD STRING,
# MAGIC   LEAD_NAME STRING,
# MAGIC   DMEM_Prio STRING,
# MAGIC   DMEM_Pref STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   OPENED_FOR_RETURNS STRING,
# MAGIC   ROUTE STRING,
# MAGIC   STOP STRING,
# MAGIC   RX_BILL_PLAN STRING,
# MAGIC   NATIONAL_GRP_CD STRING,
# MAGIC   NATIONAL_GRP_NAME STRING
# MAGIC )USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_DMEM_SUBMISSION_07' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true);;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.`340b_gold`.T_PHS_DMEM_MISSING_06 (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   ZX_BLOCK DATE,
# MAGIC   SALES_CURMTH DOUBLE,
# MAGIC   HRSA_START_DATE DATE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   PVP_Flag STRING,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   LEAD STRING,
# MAGIC   LEAD_NAME STRING,
# MAGIC   PHS_340B_ID STRING,
# MAGIC   Expansion_Entity STRING,
# MAGIC   ENTITY_TYPE STRING,
# MAGIC   COMMENTS STRING,
# MAGIC   STATUS STRING
# MAGIC )USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_PHS_DMEM_MISSING_06' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS psas_di_dev.`340b_gold`.T_WACA34_DMEM_SUBMISSION_06 (
# MAGIC   PPM STRING,
# MAGIC   ADD STRING,
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   LEAD STRING,
# MAGIC   LEAD_NAME STRING,
# MAGIC   PRIORITY STRING,
# MAGIC   PREFERENCE STRING,
# MAGIC   MARK_UP STRING,
# MAGIC   CHANGE_BY STRING,
# MAGIC   CHANGE_ON STRING,
# MAGIC   BLOCK_ID STRING,
# MAGIC   CONTRACT_BLOCK STRING,
# MAGIC   SALES_CURMTH STRING,
# MAGIC   HRSA_TERM_DATE STRING,
# MAGIC   PVP_ELIGIBILITY_DATE STRING,
# MAGIC   ZX_BLOCK STRING,
# MAGIC   OPENED_FOR_RETURNS STRING
# MAGIC )USING DELTA  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_WACA34_DMEM_SUBMISSION_06' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true);;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE psas_di_dev.`340b_gold`.T_LUQ_ACCOUNT_DMEM_ADHOC (
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   CNTRCT_LEAD_ID STRING,
# MAGIC   T_DMEM_LIST_LEAD_NAME STRING,
# MAGIC   LEAD_TYPE STRING,
# MAGIC   MARKUP_CONT BIGINT,
# MAGIC   CONT_PREFD STRING,
# MAGIC   LUTL_PHS_LEADS_LEAD_NAME STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_LUQ_ACCOUNT_DMEM_ADHOC' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE psas_di_dev.`340b_gold`.T_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3 (
# MAGIC   PPM STRING,
# MAGIC   ADD STRING,
# MAGIC   CUST_ACCT_ID STRING,
# MAGIC   CUST_ACCT_NAME STRING,
# MAGIC   LEAD STRING,
# MAGIC   LEAD_NAME STRING,
# MAGIC   PRIORITY STRING,
# MAGIC   PREFERENCE STRING,
# MAGIC   MARK_UP STRING,
# MAGIC   CHANGE_BY STRING,
# MAGIC   CHANGE_ON TIMESTAMP,
# MAGIC   BLOCK_ID STRING,
# MAGIC   CONTRACT_BLOCK STRING,
# MAGIC   SALES_CURMTH DOUBLE,
# MAGIC   HRSA_TERM_DATE DATE,
# MAGIC   PVP_ELIGIBILITY_DATE DATE,
# MAGIC   ZX_BLOCK STRING
# MAGIC )USING DELTA
# MAGIC  LOCATION 'abfss://catalog@stpsasdi340bdev.dfs.core.windows.net/gold/T_LUQ_ACCOUNT_DMEM_SUBMISSION_WACA3' 
# MAGIC TBLPROPERTIES (
# MAGIC  'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'spark.sql.files.ignoreMissingFiles'=true);
# MAGIC
