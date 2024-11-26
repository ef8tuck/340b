-- Databricks notebook source
----01 UQ_New_PVPCoding
UPDATE {coreDBName}.{coreSchemaName}.T_LUTL_PVP_CODING AS LUPVCOD SET  LUPVCOD.Coding = concat( LUPVCOD.Chain , LUPVCOD.SUB_GROUP),  LUPVCOD.PVP_CODING = 'Y'
WHERE ((( LUPVCOD.Coding) Is Null Or ( LUPVCOD.Coding)=''));

-- COMMAND ----------

---02 UQ_PHS_AUDIT
UPDATE psas_di_dev.340b_brnz.T_PHS_AUDIT AS PHAUD SET PHAUD.PVP_CODING = 'Y' from {coreDBName}.{coreSchemaName}.T_LUTL_PVP_CODING AS LUPVCOD WHERE LUPVCOD.Coding = PHAUD.PVP_CODE 
AND (((PHAUD.PVP_CODING)<>'Y') AND ((LUPVCOD.PVP_CODING)='Y') AND ((PHAUD.NATIONAL_GRP_CD)<>'0100'));

-- COMMAND ----------

---Q_PVP_Coding_Audit
DROP VIEW IF EXISTS V_PVP_CODING_AUDIT;
CREATE OR REPLACE VIEW V_PVP_CODING_AUDIT AS 
SELECT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.ZX_BLOCK, PHAUD.PHS_340B_ID, IFF(PHAUD.PVP_PARTICIPATION_FLAG='Y','Y','N') AS PVP_Flag, PHAUD.PVP_CODE, PHAUD.PVP_CODING AS PVP_Coding, PHAUD.CUST_CHN_ID, PHAUD.NATIONAL_SUBGRP_CD, PHAUD.CHAIN_NAM, PHAUD.NATIONAL_SUBGRP_NAME, PHAUD.NATIONAL_GRP_NAME, PHAUD.PVP_EXPIRATION_DATE, PHAUD.HRSA_START_DATE AS HRSA_Start, PHAUD.COMMENTS, PHAUD.PVP_ELIGIBILITY_DATE AS PVP_Start, PHAUD.HRSA_TERM_DATE
FROM {coreDBName}.{coreSchemaName}.T_PHS_AUDIT AS PHAUD 
WHERE (((PHAUD.PHS_340B_ID) IS NOT NULL) AND (IFF(PVP_PARTICIPATION_FLAG='Y','Y','N') != PVP_CODING) AND (PHAUD.CUST_CHN_ID != '989') AND (PHAUD.ACCT_CLASSIFICATION in ('004','005')) AND (UPPER(PHAUD.STATUS)='ACTIVE'))
ORDER BY PHAUD.PVP_ELIGIBILITY_DATE;

-- COMMAND ----------


---01 Q_Activated_PHSWAC
DROP VIEW IF EXISTS V_ACTIVATED_PHSWAC_01;
CREATE OR REPLACE VIEW V_ACTIVATED_PHSWAC_01 AS 
SELECT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.ACCT_CLASSIFICATION AS Acct_Class, PHAUD.STATUS, PHAUD.UPDATE_SAP, PHAUD.OPENED_FOR_RETURNS,PHAUD.UPDATE_ACTIVESTATUS AS Active_Status_Update, PHAUD.SALES_CURMTH, PHAUD.SALES_LSTMTH, PHAUD.COMMENTS
FROM {coreDBName}.{coreSchemaName}.T_PHS_AUDIT AS PHAUD 
WHERE ((PHAUD.ACCT_CLASSIFICATION='003' OR PHAUD.ACCT_CLASSIFICATION in ('004','005')) AND (UPPER(PHAUD.STATUS)='ACTIVE') AND (PHAUD.UPDATE_ACTIVESTATUS Is Not Null) AND PHAUD.CUST_CHN_ID !='989')
ORDER BY PHAUD.UPDATE_ACTIVESTATUS DESC;

-- COMMAND ----------

---02 Q_340B_Exceptions
DROP VIEW IF EXISTS V_340B_EXCEPTIONS_02;
CREATE OR REPLACE VIEW V_340B_EXCEPTIONS_02 AS 
SELECT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.PHS_340B_ID, PHAUD.UPDATE_340BID, PHAUD.COMMENTS
FROM {coreDBName}.{coreSchemaName}.T_PHS_AUDIT AS PHAUD LEFT JOIN PRD_ENT_DL_US_EXT_340B_DB.RAWDATA.OPACE_DAILY_PUBLIC AS OPACE_DAILY_PUBLIC ON PHAUD.PHS_340B_ID = OPACE_DAILY_PUBLIC.ID_340B
WHERE ((PHAUD.PHS_340B_ID Is Not Null) AND (PHAUD.UPDATE_340BID Is Not Null) AND (OPACE_DAILY_PUBLIC.ID_340B Is Null) AND ((PHAUD.CUST_CHN_ID)!='989') AND (UPPER(PHAUD.STATUS)='ACTIVE'))
ORDER BY PHAUD.UPDATE_340BID DESC;

-- COMMAND ----------


--Success
CREATE OR REPLACE VIEW V_SUCCESS AS select pi() * 2.0 * 2.0 as area_of_circle;
