-- Databricks notebook source

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_HRSA_TERMED_200 AS 
SELECT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.ZX_BLOCK, PHAUD.COMMENTS, PHAUD.PHS_340B_ID, PHAUD.COVERED_ENTITY_NAME, PHAUD.DC_NAME, PHAUD.SALES_ADMIN, PHAUD.MHS_SALES_REP, PHAUD.HRSA_TERM_DATE, PHAUD.SALES_CURMTH, PHAUD.OPENED_FOR_RETURNS
FROM psas_di_dev.340b_slvr.T_PHS_AUDIT AS PHAUD 
WHERE PHAUD.PHS_340B_ID IS NOT NULL 
AND PHAUD.HRSA_TERM_DATE != CURRENT_DATE 
AND PHAUD.ACCT_CLASSIFICATION IN ('003','004','005') 
AND PHAUD.CUST_CHN_ID != '989' 
AND UPPER(PHAUD.STATUS)='ACTIVE'
ORDER BY PHAUD.PHS_340B_ID;

-- COMMAND ----------

SELECT DISTINCT 
    PHAUD.CUST_ACCT_ID, 
    PHAUD.CUST_ACCT_NAME, 
    CONCAT(PHAUD.ACCT_NAME_A34,PHAUD.ACCT_NAME_PHS) AS Account_Name_Concat
FROM psas_di_dev.340b_slvr.T_PHS_AUDIT AS PHAUD
LIMIT 10;

-- COMMAND ----------

CREATE
OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_ELIGIBILITY_WAC_01 AS
SELECT
  DISTINCT PHAUD.CUST_ACCT_ID,
  PHAUD.CUST_ACCT_NAME,
  PHAUD.MNC,
  PHAUD.ENTITY_TYPE,
  PHAUD.ROUTE,
  PHAUD.PHS_340B_ID,
  PHAUD.CUST_CHN_ID,
  PHAUD.CHAIN_NAM,
  PHAUD.NATIONAL_SUBGRP_CD,
  PHAUD.NATIONAL_SUBGRP_NAME,
  PHAUD.PVP_MEMBER_NAME,
  PHAUD.PVP_PARTICIPATION_FLAG AS PVP_Flag,
  PHAUD.PVP_ELIGIBILITY_DATE AS Apexus_Eligible,
  PHAUD.PVP_EXPIRATION_DATE,
  PHAUD.PVP_PARTICIPANT_ID,
  PHAUD.HRSA_START_DATE,
  PHAUD.HRSA_TERM_DATE,
  PHAUD.COMMENTS,
  PHAUD.STATUS,
  PHAUD.NATIONAL_GRP_CD
FROM
  psas_di_dev.340b_slvr.T_PHS_AUDIT AS PHAUD
  LEFT JOIN psas_di_dev.340b_gold.V_NON_ORDERING_ACCOUNTS AS NOORAC ON PHAUD.CUST_ACCT_ID = NOORAC.Cust_Acct_ID
WHERE
  PHAUD.CUST_CHN_ID != '989'
  AND UPPER(PHAUD.STATUS) = 'ACTIVE'
  AND PHAUD.ACCT_CLASSIFICATION = '003'
  AND NOORAC.Cust_Acct_ID IS NULL
ORDER BY
  CASE
    WHEN CONCAT(PHAUD.ACCT_NAME_A34, PHAUD.ACCT_NAME_PHS) = 'NY' THEN 'PHS'
    ELSE 'WAC'
  END as Acct_Type;

-- COMMAND ----------


CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_ELIGIBILITY_PHS_01 AS
SELECT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.PHS_340B_ID, PHAUD.SALES_ADMIN, PHAUD.MHS_SALES_REP, PHAUD.SALES_REP, PHAUD.CUST_CHN_ID AS Chain, PHAUD.CHAIN_NAM, PHAUD.REGION_CD, PHAUD.OPENED_FOR_RETURNS, CONCAT(PVP_PARTICIPATION_FLAG,PVP_CODING) AS Combine, PHAUD.PVP_PARTICIPATION_FLAG AS PVP_Flag, PHAUD.PVP_CODING AS PVP_Coding, PHAUD.PVP_ELIGIBILITY_DATE AS PVP_Eligible, PHAUD.PVP_EXPIRATION_DATE AS PVP_Expiration, PHAUD.HRSA_START_DATE AS HRSA_Start, PHAUD.HRSA_TERM_DATE AS HRSA_Term, PHAUD.ZX_BLOCK, PHAUD.COMMENTS
FROM psas_di_dev.340b_brnz.T_PHS_AUDIT AS PHAUD 
WHERE PHAUD.CUST_CHN_ID != '989' AND CONCAT(PVP_PARTICIPATION_FLAG , PVP_CODING) IN ('NY' ,'YN') AND PHAUD.ACCT_CLASSIFICATION in ('004','005') AND UPPER(PHAUD.STATUS)='ACTIVE' AND PHAUD.BUS_TYP_CD != '31'
ORDER BY CONCAT(PVP_PARTICIPATION_FLAG,PVP_CODING), PHAUD.PHS_340B_ID;
