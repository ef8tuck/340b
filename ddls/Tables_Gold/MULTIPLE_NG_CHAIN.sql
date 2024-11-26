-- Databricks notebook source
--04 Q_340B_Chain_DT
  CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_CHAIN_DT_04 AS
  SELECT DISTINCT T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.CUST_CHN_ID
  FROM psas_di_dev.`340b_gold`.T_PHS_AUDIT AS T_PHS_AUDIT
  WHERE T_PHS_AUDIT.PHS_340B_ID IS NOT NULL AND T_PHS_AUDIT.CUST_CHN_ID <> '989' AND T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005') 
  AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE' AND T_PHS_AUDIT.NATIONAL_GRP_CD = '0240';

-- COMMAND ----------

--05 Q_340B_Chain_CT
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_CHAIN_CT_05 AS
SELECT T_340B_CHAIN_DT_04.PHS_340B_ID, COUNT(T_340B_CHAIN_DT_04.CUST_CHN_ID) AS CountOfCUST_CHN_ID
FROM psas_di_dev.340b_gold.T_340B_CHAIN_DT_04
GROUP BY T_340B_CHAIN_DT_04.PHS_340B_ID
HAVING COUNT(T_340B_CHAIN_DT_04.CUST_CHN_ID)>1;

-- COMMAND ----------

CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_CHAIN_06 AS 
SELECT T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.NATIONAL_GRP_CD, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CHAIN_NAM, T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.ROUTE, T_PHS_AUDIT.UPDATE_SAP, T_PHS_AUDIT.SALES_CURMTH
FROM psas_di_dev.340b_gold.T_340B_CHAIN_CT_05 INNER JOIN psas_di_dev.340b_brnz.T_PHS_AUDIT AS T_PHS_AUDIT ON T_340B_CHAIN_CT_05.PHS_340B_ID = T_PHS_AUDIT.PHS_340B_ID
WHERE T_PHS_AUDIT.CUST_CHN_ID <> '989' AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE'
ORDER BY T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CUST_ACCT_NAME;

-- COMMAND ----------

--01 Q_340B_NationalGrp_DT 
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_NATIONALGRP_DT_01 AS
SELECT DISTINCT T_PHS_AUDIT.PHS_340B_ID, IFF(NATIONAL_GRP_CD='0740','0240',NATIONAL_GRP_CD) AS NATIONAL_GROUP
FROM psas_di_dev.340b_gold.T_PHS_AUDIT AS T_PHS_AUDIT
WHERE T_PHS_AUDIT.PHS_340B_ID <> '' AND T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('003','004','005') AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE' 
AND T_PHS_AUDIT.CUST_CHN_ID NOT IN ('716','989');

-- COMMAND ----------

--02 Q_340B_NationalGrp_CT
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_NATIONALGRP_CT_02 AS
SELECT T_340B_NATIONALGRP_DT_01.PHS_340B_ID, Count(T_340B_NATIONALGRP_DT_01.NATIONAL_GROUP) AS COUNTOFNATIONAL_GROUP
FROM psas_di_dev.340b_gold.T_340B_NATIONALGRP_DT_01
GROUP BY T_340B_NATIONALGRP_DT_01.PHS_340B_ID
HAVING Count(T_340B_NATIONALGRP_DT_01.NATIONAL_GROUP) > 1;

-- COMMAND ----------

--03 Q_340B_NationalGrp
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_340B_NATIONALGRP_03 AS
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CHAIN_NAM, T_PHS_AUDIT.RX_COGS, T_PHS_AUDIT.OTC_COGS, T_PHS_AUDIT.NATIONAL_GRP_CD, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME, T_PHS_AUDIT.COMMENTS
FROM psas_di_dev.340b_gold.T_340B_NATIONALGRP_CT_02 INNER JOIN psas_di_dev.340b_gold.T_PHS_AUDIT AS T_PHS_AUDIT ON T_340B_NATIONALGRP_CT_02.PHS_340B_ID = T_PHS_AUDIT.PHS_340B_ID
WHERE T_PHS_AUDIT.CUST_CHN_ID NOT IN ('989','716') AND T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('005','004','003') AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE'
ORDER BY T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.NATIONAL_GRP_CD, T_PHS_AUDIT.CUST_CHN_ID;