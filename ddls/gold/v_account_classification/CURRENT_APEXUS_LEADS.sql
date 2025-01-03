-- Databricks notebook source
--01 DQ_SALES_Apexus

DELETE FROM {interDBName}.{interSchemaName}.T_SALES_APEXUS;

-- COMMAND ----------

--02 AQ_SALES_Apexus

-----------------------PTQ_NZ_SALES_APEXUS----Source query is pointin SBX_PSAS_DB.MHS_RNA.T_PHS_AUDIT  and we are using local copy of T_PHS_AUDIT
--T_PHS_AUDIT.ACTIVE_CUST_IND = 'A' Replaced with A.STATUS= 'ACTIVE' Need to check with Chinmay 
CREATE OR REPLACE VIEW V_PTQ_NZ_SALES_APEXUS_02 AS 
SELECT DISTINCT S.CUST_ACCT_ID, A.CUST_ACCT_NAME, CNTRC_LEAD_TP_ID, CAST(SUM(SLS_CR_AMT) as FLOAT) AS TOTAL_NET_SALES 
FROM PRD_PSAS_DB.RPT.T_NET_CUST_SALES S INNER JOIN SBX_PSAS_DB.MHS_RNA.T_PHS_AUDIT A
ON S.CUST_ACCT_ID = A.CUST_ACCT_ID
WHERE UPPER(A.ACTIVE_CUST_IND)= 'A' AND CNTRC_LEAD_TP_ID IN ('40019','94139','113705','125712','293388','452279','526528','869967')
AND A.CUST_CHN_ID <> '989'
AND PROC_WRK_DT BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS(current_timestamp(),0)) AND LAST_DAY(ADD_MONTHS(current_timestamp(),0))
GROUP BY S.CUST_ACCT_ID, A.CUST_ACCT_NAME, CNTRC_LEAD_TP_ID;

-- COMMAND ----------

----Accounts that purchased off the Apexus contract leads this current month
--SELECT DISTINCT S.CUST_ACCT_ID, A.CUST_ACCT_NAME, CNTRC_LEAD_TP_ID, CAST(SUM(SLS_CR_AMT) as FLOAT) AS TOTAL_NET_SALES FROM PRD_PSAS_DB.RPT.T_NET_CUST_SALES --S INNER JOIN SBX_PSAS_DB.MHS_RNA.T_PHS_AUDIT A ON S.CUST_ACCT_ID = A.CUST_ACCT_ID WHERE A.ACTIVE_CUST_IND = 'A' AND CNTRC_LEAD_TP_ID IN --('40019','94139','113705','125712','293388','452279','526528','869967') AND A.CUST_CHN_ID <> '989' AND PROC_WRK_DT BETWEEN DATE_TRUNC('MONTH', ADD_MONTHS--(current_timestamp(),0)) AND LAST_DAY(ADD_MONTHS(current_timestamp(),0)) GROUP BY S.CUST_ACCT_ID, A.CUST_ACCT_NAME, CNTRC_LEAD_TP_ID

-- COMMAND ----------

INSERT INTO {interDBName}.{interSchemaName}.T_SALES_APEXUS ( CUST_ACCT_ID, CUST_ACCT_NAME, CNTRC_LEAD_TP_ID, TOTAL_NET_SALES )
SELECT PTNZSAAPX.CUST_ACCT_ID, PTNZSAAPX.CUST_ACCT_NAME, PTNZSAAPX.CNTRC_LEAD_TP_ID, PTNZSAAPX.TOTAL_NET_SALES
FROM V_PTQ_NZ_SALES_APEXUS_02 AS PTNZSAAPX;

-- COMMAND ----------

--03 Q_SALES_Apexus
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_SALES_APEXUS_03 AS 
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, Sum(T_SALES_Apexus.TOTAL_NET_SALES) AS Total_AGP_Sales, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.HRSA_START_DATE, T_PHS_AUDIT.HRSA_TERM_DATE, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG AS PVP_Flag, T_PHS_AUDIT.ZX_BLOCK, T_PHS_AUDIT.PVP_ELIGIBILITY_DATE, T_PHS_AUDIT.PVP_EXPIRATION_DATE, T_PHS_AUDIT.COMMENTS 
FROM {interDBName}.{interSchemaName}.T_SALES_APEXUS AS T_SALES_Apexus INNER JOIN psas_di_dev.`340b_brnz`.t_phs_audit AS T_PHS_AUDIT ON T_SALES_Apexus.CUST_ACCT_ID = T_PHS_AUDIT.CUST_ACCT_ID GROUP BY T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.HRSA_START_DATE, T_PHS_AUDIT.HRSA_TERM_DATE, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG, T_PHS_AUDIT.ZX_BLOCK, T_PHS_AUDIT.PVP_ELIGIBILITY_DATE, T_PHS_AUDIT.PVP_EXPIRATION_DATE, T_PHS_AUDIT.COMMENTS;
