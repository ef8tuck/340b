-- Databricks notebook source
CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_Accounts_Generics_Duplicates_01 AS
SELECT T_PHS_Accounts_Generics.Accounts
FROM psas_di_dev.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics
GROUP BY T_PHS_Accounts_Generics.Accounts
HAVING Count(T_PHS_Accounts_Generics.Accounts)>1;


-- COMMAND ----------

CREATE OR REPLACE TABLE psas_di_dev.340b_gold.T_PHS_Accounts_Generics_Coding_02 AS
SELECT DISTINCT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.ACCT_CLASSIFICATION, T_PHS_AUDIT.HRSA_TERM_DATE, T_PHS_AUDIT.PVP_PARTICIPANT_ID, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG, T_PHS_AUDIT.PVP_ELIGIBILITY_DATE, T_PHS_AUDIT.STATUS, T_PHS_AUDIT.PVP_CODING, 
IFF(psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Duplicates_01.Accounts is null,'','Yes') AS Duplicate
FROM (psas_di_dev.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics INNER JOIN psas_di_dev.340b_gold.T_PHS_AUDIT AS T_PHS_AUDIT ON T_PHS_Accounts_Generics.Accounts = T_PHS_AUDIT.CUST_ACCT_ID) LEFT JOIN psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Duplicates_01 ON T_PHS_AUDIT.CUST_ACCT_ID = V_PHS_Accounts_Generics_Duplicates_01.Accounts
ORDER BY T_PHS_AUDIT.CUST_ACCT_ID;

