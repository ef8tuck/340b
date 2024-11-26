# Databricks notebook source
coreDBName = 'psas_di_dev'
coreSchemaName = '340b_brnz'

spark.sql(f"""
CREATE OR REPLACE table V_NON_ORDERING_ACCOUNTS AS 
SELECT LUNOORAC.Cust_Acct_ID, PHAUD.CUST_ACCT_NAME, PHAUD.CUST_CHN_ID, PHAUD.STATUS
FROM {coreDBName}.{coreSchemaName}.t_lutl_non_ordering_accounts AS LUNOORAC 
LEFT JOIN {coreDBName}.{coreSchemaName}.T_PHS_AUDIT AS PHAUD 
ON LUNOORAC.Cust_Acct_ID = PHAUD.CUST_ACCT_ID;
""")
