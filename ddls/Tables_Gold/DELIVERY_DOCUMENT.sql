-- Databricks notebook source
----01 Q_Delivery_Document
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_DELIVERY_DOCUMENT_01 AS 
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME, T_PHS_AUDIT.UPDATE_SAP, T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.DELIVERY_DOC, T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL, T_PHS_AUDIT.COMMENTS
FROM psas_di_dev.`340b_gold`.T_PHS_AUDIT AS T_PHS_AUDIT LEFT JOIN psas_di_dev.`340b_slvr`.T_CONTRACTPHARMACY_EXCEPTIONS AS COEX ON T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL = COEX.Retail_Chain
WHERE ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID != '989') AND (T_PHS_AUDIT.BUS_TYP_CD ='44') AND (T_PHS_AUDIT.DELIVERY_DOC != 'C') AND (UPPER(T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL) != 'RITE AID') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND (COEX.ID Is Null)) OR ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID!='989') AND (T_PHS_AUDIT.BUS_TYP_CD Not In ('31','44')) AND (T_PHS_AUDIT.DELIVERY_DOC='C' Or T_PHS_AUDIT.DELIVERY_DOC='D') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND ((COEX.ID) Is Null)) OR ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID != '989') AND (T_PHS_AUDIT.BUS_TYP_CD='44') AND (T_PHS_AUDIT.DELIVERY_DOC != 'D') AND (T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL='RITE AID') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND (COEX.ID Is Null));

-- COMMAND ----------

----02 Q_Delivery_Document_CP_Pays_Bill
CREATE OR REPLACE Table psas_di_dev.340b_gold.T_DELIVERY_DOCUMENT_CP_PAYS_BILL_02 AS 
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME,T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.DELIVERY_DOC, T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL
FROM psas_di_dev.340b_gold.T_PHS_AUDIT AS T_PHS_AUDIT INNER JOIN psas_di_dev.340b_slvr.T_ContractPharmacy_Exceptions AS COEX ON T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL = COEX.Retail_Chain
WHERE ((T_PHS_AUDIT.CUST_CHN_ID != '989') AND ((T_PHS_AUDIT.BUS_TYP_CD)='44') AND (T_PHS_AUDIT.DELIVERY_DOC IN('A','C','D')) AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')));

-- COMMAND ----------

--Success
CREATE OR REPLACE VIEW V_SUCCESS AS select pi() * 2.0 * 2.0 as area_of_circle;

-- COMMAND ----------

create or replace table psas_di_dev.340b_gold.T_CONTRACTPHARMACY_EXCEPTIONS as
SELECT ID, RETAIL_CHAIN 
FROM psas_di_dev.`340b_slvr`.T_CONTRACTPHARMACY_EXCEPTIONS;
