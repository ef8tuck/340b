-- Databricks notebook source
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_NON_ORDERING_ACCOUNTS AS 
SELECT LUNOORAC.Cust_Acct_ID, PHAUD.CUST_ACCT_NAME, PHAUD.CUST_CHN_ID, PHAUD.STATUS
FROM psas_di_dev.340b_brnz.t_lutl_non_ordering_accounts AS LUNOORAC LEFT JOIN psas_di_dev.340b_brnz.t_phs_audit AS PHAUD ON LUNOORAC.Cust_Acct_ID = PHAUD.CUST_ACCT_ID;

-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_ACCOUNT_CLASSIFICATION_02 AS 
SELECT DISTINCT PHAUD.CUST_ACCT_ID, PHAUD.CUST_ACCT_NAME, PHAUD.ROUTE, PHAUD.STOP, PHAUD.ACTIVATION_DATE, PHAUD.UPDATE_SAP, PHAUD.PHS_340B_ID, PHAUD.CUST_CHN_ID, PHAUD.ACCT_CLASSIFICATION, PHAUD.NATIONAL_GRP_CD, PHAUD.STATUS, PHAUD.PVP_CODING, PHAUD.COMMENTS
FROM psas_di_dev.340b_brnz.T_PHS_AUDIT  AS PHAUD LEFT JOIN psas_di_dev.340b_gold.V_NON_ORDERING_ACCOUNTS AS NOORAC ON PHAUD.CUST_ACCT_ID = NOORAC.Cust_Acct_ID
WHERE (PHAUD.CUST_CHN_ID In ('147','586','889') AND PHAUD.CUST_CHN_ID Not In ('989','585') AND PHAUD.ACCT_CLASSIFICATION not in ('004','005') AND 
UPPER(PHAUD.STATUS)='ACTIVE' AND CHARINDEX('Y',CONCAT(ACCT_NAME_A34,ACCT_NAME_A34))=0 AND NOORAC.Cust_Acct_ID Is Null) OR 
(PHAUD.CUST_CHN_ID In ('147','586','889') AND PHAUD.CUST_CHN_ID Not In ('989','585') AND PHAUD.ACCT_CLASSIFICATION not in ('004','005') AND 
PHAUD.NATIONAL_GRP_CD='0240' AND UPPER(PHAUD.STATUS)='ACTIVE' AND CHARINDEX('Y',CONCAT(ACCT_NAME_A34,ACCT_NAME_A34))=0 AND 
NOORAC.Cust_Acct_ID Is Null) OR (PHAUD.CUST_CHN_ID In ('147','586','889') AND PHAUD.CUST_CHN_ID Not In ('989','585') AND 
PHAUD.ACCT_CLASSIFICATION != '003' AND PHAUD.NATIONAL_GRP_CD='0240' AND UPPER(PHAUD.STATUS)='ACTIVE' AND 
CHARINDEX('Y',CONCAT(ACCT_NAME_A34,ACCT_NAME_A34))=1 AND NOORAC.Cust_Acct_ID Is Null) OR (PHAUD.CUST_CHN_ID In ('147','586','889') 
AND PHAUD.CUST_CHN_ID Not In ('989','585') AND PHAUD.ACCT_CLASSIFICATION != '003' AND UPPER(PHAUD.STATUS)='ACTIVE' 
AND CHARINDEX('Y',CONCAT(ACCT_NAME_A34,ACCT_NAME_A34))=1 AND NOORAC.Cust_Acct_ID Is Null) OR (PHAUD.ACCT_CLASSIFICATION Not In ('003','004','005') 
AND UPPER(PHAUD.STATUS)='ACTIVE' AND PHAUD.PVP_CODING='Y' AND NOORAC.Cust_Acct_ID Is Null);

-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Duplicates_01 AS
SELECT T_PHS_Accounts_Generics.Accounts
FROM psas_di_dev.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics
GROUP BY T_PHS_Accounts_Generics.Accounts
HAVING Count(T_PHS_Accounts_Generics.Accounts)>1;


-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Coding_02 AS
SELECT DISTINCT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.ACCT_CLASSIFICATION, T_PHS_AUDIT.HRSA_TERM_DATE, T_PHS_AUDIT.PVP_PARTICIPANT_ID, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG, T_PHS_AUDIT.PVP_ELIGIBILITY_DATE, T_PHS_AUDIT.STATUS, T_PHS_AUDIT.PVP_CODING, 
IFF(psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Duplicates_01.Accounts is null,'','Yes') AS Duplicate
FROM (psas_di_dev.340b_slvr.T_PHS_Accounts_Generics AS T_PHS_Accounts_Generics INNER JOIN psas_di_dev.340b_slvr.T_PHS_AUDIT AS T_PHS_AUDIT ON T_PHS_Accounts_Generics.Accounts = T_PHS_AUDIT.CUST_ACCT_ID) LEFT JOIN psas_di_dev.340b_gold.V_PHS_Accounts_Generics_Duplicates_01 ON T_PHS_AUDIT.CUST_ACCT_ID = V_PHS_Accounts_Generics_Duplicates_01.Accounts
ORDER BY T_PHS_AUDIT.CUST_ACCT_ID;


-- COMMAND ----------


CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_ACCOUNTCONFIRMATION_LOOKUP_01 AS 
SELECT DISTINCT T_APEXUSFILE.SHIP_TO_ACCT_NO, T_APEXUSFILE.ENTITY_NAME, T_APEXUSFILE.SHIP_TO_ACCT_NAME, T_APEXUSFILE.APEXUS_340B_ID, T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.STATUS
FROM psas_di_dev.`340b_brnz`.t_apexusfile AS T_APEXUSFILE LEFT JOIN psas_di_dev.`340b_slvr`.T_PHS_AUDIT AS T_PHS_AUDIT ON T_APEXUSFILE.SHIP_TO_ACCT_NO = T_PHS_AUDIT.CUST_ACCT_ID
WHERE T_APEXUSFILE.SHIP_TO_ACCT_NO IS NOT NULL
ORDER BY T_PHS_AUDIT.CUST_ACCT_ID;



-- COMMAND ----------

 CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_CODE_KEY_01 AS 
SELECT DISTINCT APCOKEY.Lead_No AS Lead_No, APCOKEY.Org_ID_Seq, APCOKEY.Contract_Name AS Contract_Name, APCOKEY.Material, APCOKEY.EAN_UPC, APCOKEY.Material_Description AS Material_Description, APCOKEY.Original_Valid_From AS Original_Valid_From, APCOKEY.Original_Valid_To AS Original_Valid_To, APCOKEY.Bid_Price AS Bid_Price, APCOKEY.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_dev.`340b_slvr`.T_APEXUS_CODE_KEY AS APCOKEY 
WHERE (APCOKEY.Lead_No Is Not Null);



-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_WAC_ACCT_BY_LEAD_04 AS 
SELECT DISTINCT APWAACBYLE.Lead_No AS Lead_No, APWAACBYLE.Org_ID_Seq, APWAACBYLE.Contract_Name AS Contract_Name, APWAACBYLE.Material, APWAACBYLE.EAN_UPC, APWAACBYLE.Material_Description AS Material_Description, APWAACBYLE.Original_Valid_From AS Original_Valid_From, APWAACBYLE.Original_Valid_To AS Original_Valid_To, APWAACBYLE.Bid_Price AS Bid_Price, APWAACBYLE.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_dev.`340b_slvr`.T_APEXUS_WAC_ACCT_BY_LEAD AS APWAACBYLE;


-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_APEXUS_PHS_ACCT_BY_LEAD_02 AS 
SELECT DISTINCT APPHACBYLE.Lead_No AS Lead_No, APPHACBYLE.Org_ID_Seq, APPHACBYLE.Contract_Name AS Contract_Name, APPHACBYLE.Material, APPHACBYLE.EAN_UPC, APPHACBYLE.Material_Description AS Material_Description, APPHACBYLE.Original_Valid_From AS Original_Valid_From, APPHACBYLE.Original_Valid_To AS Original_Valid_To, APPHACBYLE.Bid_Price AS Bid_Price, APPHACBYLE.GPO_Chbk_Ref_No AS GPO_Chbk_Ref_No
FROM psas_di_dev.`340b_slvr`.T_APEXUS_PHS_ACCT_BY_LEAD AS APPHACBYLE 
WHERE (APPHACBYLE.Lead_No Is Not Null);


-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_WAC_ACCOUNTS_05 AS 
SELECT VAPCOKEY.Lead_No, VAPCOKEY.Org_ID_Seq, VAPCOKEY.Contract_Name, VAPCOKEY.Material, VAPCOKEY.EAN_UPC, VAPCOKEY.Material_Description, VAPCOKEY.Original_Valid_From, VAPCOKEY.Original_Valid_To, VAPCOKEY.Bid_Price, VAPCOKEY.GPO_Chbk_Ref_No
FROM psas_di_dev.340b_gold. V_APEXUS_CODE_KEY_01 VAPCOKEY 
UNION SELECT VAPWAACBYLE.Lead_No, VAPWAACBYLE.Org_ID_Seq, VAPWAACBYLE.Contract_Name, VAPWAACBYLE.Material, VAPWAACBYLE.EAN_UPC, VAPWAACBYLE.Material_Description, VAPWAACBYLE.Original_Valid_From, VAPWAACBYLE.Original_Valid_To, VAPWAACBYLE.Bid_Price, VAPWAACBYLE.GPO_Chbk_Ref_No
FROM psas_di_dev.`340b_gold`.V_APEXUS_WAC_ACCT_BY_LEAD_04 AS VAPWAACBYLE;


-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_PHS_ACCOUNTS_03 AS 
SELECT VAPCOKEY.Lead_No, VAPCOKEY.Org_ID_Seq, VAPCOKEY.Contract_Name, VAPCOKEY.Material, VAPCOKEY.EAN_UPC, VAPCOKEY.Material_Description, VAPCOKEY.Original_Valid_From, VAPCOKEY.Original_Valid_To, VAPCOKEY.Bid_Price, VAPCOKEY.GPO_Chbk_Ref_No
FROM psas_di_dev.340b_gold.V_APEXUS_CODE_KEY_01 AS VAPCOKEY 
UNION SELECT VAPPHACBYLE.Lead_No, VAPPHACBYLE.Org_ID_Seq, VAPPHACBYLE.Contract_Name, VAPPHACBYLE.Material, VAPPHACBYLE.EAN_UPC, VAPPHACBYLE.Material_Description, VAPPHACBYLE.Original_Valid_From, VAPPHACBYLE.Original_Valid_To, VAPPHACBYLE.Bid_Price, VAPPHACBYLE.GPO_Chbk_Ref_No
FROM psas_di_dev.340b_gold.V_APEXUS_PHS_ACCT_BY_LEAD_02 AS VAPPHACBYLE;


-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_WAC_ACCT_BY_LEAD_CHECK_04B AS 
SELECT DISTINCT VAPWAACBYLE.Lead_No, LUPHLEAD.LEAD_Unformatted, LUPHLEAD.WAC
FROM  psas_di_dev.`340b_gold`.V_APEXUS_WAC_ACCT_BY_LEAD_04  AS VAPWAACBYLE LEFT JOIN psas_di_dev.`340b_brnz`.T_LUTL_PHS_LEADS AS LUPHLEAD ON VAPWAACBYLE.Lead_No = LUPHLEAD.LEAD_Unformatted
ORDER BY VAPWAACBYLE.Lead_No;
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_PHS_ACCT_BY_LEAD_CHECK_02B AS 
SELECT DISTINCT VAPPHACBYLE.Lead_No, LUPHLEAD.LEAD_Unformatted, LUPHLEAD.PHS
FROM  psas_di_dev.`340b_gold`.V_APEXUS_PHS_ACCT_BY_LEAD_02  AS VAPPHACBYLE LEFT JOIN  psas_di_dev.`340b_brnz`.T_LUTL_PHS_LEADS AS LUPHLEAD ON VAPPHACBYLE.Lead_No = LUPHLEAD.LEAD_Unformatted
ORDER BY VAPPHACBYLE.Lead_No;

-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_BUSTYPE_01 AS
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.ACCT_CLASSIFICATION, T_PHS_AUDIT.UPDATE_SAP, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.DEA_NUM, T_PHS_AUDIT.DELIVERY_DOC, T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.PVP_PARTICIPATION_FLAG AS PVP_Flag, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.COMMENTS, T_PHS_AUDIT.SALES_LSTMTH, T_PHS_AUDIT.SALES_CURMTH
FROM psas_di_dev.340b_silver.T_PHS_AUDIT AS T_PHS_AUDIT
WHERE (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005') AND T_PHS_AUDIT.BUS_TYP_CD Not In ('31','44') AND T_PHS_AUDIT.CUST_CHN_ID<>'989' AND (case when CHARINDEX('/',CUST_ACCT_NAME) > 0 then 'Y'else 'N' end)='Y' AND UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') OR (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005') AND T_PHS_AUDIT.BUS_TYP_CD='44' AND T_PHS_AUDIT.CUST_CHN_ID<>'989' AND (case when CHARINDEX('/',CUST_ACCT_NAME) > 0 then 'Y'else 'N' end)='N' AND UPPER(T_PHS_AUDIT.STATUS)='ACTIVE' ) OR (T_PHS_AUDIT.ACCT_CLASSIFICATION ='003' AND T_PHS_AUDIT.BUS_TYP_CD Not In ('31','44') AND T_PHS_AUDIT.CUST_CHN_ID<>'989' AND (case when CHARINDEX('/',CUST_ACCT_NAME) > 0 then 'Y'else 'N' end)='Y' AND UPPER(T_PHS_AUDIT.STATUS) ='ACTIVE' ) OR (T_PHS_AUDIT.ACCT_CLASSIFICATION ='003' AND T_PHS_AUDIT.BUS_TYP_CD ='44' AND T_PHS_AUDIT.CUST_CHN_ID<>'989' AND 
(case when CHARINDEX('/',CUST_ACCT_NAME) > 0 then 'Y'else 'N' end)='N' AND UPPER(T_PHS_AUDIT.STATUS) ='ACTIVE')
ORDER BY T_PHS_AUDIT.UPDATE_SAP DESC;

-- COMMAND ----------

----01 Q_Delivery_Document
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_DELIVERY_DOCUMENT_01 AS 
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME, T_PHS_AUDIT.UPDATE_SAP, T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.DELIVERY_DOC, T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL, T_PHS_AUDIT.COMMENTS
FROM psas_di_dev.`340b_slvr`.T_PHS_AUDIT AS T_PHS_AUDIT LEFT JOIN psas_di_dev.`340b_slvr`.T_CONTRACTPHARMACY_EXCEPTIONS AS COEX ON T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL = COEX.Retail_Chain
WHERE ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID != '989') AND (T_PHS_AUDIT.BUS_TYP_CD ='44') AND (T_PHS_AUDIT.DELIVERY_DOC != 'C') AND (UPPER(T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL) != 'RITE AID') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND (COEX.ID Is Null)) OR ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID!='989') AND (T_PHS_AUDIT.BUS_TYP_CD Not In ('31','44')) AND (T_PHS_AUDIT.DELIVERY_DOC='C' Or T_PHS_AUDIT.DELIVERY_DOC='D') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND ((COEX.ID) Is Null)) OR ((T_PHS_AUDIT.CUST_ACCT_ID Not In ('061971')) AND (T_PHS_AUDIT.CUST_CHN_ID != '989') AND (T_PHS_AUDIT.BUS_TYP_CD='44') AND (T_PHS_AUDIT.DELIVERY_DOC != 'D') AND (T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL='RITE AID') AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')) AND (COEX.ID Is Null));

-- COMMAND ----------

----02 Q_Delivery_Document_CP_Pays_Bill
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_DELIVERY_DOCUMENT_CP_PAYS_BILL_02 AS 
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME,T_PHS_AUDIT.BUS_TYP_CD, T_PHS_AUDIT.DELIVERY_DOC, T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL
FROM {psas_di_dev.340b_slvr.T_PHS_AUDIT AS T_PHS_AUDIT INNER JOIN psas_di_dev.340b_slvr.T_ContractPharmacy_Exceptions AS COEX ON T_PHS_AUDIT.CONTRACT_PHARMACY_RETAIL = COEX.Retail_Chain
WHERE ((T_PHS_AUDIT.CUST_CHN_ID != '989') AND ((T_PHS_AUDIT.BUS_TYP_CD)='44') AND (T_PHS_AUDIT.DELIVERY_DOC IN('A','C','D')) AND (UPPER(T_PHS_AUDIT.STATUS)='ACTIVE') AND (T_PHS_AUDIT.ACCT_CLASSIFICATION in ('004','005')));

-- COMMAND ----------

--Success
CREATE OR REPLACE VIEW V_SUCCESS AS select pi() * 2.0 * 2.0 as area_of_circle;

-- COMMAND ----------

create or replace view psas_di_dev.340b_gold.V_CONTRACTPHARMACY_EXCEPTIONS as
SELECT ID, RETAIL_CHAIN 
FROM psas_di_dev.`340b_slvr`.T_CONTRACTPHARMACY_EXCEPTIONS;

-- COMMAND ----------


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

-- COMMAND ----------

create or replace view psas_di_dev.340b_gold.V_LUTL_PHS_LEADS(
	ID,
	LEAD,
	LEAD_NAME,
	APEXUS,
	PVP_FLAG,
	PHS,
	WAC,
	EXPANSION,
	DMEM_ORG,
	DMEM_ID,
	DMEM_SEQ,
	DMEM_PRIO,
	DMEM_PREF,
	AGP,
	LEAD_UNFORMATTED
) as
SELECT ID, LEAD, LEAD_NAME, APEXUS, PVP_FLAG, PHS, WAC, EXPANSION, DMEM_ORG, DMEM_ID, DMEM_SEQ, DMEM_PRIO, DMEM_PREF,AGP,
LEAD_UNFORMATTED FROM psas_di_dev.`340b_brnz`.t_lutl_phs_leads;

-- COMMAND ----------

--01 Q_340B_NationalGrp_DT 
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_340B_NATIONALGRP_DT_01 AS
SELECT DISTINCT T_PHS_AUDIT.PHS_340B_ID, IFF(NATIONAL_GRP_CD='0740','0240',NATIONAL_GRP_CD) AS NATIONAL_GROUP
FROM psas_di_dev.340b_slvr.T_PHS_AUDIT AS T_PHS_AUDIT
WHERE T_PHS_AUDIT.PHS_340B_ID <> '' AND T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('003','004','005') AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE' 
AND T_PHS_AUDIT.CUST_CHN_ID NOT IN ('716','989');

-- COMMAND ----------

--02 Q_340B_NationalGrp_CT
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_340B_NATIONALGRP_CT_02 AS
SELECT V_340B_NATIONALGRP_DT_01.PHS_340B_ID, Count(V_340B_NATIONALGRP_DT_01.NATIONAL_GROUP) AS COUNTOFNATIONAL_GROUP
FROM psas_di_dev.340b_gold.V_340B_NATIONALGRP_DT_01
GROUP BY V_340B_NATIONALGRP_DT_01.PHS_340B_ID
HAVING Count(V_340B_NATIONALGRP_DT_01.NATIONAL_GROUP) > 1;

-- COMMAND ----------

--03 Q_340B_NationalGrp
CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_340B_NATIONALGRP_03 AS
SELECT T_PHS_AUDIT.CUST_ACCT_ID, T_PHS_AUDIT.CUST_ACCT_NAME, T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.CUST_CHN_ID, T_PHS_AUDIT.CHAIN_NAM, T_PHS_AUDIT.RX_COGS, T_PHS_AUDIT.OTC_COGS, T_PHS_AUDIT.NATIONAL_GRP_CD, T_PHS_AUDIT.NATIONAL_GRP_NAME, T_PHS_AUDIT.NATIONAL_SUBGRP_CD, T_PHS_AUDIT.NATIONAL_SUBGRP_NAME, T_PHS_AUDIT.COMMENTS
FROM psas_di_dev.340b_gold.V_340B_NATIONALGRP_CT_02 INNER JOIN psas_di_dev.340b_slvr.T_PHS_AUDIT AS T_PHS_AUDIT ON V_340B_NATIONALGRP_CT_02.PHS_340B_ID = T_PHS_AUDIT.PHS_340B_ID
WHERE T_PHS_AUDIT.CUST_CHN_ID NOT IN ('989','716') AND T_PHS_AUDIT.ACCT_CLASSIFICATION IN ('005','004','003') AND UPPER(T_PHS_AUDIT.STATUS) = 'ACTIVE'
ORDER BY T_PHS_AUDIT.PHS_340B_ID, T_PHS_AUDIT.NATIONAL_GRP_CD, T_PHS_AUDIT.CUST_CHN_ID;