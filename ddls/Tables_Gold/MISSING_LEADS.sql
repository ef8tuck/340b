-- Databricks notebook source

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_WAC_LEADS_01 AS SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_FLAG, A.WAC, A.EXPANSION
FROM psas_di_dev.`340b_brnz`.t_lutl_phs_leads AS A 
WHERE (A.LEAD != '293388' AND A.WAC ='Yes');

-- COMMAND ----------

CREATE OR REPLACE VIEW psas_di_dev.340b_gold.V_WAC_ACCOUNTS_02 AS 
SELECT A.CUST_ACCT_ID, A.CUST_ACCT_NAME, B.LEAD, B.LEAD_NAME, A.PHS_340B_ID, REPLACE(UPPER(A.PVP_PARTICIPATION_FLAG),'TRUE','Y') AS PVP_PARTICIPATION_FLAG, A.ENTITY_TYPE
FROM psas_di_dev.340b_brnz.T_PHS_AUDIT AS A CROSS JOIN psas_di_dev.340b_gold.V_WAC_LEADS_01 AS B 
WHERE ((A.PVP_PARTICIPATION_FLAG='Y' OR UPPER(A.PVP_PARTICIPATION_FLAG)='TRUE') AND (A.ENTITY_TYPE='CAN' OR A.ENTITY_TYPE='DSH' OR A.ENTITY_TYPE='PED') 
       AND (A.CUST_CHN_ID != '989') AND (A.ACCT_CLASSIFICATION='003') AND (UPPER(A.STATUS)='ACTIVE') 
       AND (CHARINDEX('A34', A.CUST_ACCT_NAME)>0));

-- COMMAND ----------

INSERT INTO T_MT_WAC_DMEM ( CUST_ACCT_ID, LEAD, LEAD_NAME, ENTITY_TYPE )
SELECT A.CUST_ACCT_ID, A.LEAD,A.LEAD_NAME, A.ENTITY_TYPE
FROM V_WAC_ACCOUNTS_02 AS A INNER JOIN V_WAC_LEADS_01 AS B ON (A.PVP_PARTICIPATION_FLAG = B.PVP_FLAG) AND (A.LEAD = B.LEAD);


-- COMMAND ----------

------------------05 Q_WAC_DMEM_Missing
CREATE OR REPLACE VIEW V_WAC_DMEM_MISSING_05 AS 
SELECT DISTINCT A.CUST_ACCT_ID, C.CUST_ACCT_NAME, C.ZX_BLOCK, C.SALES_CURMTH, C.HRSA_START_DATE, C.HRSA_TERM_DATE, C.PVP_PARTICIPATION_FLAG AS PVP_FLAG, C.PVP_ELIGIBILITY_DATE, A.LEAD, A.LEAD_NAME, C.PHS_340B_ID, A.Expansion_Entity, A.ENTITY_TYPE, C.STATUS, C.CUST_CHN_ID FROM T_MT_WAC_DMEM AS A LEFT JOIN T_DMEM_LIST AS B ON ((A.LEAD = B.CNTRCT_LEAD_ID) AND (A.CUST_ACCT_ID = B.CUST_ACCT_ID)) INNER JOIN T_PHS_AUDIT AS C ON A.CUST_ACCT_ID = C.CUST_ACCT_ID
WHERE (UPPER(C.STATUS)='ACTIVE' AND (C.CUST_CHN_ID != '989') AND (B.CUST_ACCT_ID IS NULL) AND (B.CNTRCT_LEAD_ID IS NULL))
ORDER BY A.CUST_ACCT_ID;

-- COMMAND ----------


DELETE FROM T_MT_PHS_DMEM;

-- COMMAND ----------

-------------------------------------01 Q_PHS_Leads
CREATE OR REPLACE VIEW V_PHS_LEADS_01 AS 
SELECT DISTINCT A.LEAD, A.LEAD_NAME, A.PVP_Flag, A.PHS, A.Expansion
FROM T_LUTL_PHS_LEADS AS A 
WHERE ((A.LEAD != '293388') AND (A.PHS='Yes'));

-- COMMAND ----------

CREATE OR REPLACE VIEW V_PHS_ACCOUNTS_02 AS 
SELECT A.CUST_ACCT_ID, A.CUST_ACCT_NAME,REPLACE(UPPER(A.PVP_PARTICIPATION_FLAG),'TRUE','Y') AS PVP_PARTICIPATION_FLAG, B.LEAD, B.LEAD_NAME, A.PHS_340B_ID, CASE
    WHEN ENTITY_TYPE='RRC' THEN 'Y'
	WHEN ENTITY_TYPE='SCH' THEN 'Y'
	WHEN ENTITY_TYPE='CAN' THEN 'Y'
	WHEN ENTITY_TYPE='CAH' THEN 'Y'
	ELSE 'N'
END AS Expansion_Entity, A.ENTITY_TYPE
FROM T_PHS_AUDIT AS A, V_PHS_LEADS_01 as B 
WHERE ((A.CUST_CHN_ID != '989') AND (A.ACCT_CLASSIFICATION in ('004','005')) AND (UPPER(A.STATUS)='ACTIVE'));

-- COMMAND ----------

INSERT INTO T_MT_PHS_DMEM ( CUST_ACCT_ID, LEAD, LEAD_NAME, Expansion_Entity, ENTITY_TYPE )
SELECT A.CUST_ACCT_ID, A.LEAD, A.LEAD_NAME, A.Expansion_Entity, A.ENTITY_TYPE
FROM V_PHS_ACCOUNTS_02 AS A INNER JOIN V_PHS_LEADS_01 AS B ON (A.LEAD = B.LEAD) AND ((A.Expansion_Entity = B.Expansion) OR (A.PVP_PARTICIPATION_FLAG = B.PVP_Flag));

-- COMMAND ----------


------------05 AQ_PHS_Accounts_DMEM_PVP ... Merged with 4 
--INSERT INTO T_MT_PHS_DMEM_TEMP ( CUST_ACCT_ID, LEAD, LEAD_NAME, Expansion_Entity, ENTITY_TYPE )
---SELECT A.CUST_ACCT_ID, A.LEAD, A.LEAD_NAME, A.Expansion_Entity, A.ENTITY_TYPE
--FROM V_PHS_ACCOUNTS_02 AS A INNER JOIN V_PHS_LEADS_01 AS B ON (A.PVP_PARTICIPATION_FLAG = B.PVP_Flag) AND (A.LEAD = B.LEAD);

-- COMMAND ----------

------------06 Q_PHS_DMEM_Missing
CREATE OR REPLACE VIEW V_PHS_DMEM_MISSING_06 AS 
SELECT A.CUST_ACCT_ID, C.CUST_ACCT_NAME, C.ZX_BLOCK, C.SALES_CURMTH, C.HRSA_START_DATE, C.HRSA_TERM_DATE, C.PVP_PARTICIPATION_FLAG AS PVP_Flag, C.PVP_ELIGIBILITY_DATE, A.LEAD, A.LEAD_NAME, C.PHS_340B_ID, A.Expansion_Entity, A.ENTITY_TYPE, C.COMMENTS, C.STATUS
FROM T_MT_PHS_DMEM AS A LEFT JOIN T_DMEM_LIST AS B ON (A.LEAD = B.CNTRCT_LEAD_ID) AND (A.CUST_ACCT_ID = B.CUST_ACCT_ID) INNER JOIN T_PHS_AUDIT AS C ON A.CUST_ACCT_ID = C.CUST_ACCT_ID
WHERE ((UPPER(C.STATUS)='ACTIVE') AND (B.CUST_ACCT_ID IS NULL) AND (B.CNTRCT_LEAD_ID IS NULL))
ORDER BY A.CUST_ACCT_ID;

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