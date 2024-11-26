-- Databricks notebook source
CREATE TABLE IF NOT EXISTS $$unity_catalog_name.$$schema_name.$$delta_table_name
(
  Cust_Acct_ID STRING
)
USING DELTA
LOCATION '$$delta_table_location'
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.enableChangeDataFeed' = true,
  'spark.sql.files.ignoreMissingFiles'= true,
  'delta.autoOptimize.optimizeWrite' = true);
-- INSERT INTO $$unity_catalog_name.$$schema_name.$$delta_table_name (
--   Cust_Acct_ID
-- ) VALUES (
--   '-9999',
-- );

