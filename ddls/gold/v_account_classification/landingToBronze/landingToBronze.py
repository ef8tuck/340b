# Databricks notebook source
import dlt
from pyspark.sql.functions import col



# COMMAND ----------

 #settig  up external volumep
external_volume_path = "/Volumes/psas_di_dev/340b_landing/static"

# COMMAND ----------

@dlt.table(
    name="t_account_mnc_list",
    comment="Processed data from external volume/ t_account_mnc_list.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_account_mnc_list():      
    # Read the files given they are in CSV format
    t_account_mnc_list = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load(external_volume_path+"/T_ACCOUNT_MNC_LIST.csv")
    #t_account_mnc_list.show()
   
    # transformations code here
    
    return t_account_mnc_list


# COMMAND ----------

@dlt.table(
    name="T_ACTIVE_STATUS_UPDATE",
    comment="Processed data from external volume/ t_activestatusupdatet.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_account_mnc_list():      
    # Read the files given they are in CSV format
    t_active_status_update = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_ACTIVESTATUSUPDATE.csv")
    #t_active_status_update.show()
    return t_active_status_update


# COMMAND ----------

@dlt.table(
    name="T_CHAINS",
    comment="Processed data from external volume/ t_activestatusupdatet.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_CHAINS():      
    # Read the files given they are in CSV format
    T_CHAINS = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_CHAINS.csv")
    #T_CHAINS.show()
    return T_CHAINS


# COMMAND ----------

@dlt.table(
    name="T_CONTRACTPHARMACY_EXCEPTIONS",
    comment="Processed data from external volume/ T_CONTRACTPHARMACY_EXCEPTIONS.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_CONTRACTPHARMACY_EXCEPTIONS():      
    # Read the files given they are in CSV format
    T_CONTRACTPHARMACY_EXCEPTIONS = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_CONTRACTPHARMACY_EXCEPTIONS.csv")
    #T_CONTRACTPHARMACY_EXCEPTIONS.show()
    return T_CONTRACTPHARMACY_EXCEPTIONS


# COMMAND ----------

@dlt.table(
    name="T_GPO_MANAGERS",
    comment="Processed data from external volume/ T_GPO_MANAGERS.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_GPO_MANAGERS():      
    # Read the files given they are in CSV format
    T_GPO_MANAGERS = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_GPO_MANAGERS.csv")
    #T_GPO_MANAGERS.show()
    return T_GPO_MANAGERS


# COMMAND ----------

@dlt.table(
    name="T_LUQ_ACCOUNT",
    comment="Processed data from external volume/ T_LUQ_ACCOUNT.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_LUQ_ACCOUNT():      
    # Read the files given they are in CSV format
    T_LUQ_ACCOUNT = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_LUQ_ACCOUNT.csv")
    #T_LUQ_ACCOUNT.show()
    return T_LUQ_ACCOUNT


# COMMAND ----------

@dlt.table(
    name="T_LUTL_ENTITY",
    comment="Processed data from external volume/ T_LUTL_ENTITY.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_LUTL_ENTITY():      
    # Read the files given they are in CSV format
    T_LUTL_ENTITY = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_LUTL_ENTITY.csv")
    #T_LUTL_ENTITY.show()
    return T_LUTL_ENTITY


# COMMAND ----------

@dlt.table(
    name="T_LUTL_NON_ORDERING_ACCOUNTS",
    comment="Processed data from external volume/ T_LUTL_NON_ORDERING_ACCOUNTS.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_LUTL_NON_ORDERING_ACCOUNTS():      
    # Read the files given they are in CSV format
    T_LUTL_NON_ORDERING_ACCOUNTS = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_LUTL_NON_ORDERING_ACCOUNTS.csv")
    #T_LUTL_NON_ORDERING_ACCOUNTS.show()
    return T_LUTL_NON_ORDERING_ACCOUNTS


# COMMAND ----------

@dlt.table(
    name="T_LUTL_PHS_LEADS",
    comment="Processed data from external volume/ t_activestatusupdatet.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_LUTL_PHS_LEADS():      
    # Read the files given they are in CSV format
    T_LUTL_PHS_LEADS = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_LUTL_PHS_LEADS.csv")
    #T_LUTL_PHS_LEADS.show()
    return T_LUTL_PHS_LEADS


# COMMAND ----------

@dlt.table(
    name="T_LUTL_SALES_ADMIN",
    comment="Processed data from external volume/ T_LUTL_SALES_ADMIN.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_LUTL_SALES_ADMIN():      
    # Read the files given they are in CSV format
    T_LUTL_SALES_ADMIN = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_LUTL_SALES_ADMIN.csv")
    #T_LUTL_SALES_ADMIN.show()
    return T_LUTL_SALES_ADMIN


# COMMAND ----------

@dlt.table(
    name="T_MT_LEAD_INELIGIBLE_LIST",
    comment="Processed data from external volume/ T_MT_LEAD_INELIGIBLE_LIST.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_MT_LEAD_INELIGIBLE_LIST():      
    # Read the files given they are in CSV format
    T_MT_LEAD_INELIGIBLE_LIST = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_MT_LEAD_INELIGIBLE_LIST.csv")
    #T_MT_LEAD_INELIGIBLE_LIST.show()
    return T_MT_LEAD_INELIGIBLE_LIST


# COMMAND ----------

@dlt.table(
    name="T_MT_OVERRIDE_LEAD_LIST",
    comment="Processed data from external volume/ T_MT_OVERRIDE_LEAD_LIST.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_MT_OVERRIDE_LEAD_LIST():      
    # Read the files given they are in CSV format
    T_MT_OVERRIDE_LEAD_LIST = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_MT_OVERRIDE_LEAD_LIST.csv")
    #T_MT_OVERRIDE_LEAD_LIST.show()
    return T_MT_OVERRIDE_LEAD_LIST


# COMMAND ----------

@dlt.table(
    name="T_MT_PVP_REPORT_PARENTMATCH",
    comment="Processed data from external volume/ T_MT_PVP_REPORT_PARENTMATCH.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_MT_PVP_REPORT_PARENTMATCHT():      
    # Read the files given they are in CSV format
    T_MT_PVP_REPORT_PARENTMATCH = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_MT_PVP_REPORT_PARENTMATCH.csv")
    #T_MT_PVP_REPORT_PARENTMATCH.show()
    return T_MT_PVP_REPORT_PARENTMATCH


# COMMAND ----------

@dlt.table(
    name="T_PHARMACY_TYPE_DATA",
    comment="Processed data from external volume/ T_PHARMACY_TYPE_DATA.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_MT_OVERRIDE_LEAD_LIST():      
    # Read the files given they are in CSV format
    T_PHARMACY_TYPE_DATA = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_PHARMACY_TYPE_DATA.csv")
    #T_PHARMACY_TYPE_DATA.show()
    return T_PHARMACY_TYPE_DATA


# COMMAND ----------

@dlt.table(
    name="T_TPV",
    comment="Processed data from external volume/T_TPV.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_T_TPVT():      
    # Read the files given they are in CSV format
    T_TPV = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_TPV.csv")
    #T_TPV.show()
    return T_TPV


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os

spark = SparkSession.builder.appName("DynamicTableCreation").getOrCreate()

external_volume_path = "/Volumes/psas_di_dev/340b_landing/static"  # Path to store tables inside the volume

files = dbutils.fs.ls(external_volume_path)
csv_files = [file.path for file in files if file.path.endswith(".csv")]
for file_path in csv_files:
    table_name = os.path.basename(file_path).split(".")[0]
    try:
        @dlt.table(
            name=table_name,
            comment=f"Delta table for {table_name} CSV file."
        )
        def load_table():
            # Read the CSV file into a DataFrame
            df = spark.read.option("header", "true").option("inferschema","true").csv(file_path)
            return df

        print(f"Delta table '{table_name}' created successfully.")
    
    except AnalysisException as e:
        print(f"Error processing file '{file_path}': {str(e)}")
load_table()

# COMMAND ----------

@dlt.table(
    name="T_PHS_AUDIT",
    comment="Processed data from external volume/ t_phs_audit.csv",
    table_properties={
        "quality": "bronze"
    }
)
def load_account_mnc_list():      
    # Read the files given they are in CSV format
    T_PHS_AUDIT = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(external_volume_path+"/T_PHS_AUDIT.csv")
    #t_active_status_update.show()
    return T_PHS_AUDIT

