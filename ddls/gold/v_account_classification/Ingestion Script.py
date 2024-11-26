# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os
import re

def column_name(df):
    for col_name in df.columns:
        clean_name = re.sub(r'[ ,;{}()\n\t=]', '_', col_name)
        df = df.withColumnRenamed(col_name, clean_name)
    return df

source_volume = "/Volumes/psas_di_dev/340b_landing/static"  
target_database = "psas_di_dev.340b_brnz"       

spark.sql(f"USE {target_database}")

csv_files = [file.path for file in dbutils.fs.ls(source_volume) if file.path.endswith(".csv")]

for file_path in csv_files:
    table_name = os.path.basename(file_path).replace(".csv", "")

    try:
        df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(file_path))
        df = column_name(df)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{target_database}.{table_name}")
        print("Created Delta table for: " + table_name)
    except AnalysisException as e:
        print(f"Error processing {table_name}: {e}")
