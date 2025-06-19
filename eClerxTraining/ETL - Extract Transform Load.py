# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/eclerx_input_files/

# COMMAND ----------

#ETL - Extract Transform Load
input_path="dbfs:/FileStore/eclerx_input_files/circuits.csv"

#df=spark.read.csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
#df=spark.read.option("header",True).csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
#df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")

df=spark.read.csv(input_path,header=True,inferSchema=True)
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists formula1

# COMMAND ----------

from pyspark.sql.functions import *

input_path="dbfs:/FileStore/eclerx_input_files/circuits.csv"
df=spark.read.csv(input_path,header=True,inferSchema=True)

df_final=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitref":"circuit_ref"})\
.withColumn("ingestion_date",current_date())\
.drop("url")

df_final.write.mode("overwrite").saveAsTable("formula1.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.circuit
