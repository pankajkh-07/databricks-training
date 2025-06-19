# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/raw");

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upload Test_Batch_2025_05_29.csv manually to raw folder

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/raw

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/eclerx_input_files");

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/raw/Test_Batch_2025_05_29.csv","dbfs:/FileStore/eclerx_input_files");

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/raw/");
