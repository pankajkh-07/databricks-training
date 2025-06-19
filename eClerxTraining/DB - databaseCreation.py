# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE if NOT EXISTS practDB;
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO practdb.products (pname,pdescription,price,inStock) values("prod1","product dummy description", 100, true);
# MAGIC INSERT INTO practdb.products (pname,pdescription,price,inStock) values("prod2","product dummy description", 50, false);
# MAGIC INSERT INTO practdb.products (pname,pdescription,price,inStock) values("prod3","product dummy description", 70, true);
# MAGIC INSERT INTO practdb.products (pname,pdescription,price,inStock) values("prod4","product dummy description", 80, true);

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/eclerx_input_files");


# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/raw/Test_Batch_2025_05_29.csv","dbfs:/FileStore/eclerx_input_files");

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/raw/");
