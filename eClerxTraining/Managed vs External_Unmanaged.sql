-- Databricks notebook source
-- MAGIC %md
-- MAGIC Managed
-- MAGIC Meta data files created at default location by databricks
-- MAGIC
-- MAGIC External / Unmanaged Table
-- MAGIC User provides the location where he wants to create the metadata for tables

-- COMMAND ----------

create table demo_vendors (id int, name string) location '/FileStore/eclerx_metadata/vendors'

-- COMMAND ----------

insert into demo_vendors values (1,'a')

-- COMMAND ----------

drop table demo_vendors
