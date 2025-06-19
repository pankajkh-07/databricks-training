# Databricks notebook source
# MAGIC %sql
# MAGIC create database formula1;
# MAGIC create table formula1.emp (id int, name string, age int);

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc extended hive_metastore.formula1.emp
