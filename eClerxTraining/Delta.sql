-- Databricks notebook source
create table emp(id int, name string, city string)

-- COMMAND ----------

desc extended emp;

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values(1,"Pankaj","Pune")

-- COMMAND ----------

select * from emp

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

insert into emp values(2,'Saavi', 'Mumbai'),(3,'Anika', 'Kolhapur'),(4,'Aarti','Solapur');

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values(5, 'Himanshi', 'Mumbai');
insert into emp values(6, 'Nilesh', 'Indore');

-- COMMAND ----------

desc history emp;
desc detail emp;

-- COMMAND ----------

delete from emp where id = 2

-- COMMAND ----------

desc detail emp;

-- COMMAND ----------

delete from emp where id = 1

-- COMMAND ----------

select * from emp version as of 4

-- COMMAND ----------

select * from emp timestamp as of '2025-06-09T07:05:30.000+00:00'
