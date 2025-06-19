-- Databricks notebook source
create table demo (id int, name string)

-- COMMAND ----------

insert into demo values(1, 'A');
insert into demo values(2, 'B');
insert into demo values(3, 'C');
insert into demo values(4, 'D');
insert into demo values(5, 'E');
insert into demo values(6, 'F');
insert into demo values(7, 'G');

-- COMMAND ----------

/* THis will show us how many version and files there */
desc history demo

-- COMMAND ----------

desc extended demo

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo where name='a'

-- COMMAND ----------

select * from demo

-- COMMAND ----------

optimize demo
zorder by (id)

-- COMMAND ----------

desc history demo

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo

-- COMMAND ----------

select * from demo

-- COMMAND ----------

restore table demo to version as of 12

-- COMMAND ----------

vacuum demo

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

select * from demo version as of  7
