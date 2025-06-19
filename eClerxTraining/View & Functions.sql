-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### View are like virtual table
-- MAGIC 1. Standard view 
-- MAGIC     - which get persisted, means its get saved  - to schema or catalog
-- MAGIC     - We can create standard view by SQL query
-- MAGIC 2. Temp view
-- MAGIC     - Not persisted 
-- MAGIC     - created using SQL OR PySpark

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("bike_day")

-- COMMAND ----------

select * from bike_day

-- COMMAND ----------

create or replace view max_month as 
select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

desc extended max_month

-- COMMAND ----------

select * from max_month

-- COMMAND ----------

show views

-- COMMAND ----------

create or replace temp view holiday_count_temp as
select mnth, count(*) as count from default.bike_day where holiday = 1  group by mnth order by mnth

-- COMMAND ----------

select * from holiday_count_temp 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDF
-- MAGIC
-- MAGIC   Create function function_name(para datatype, para datatype)
-- MAGIC   returns datatype
-- MAGIC   return logic/expression
-- MAGIC

-- COMMAND ----------

create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ", last_name)

-- COMMAND ----------

select fullname("Pankaj", "Khedekar") as Fullname

-- COMMAND ----------

create or replace table default.example (id int, firstname string, surname string, age int);
insert into default.example values (1, 'Akash',"More", 33),(2, 'Shruti', 'More', 33),(3,'Akshay','Khedekar',30),(4,  'Vitthal', 'Khedekar',62),(5,'Saavi','Khedekar',5),(6,'Anika','Khedekar',13)

-- COMMAND ----------

select id, fullname(firstname,surname) as fullname, age from default.example

-- COMMAND ----------

/*create or replace function(age int)
returns string
return */

-- SQL function to determine age category (Senior, Teenager, or Adult)
CREATE OR REPLACE FUNCTION Age_Category(
    age INT
) RETURNS string
RETURN
CASE
WHEN age >= 60 THEN 'SENIOR'
WHEN age >= 18 THEN 'ADULT'
WHEN age >=13  THEN 'TEENAGER'
WHEN age <= 12 THEN 'KID'
END

-- COMMAND ----------

select id, fullname(firstname,surname) as Fullname, Age_Category(age) as Category from default.example
