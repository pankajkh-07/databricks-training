# Databricks notebook source
spark

# COMMAND ----------

user_data = ([(1,"Pankaj",36),(2,"Nilesh",39),(3,"Shashi",43)])
user_schema="id int, name string, age int"
df=spark.createDataFrame(data=user_data, schema=user_schema)



# COMMAND ----------

df.show()
df.display()

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("id").display()
df.select("*").show()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df1=df.select(col("id").alias("emp_id"))

# COMMAND ----------

df1.show()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").withColumnRenamed("name","emp_name").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()

# COMMAND ----------

#Do not store the result into df2
df2=df.withColumn("ingestion_date",current_date())
df2.display()
