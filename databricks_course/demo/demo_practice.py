# Databricks notebook source
res=sc.parallelize([1,2,3,4,5],2)

# COMMAND ----------

display(res)

# COMMAND ----------

df=res.toDF(['age'])

# COMMAND ----------

