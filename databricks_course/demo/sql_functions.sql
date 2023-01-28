-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select driver_ref,name,lag(name,1) over(partition by nationality order by name asc) from drivers

-- COMMAND ----------

