-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet(f"/{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('parquet').saveAsTable("race_results_python")

-- COMMAND ----------

create database demo

-- COMMAND ----------

use demo


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('parquet').saveAsTable("race_results_python")

-- COMMAND ----------

select * from demo.race_results_python

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python

-- COMMAND ----------

show tables in default

-- COMMAND ----------

desc table extended race_results_python

-- COMMAND ----------

drop table default.race_results_python


-- COMMAND ----------

create or replace temp view t_v_race_results_python
as 
select * from race_results_python limit 10

-- COMMAND ----------

select * from t_v_race_results_python

-- COMMAND ----------

create or replace global temp view g_t_v_race_results_python
as 
select * from race_results_python where race_year=2019

-- COMMAND ----------

select * from global_temp.g_t_v_race_results_python

-- COMMAND ----------

