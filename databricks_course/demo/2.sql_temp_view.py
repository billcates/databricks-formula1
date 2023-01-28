# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"/{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from v_race_results limit 100

# COMMAND ----------

 dbutils.widgets.text("name"," ")
name=dbutils.widgets.get("name")

# COMMAND ----------

name

# COMMAND ----------

# MAGIC %md
# MAGIC ##global temp view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

