# Databricks notebook source
status=dbutils.notebook.run("1.ingest_circuits_file",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("2.ingestion_races",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("3.ingestion_constructors",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("4.ingestion_driver_files",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("5.ingestion_results",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("6.ingestion_pitstops_json",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("7.ingestion_laptimes",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

status=dbutils.notebook.run("8.ingestion_qualifying",0,{"datasource":"Ergast api","file_date":"2021-04-18"})
status

# COMMAND ----------

