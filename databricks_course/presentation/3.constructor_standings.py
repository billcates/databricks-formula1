# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list=spark.read.format('delta').load(f'/{presentation_folder_path}/race_results')\
.filter(f"file_date='{file_date}'")\
.select("race_year").distinct().collect()

# COMMAND ----------

lt=[]
for each in race_results_list:
    lt.append(each.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df=spark.read.format('delta').load(f'/{presentation_folder_path}/race_results')\
.filter(col('race_year').isin(lt))

# COMMAND ----------

from pyspark.sql import functions as fn 

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##constructor standings

# COMMAND ----------

constructor_standings=race_results_df.groupBy("race_year","team")\
.agg(fn.count(fn.when(fn.col("position")==1,True)).alias("wins"),fn.sum("points").alias("total_points"))

# COMMAND ----------

team_spec=Window.partitionBy("race_year").orderBy(fn.desc("total_points"))
constructor_standings=constructor_standings.withColumn("rank",fn.rank().over(team_spec))

# COMMAND ----------

#constructor_standings.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')
#incremental(constructor_standings,'f1_presentation','constructor_standings','race_year')
merge_condition='tgt.team=src.team and tgt.race_year=src.race_year'
merge_delta_data(constructor_standings,'f1_presentation','constructor_standings',presentation_folder_path,merge_condition,'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings

# COMMAND ----------

