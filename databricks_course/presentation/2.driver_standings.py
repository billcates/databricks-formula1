# Databricks notebook source
# MAGIC %md
# MAGIC ##driver standings

# COMMAND ----------

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

driver_standings_df=race_results_df.groupBy("race_year","driver_name","driver_nationality")\
.agg(fn.sum("points").alias("total_points"),fn.count(fn.when(fn.col("position")==1,True)).alias("wins"))


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

spec=Window.partitionBy("race_year").orderBy(fn.desc("total_points"),fn.desc("wins"))
df=driver_standings_df.withColumn('rank',fn.rank().over(spec))

# COMMAND ----------

#df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')
#incremental(df,'f1_presentation','driver_standings','race_year')

# COMMAND ----------

merge_condition='tgt.driver_name=src.driver_name and tgt.race_year=src.race_year'
merge_delta_data(df,'f1_presentation','driver_standings',presentation_folder_path,merge_condition,'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings

# COMMAND ----------

