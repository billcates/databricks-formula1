# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

result_schema="resultId INT,raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points DOUBLE,laps INT,time STRING,milliseconds INT,fastestLap INT,rank INT,fastestLapTime STRING,fastestLapSpeed STRING,statusId INT"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
file_date=dbutils.widgets.get("p_file_date")


# COMMAND ----------

results_df=spark.read.schema(result_schema).json(f'{raw_folder_path}/{file_date}/results.json')

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
results_df=results_df.drop('status_id')\
.withColumnRenamed('resultId','result_id')\
.withColumnRenamed('raceId','race_id')\
.withColumnRenamed('driverId','driver_id')\
.withColumnRenamed('constructorId','constructor_id')\
.withColumnRenamed('positionText','position_text')\
.withColumnRenamed('positionOrder','position_order')\
.withColumnRenamed('fastestLap','fastest_lap')\
.withColumnRenamed('fastestLapTime','fastest_lap_time')\
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
.withColumn('datasource',lit(v_data_source))\
.withColumn('file_date',lit(file_date))
results_df=add_ingestion_date(results_df)

# COMMAND ----------

results_dedup_df=results_df.dropDuplicates(['race_id','driver_id'])
reults_df=results_dedup_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##method 1 for incremental load

# COMMAND ----------

# lt=results_df.select("race_id").distinct().collect()
# for each in lt:
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {each.race_id})")

# COMMAND ----------

#results_df.write.mode('overwrite').partitionBy('race_id').parquet(f'{processed_folder_path}/results')
#results_df.write.mode('append').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# results_df=results_df.select("result_id"
# ,"driver_id"
# ,"constructor_id"
# ,"number"
# ,"grid"
# ,"position"
# ,"position_text"
# ,"position_order"
# ,"points"
# ,"laps"
# ,"time"
# ,"milliseconds"
# ,"fastest_lap"
# ,"rank"
# ,"fastest_lap_time"
# ,"fastest_lap_speed"
# ,"statusId"
# ,"datasource"
# ,"file_date"
# ,"ingestion_date","race_id")

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     results_df.write.mode('overwrite').insertInto("f1_processed.results")
# else:
#     results_df.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition='tgt.result_id=src.result_id and tgt.race_id=src.race_id'
merge_delta_data(results_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

spark.read.format('delta').load('/mnt/processed/results')

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

