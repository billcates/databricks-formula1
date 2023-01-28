# Databricks notebook source
# MAGIC %md
# MAGIC #ingest lap times folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

lap_times_schema="raceId INT,driverId INT,lap INT,position INT,time STRING,milliseconds INT"


# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
laptimes_df=lap_times_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("datasource",lit(v_data_source))\
.withColumn("file_date",lit(file_date))
laptimes_df=add_ingestion_date(laptimes_df)

# COMMAND ----------

#laptimes_df.write.mode('overwrite').parquet(f"{processed_folder_path}/lap_times")
#laptimes_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.lap_times")
#incremental(laptimes_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

merge_condition='tgt.driver_id=src.driver_id and tgt.lap=src.lap and tgt.race_id=src.race_id'
merge_delta_data(laptimes_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

