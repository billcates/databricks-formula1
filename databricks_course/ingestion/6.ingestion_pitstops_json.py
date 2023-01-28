# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

pitstop_schema="raceId INT,driverId INT,stop STRING,lap INT,time STRING,duration STRING,milliseconds INT"


# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

pit_stops_df=spark.read.schema(pitstop_schema).option("multiline",True).json(f"{raw_folder_path}/{file_date}/pit_stops.json")

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pitstops_df=pit_stops_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("datasource",lit(v_data_source))\
.withColumn("file_date",lit(file_date))
pitstops_df=add_ingestion_date(pitstops_df)

# COMMAND ----------

#pitstops_df.write.mode('overwrite').parquet(f"{processed_folder_path}/pit_stops")
#pitstops_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pit_stops")
#incremental(pitstops_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

merge_condition='tgt.driver_id=src.driver_id and tgt.race_id=src.race_id and tgt.stop=src.stop and tgt.race_id=src.race_id'
merge_delta_data(pitstops_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

