# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

qualifying_schema="qualifyId INT,raceId INT,driverId INT,constructorId INT,number INT,position INT,q1 STRING,q2 STRING,q3 STRING"


# COMMAND ----------

dbutils.widgets.text("file_date",'2021-03-21')
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiline",True).json(f"{raw_folder_path}/{file_date}/qualifying")

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
qualifying_df=qualifying_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("constructorID","constructor_id")\
.withColumn("datasource",lit(v_data_source))\
.withColumn('file_date',lit(file_date))
qualifying_df=add_ingestion_date(qualifying_df)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

#qualifying_df.write.mode('overwrite').parquet(f"{processed_folder_path}/qualifying")
#qualifying_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.qualifying")
#incremental(qualifying_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

merge_condition='tgt.qualify_id=src.qualify_id and tgt.race_id=src.race_id'
merge_delta_data(qualifying_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("success ")