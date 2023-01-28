# Databricks notebook source
dbutils.fs.mounts()


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
file_date=dbutils.widgets.get("p_file_date")


# COMMAND ----------

race_df=spark.read.format("delta").load(f"/{processed_folder_path}/races").withColumnRenamed("circuit_id","race_circuit_id")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("date","race_date")
race_df=race_df.select("race_id","race_circuit_id","race_year","race_name","race_date")

# circuits_df=spark.read.parquet(f"/{processed_folder_path}/circuits")\
# .withColumnRenamed("circuit_id","circuits_circuit_id")\
# .withColumnRenamed("location","circuit_location")\
# .select("circuits_circuit_id","circuit_location")

circuits_df=spark.read.format('delta').load(f"/{processed_folder_path}/circuits")\
.withColumnRenamed("circuit_id","circuits_circuit_id")\
.withColumnRenamed("location","circuit_location")\
.select("circuits_circuit_id","circuit_location")


# COMMAND ----------

circuits_races=circuits_df.join(race_df,race_df.race_circuit_id==circuits_df.circuits_circuit_id)

# COMMAND ----------

from pyspark.sql import functions as fn

# COMMAND ----------

# results_df=spark.read.parquet(f"/{processed_folder_path}/results").filter(f'file_date="{file_date}"').select(fn.col("race_id").alias('q_race_id'),\
#                                                                                fn.col("driver_id").alias('q_driver_id'),\
#                                                                                fn.col("constructor_id").alias('q_constructor_id'),\
#                                                                          fn.col('grid'),fn.col('fastest_lap'),\
#                                                                          fn.col('time').alias('race_time'),fn.col('position'),\
#                                                                          fn.col('points'),fn.col('file_date'))

# COMMAND ----------

results_df=spark.read.format('delta').load(f"/{processed_folder_path}/results").filter(f'file_date="{file_date}"').select(fn.col("race_id").alias('q_race_id'),\
                                                                               fn.col("driver_id").alias('q_driver_id'),\
                                                                               fn.col("constructor_id").alias('q_constructor_id'),\
                                                                         fn.col('grid'),fn.col('fastest_lap'),\
                                                                         fn.col('time').alias('race_time'),fn.col('position'),\
                                                                         fn.col('points'),fn.col('file_date'))

# COMMAND ----------

circuits_races_drivers_df=circuits_races.join(results_df,circuits_races.race_id==results_df.q_race_id).drop('q_race_id')

# COMMAND ----------

# drivers_df=spark.read.parquet(f"/{processed_folder_path}/drivers").select(fn.col('driver_id'),fn.col('name').alias('driver_name'),\
#                                                                          fn.col('number').alias('driver_number'),\
#                                                                           fn.col('nationality').alias('driver_nationality'))

# COMMAND ----------

drivers_df=spark.read.format('delta').load(f"/{processed_folder_path}/drivers").select(fn.col('driver_id'),fn.col('name').alias('driver_name'),\
                                                                         fn.col('number').alias('driver_number'),\
                                                                          fn.col('nationality').alias('driver_nationality'))

# COMMAND ----------

master_df=drivers_df.join(circuits_races_drivers_df,circuits_races_drivers_df.q_driver_id==drivers_df.driver_id).drop('q_driver_id')

# COMMAND ----------

# constructors_df=spark.read.parquet(f"/{processed_folder_path}/constructors").select(fn.col('constructor_id'),fn.col('name').alias('team'))

# COMMAND ----------

constructors_df=spark.read.format('delta').load(f"/{processed_folder_path}/constructors").select(fn.col('constructor_id'),fn.col('name').alias('team'))

# COMMAND ----------

master_df=master_df.join(constructors_df,constructors_df.constructor_id==master_df.q_constructor_id).drop("q_constructor_id")

# COMMAND ----------

final_df=master_df.select(fn.col('race_id'),fn.col('race_year'),\
                          fn.col('race_name'),\
                          fn.col('race_date'),\
                          fn.col('circuit_location'),\
                           fn.col('driver_name'),\
                           fn.col('driver_number'),\
                           fn.col('driver_nationality'),\
                          fn.col('team'),\
                          fn.col('grid'),\
                          fn.col('fastest_lap'),\
                          fn.col('race_time'),\
                          fn.col('points'),
                          fn.col('position'),
                          fn.col('file_date')
                          )

# COMMAND ----------

final_df=final_df.withColumn('created_date',fn.current_timestamp())

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(f"/{presentation_folder_path}/race_results")
#final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
#incremental(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------

merge_condition='tgt.driver_name=src.driver_name and tgt.race_id=src.race_id'
merge_delta_data(final_df,'f1_presentation','race_results',presentation_folder_path,merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_presentation.race_results

# COMMAND ----------

