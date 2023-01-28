# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ####include child notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source=dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
p_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitid",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)
                                  ])

# COMMAND ----------

circuits_df=spark.read.options(header='True')\
            .schema(circuits_schema)\
            .csv(f"dbfs:{raw_folder_path}/{p_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #select only required columns

# COMMAND ----------

circuits_df=circuits_df.select('circuitid','circuitRef','name','location','country','lat','lng','alt')

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuits_selected_df=circuits_df.select(col('circuitid'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC #renaming the columns 

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitid","circuit_id")\
                    .withColumnRenamed("circuitRef","circuit_ref")\
                    .withColumnRenamed("lat","latitude")\
                    .withColumnRenamed("lng","longitude")\
                    .withColumnRenamed("alt","altitude")\
                    .withColumn("file_date",lit(p_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #write to parquet file

# COMMAND ----------

#circuits_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#circuits_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

circuits_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/circuits

# COMMAND ----------

dbutils.notebook.exit("success")