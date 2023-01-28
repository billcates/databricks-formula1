# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                              StructField("surname",StringType(),True)])

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),False),
                                 StructField("driverRef",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("name",name_schema),
                                 StructField("dob",DateType(),True),
                                 StructField("nationality",StringType(),True),
                                 StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json(f'{raw_folder_path}/{file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #rename the columns and add the ingested_date column

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

drivers_df_final=drivers_df.withColumnRenamed("driverId",'driver_id')\
                    .withColumnRenamed("driverRef",'driver_ref')\
                    .withColumn("name",concat(drivers_df["name.forename"],lit(" "),drivers_df["name.surname"]))\
                    .withColumn("datasource",lit(v_data_source))\
                    .withColumn("file_date",lit(file_date))
drivers_df_final=add_ingestion_date(drivers_df_final)

# COMMAND ----------

drivers_df_final.drop('url')

# COMMAND ----------

#drivers_df_final.write.mode('overwrite').parquet(f"{processed_folder_path}/drivers")
#drivers_df_final.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")
drivers_df_final.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("success")