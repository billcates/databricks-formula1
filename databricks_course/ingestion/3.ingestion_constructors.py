# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("datasource","")
v_data_source=dbutils.widgets.get("datasource")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructor_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df=spark.read.schema(constructor_schema).json(f'{raw_folder_path}/{file_date}/constructors.json')

# COMMAND ----------

constructor_df=constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
constructor_df=constructor_df.withColumnRenamed("constructorId","constructor_id")\
                            .withColumnRenamed("constructorRef","constructor_ref")\
                            .withColumn("datasource",lit(v_data_source))\
                            .withColumn('file_date',lit(file_date))
constructor_df=add_ingestion_date(constructor_df)

# COMMAND ----------

#constructor_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')
#constructor_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.constructors")
constructor_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

