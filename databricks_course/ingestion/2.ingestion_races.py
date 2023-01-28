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

# MAGIC %fs
# MAGIC ls /mnt/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType
from pyspark.sql.functions import col,to_timestamp,concat,lit,current_timestamp

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceid",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitid",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",StringType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True),
                               ])

# COMMAND ----------

races_df=spark.read.options(header=True).schema(races_schema).csv(f"{raw_folder_path}/{file_date}/races.csv")

# COMMAND ----------

races_df=races_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("datasource",lit(v_data_source)).withColumn("file_date",lit(file_date))
races_df=add_ingestion_date(races_df)


# COMMAND ----------


races_select_df=races_df.select(col("raceid").alias("race_id"),col("year").alias("race_year")\
                               ,col("round"),col("circuitid").alias("circuit_id")\
                               ,col("name"),col("date"),col("race_timestamp"),col("datasource"),col("file_date"),col("ingestion_date"))

# COMMAND ----------

#races_select_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
#races_select_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.races")
races_select_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

