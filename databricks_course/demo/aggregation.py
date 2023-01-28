# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

play_df=spark.read.parquet(f"/{presentation_folder_path}/race_results")

# COMMAND ----------

display(play_df)

# COMMAND ----------

from pyspark.sql.functions import sum,countDistinct

# COMMAND ----------

df=play_df.groupBy('driver_name').agg(sum("points"),countDistinct("race_name"))

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###window functions

# COMMAND ----------

demo_df=play_df.where("race_year in (2019,2020)" )

# COMMAND ----------

demo_df=demo_df.groupBy('race_year','driver_name').agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,dense_rank

spec=Window.partitionBy("race_year").orderBy(desc("total_points"))
df=demo_df.withColumn("rank",dense_rank().over(spec))

# COMMAND ----------

display(df)

# COMMAND ----------

