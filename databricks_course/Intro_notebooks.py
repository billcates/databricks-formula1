# Databricks notebook source
# MAGIC %md
# MAGIC #introduction 
# MAGIC ##1st heading

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "date"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC cd databricks-datasets

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %fs
# MAGIC ls databricks-datasets

# COMMAND ----------

ct=0
for each in dbutils.fs.ls('/databricks-datasets'):
    ct+=1
ct

# COMMAND ----------

# MAGIC %md
# MAGIC #mounting azure blob storage

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://raw@databricksdatalake12.blob.core.windows.net',
  mount_point = '/mnt/raw',
  extra_configs = {'fs.azure.sas.raw.databricksdatalake12.blob.core.windows.net':'sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-02-07T14:30:19Z&st=2023-01-07T06:30:19Z&spr=https&sig=tCoRf2TLc9toWhpoo0t0F0%2F3V7ODYKjEz9IE5XeEupc%3D'})

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://processed@databricksdatalake12.blob.core.windows.net',
  mount_point = '/mnt/processed',
  extra_configs = {'fs.azure.sas.processed.databricksdatalake12.blob.core.windows.net':'sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-02-07T14:30:19Z&st=2023-01-07T06:30:19Z&spr=https&sig=tCoRf2TLc9toWhpoo0t0F0%2F3V7ODYKjEz9IE5XeEupc%3D'})

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://presentation@databricksdatalake12.blob.core.windows.net',
  mount_point = '/mnt/presentation',
  extra_configs = {'fs.azure.sas.presentation.databricksdatalake12.blob.core.windows.net':'sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-02-07T14:30:19Z&st=2023-01-07T06:30:19Z&spr=https&sig=tCoRf2TLc9toWhpoo0t0F0%2F3V7ODYKjEz9IE5XeEupc%3D'})

# COMMAND ----------

dbutils.fs.mount(
  source = 'wasbs://demo@databricksdatalake12.blob.core.windows.net',
  mount_point = '/mnt/demo',
  extra_configs = {'fs.azure.sas.demo.databricksdatalake12.blob.core.windows.net':'sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-02-07T14:30:19Z&st=2023-01-07T06:30:19Z&spr=https&sig=tCoRf2TLc9toWhpoo0t0F0%2F3V7ODYKjEz9IE5XeEupc%3D'})

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/raw1")

# COMMAND ----------

dbutils.fs.ls("/mnt/demo")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC   val arrayStructureData = Seq(
# MAGIC     Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
# MAGIC     Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
# MAGIC     Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
# MAGIC     Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
# MAGIC     Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
# MAGIC     Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
# MAGIC   )
# MAGIC 
# MAGIC   val arrayStructureSchema = new StructType()
# MAGIC     .add("name",new StructType()
# MAGIC       .add("firstname",StringType)
# MAGIC       .add("middlename",StringType)
# MAGIC       .add("lastname",StringType))
# MAGIC     .add("languages", ArrayType(StringType))
# MAGIC     .add("state", StringType)
# MAGIC     .add("gender", StringType)
# MAGIC 
# MAGIC   val df = spark.createDataFrame(
# MAGIC     spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
# MAGIC   df.printSchema()
# MAGIC   df.show()

# COMMAND ----------

rdd=spark.parallelize([1,2,3,4,5])

# COMMAND ----------

# MAGIC %md
# MAGIC ###filter

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"/{processed_folder_path}/circuits")

# COMMAND ----------

races_df=spark.read.parquet(f"/{processed_folder_path}/races").filter("race_year=2019")

# COMMAND ----------

joined_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner")

# COMMAND ----------

display(joined_df)

# COMMAND ----------

