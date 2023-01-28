-- Databricks notebook source
create database if not exists f1_demo
location "/mnt/demo"

-- COMMAND ----------

use f1_demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df=spark.read\
-- MAGIC .option('inferSchema',True)\
-- MAGIC .json('/mnt/raw/2021-03-28/results.json')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##uspsert using merge

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1=spark.read.option('inferSchema',True)\
-- MAGIC .json('/mnt/raw/2021-03-28/drivers.json')\
-- MAGIC .filter('driverId<=10')\
-- MAGIC .select("driverId","dob","name.forename","name.surname")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1.createOrReplaceTempView("drivers_day_1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC 
-- MAGIC drivers_day2=spark.read.option('inferSchema',True)\
-- MAGIC .json('/mnt/raw/2021-03-28/drivers.json')\
-- MAGIC .filter('driverId between 6 and 15')\
-- MAGIC .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day2.createOrReplaceTempView("drivers_day_2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(drivers_day2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC 
-- MAGIC drivers_day3=spark.read.option('inferSchema',True)\
-- MAGIC .json('/mnt/raw/2021-03-28/drivers.json')\
-- MAGIC .filter('(driverId between 1 and 5 )or( driverId between 16 and 20)')\
-- MAGIC .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
-- MAGIC display(drivers_day3)

-- COMMAND ----------

create Table if not exists f1_demo.drivers_merge(
driverId INT,
dob DATE,
forename STRING,
surname STRING,
createddate DATE,
updateddate Date
)

-- COMMAND ----------

MERGE INTO f1_demo.drivers_merge tgt
USING drivers_day_1 upd
ON tgt.driverId = upd.driverId
WHEN MATCHED THEN
  UPDATE SET
    tgt.dob = upd.dob,
    tgt.forename=upd.forename,
    tgt.surname=upd.surname,
    tgt.updateddate=current_timestamp
WHEN NOT MATCHED
  THEN INSERT 
    (driverId,dob,forename,surname,createddate)
  VALUES (
    driverId,dob,forename,surname,current_timestamp )

-- COMMAND ----------

MERGE INTO f1_demo.drivers_merge tgt
USING drivers_day_2 upd
ON tgt.driverId = upd.driverId
WHEN MATCHED THEN
  UPDATE SET
    tgt.dob = upd.dob,
    tgt.forename=upd.forename,
    tgt.surname=upd.surname,
    tgt.updateddate=current_timestamp
WHEN NOT MATCHED
  THEN INSERT 
    (driverId,dob,forename,surname,createddate)
  VALUES (
    driverId,dob,forename,surname,current_timestamp )

-- COMMAND ----------

select * from f1_demo.drivers_merge

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp
-- MAGIC from delta.tables import *
-- MAGIC 
-- MAGIC deltaTablePeople = DeltaTable.forPath(spark, '/mnt/demo/drivers_merge')
-- MAGIC 
-- MAGIC deltaTablePeople.alias('tgt') \
-- MAGIC   .merge(
-- MAGIC     drivers_day3.alias('upd'),
-- MAGIC     'tgt.driverId = upd.driverId'
-- MAGIC   ) \
-- MAGIC   .whenMatchedUpdate(set =
-- MAGIC     {
-- MAGIC       "dob": "upd.dob",
-- MAGIC       "forename": "upd.forename",
-- MAGIC       "surname": "upd.surname",
-- MAGIC       "updateddate": "current_timestamp()"
-- MAGIC     }
-- MAGIC   ) \
-- MAGIC   .whenNotMatchedInsert(values =
-- MAGIC     {
-- MAGIC       "dob": "upd.dob",
-- MAGIC       "forename": "upd.forename",
-- MAGIC       "surname": "upd.surname",
-- MAGIC       "createddate":"current_timestamp()"
-- MAGIC     }
-- MAGIC   ) \
-- MAGIC   .execute()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format("delta").load("/mnt/demo/drivers_merge")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##history

-- COMMAND ----------

desc history f1_demo.drivers_merge

-- COMMAND ----------

select * from f1_demo.drivers_merge version as of 3

-- COMMAND ----------

select * from f1_demo.drivers_merge timestamp as of '2023-01-12T06:13:35.000+0000'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format('delta').option('timestampAsOf','2023-01-12T06:13:35.000+0000').load('/mnt/demo/drivers_merge')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format('delta').option('versionAsOf','2').load('/mnt/demo/drivers_merge')

-- COMMAND ----------

-- MAGIC  %python
-- MAGIC display(df)

-- COMMAND ----------

