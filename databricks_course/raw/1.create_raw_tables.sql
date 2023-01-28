-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitid INT,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt INT,
url string
)
using csv
options (path "/mnt/raw/circuits.csv" ,header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceid INT,
year INT,
round INT,
circuitid INT,
name string,
date string,
time string,
url string
)
using csv
options (path "/mnt/raw/races.csv" ,header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##constructors

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId INT,
constructorRef string,
name string,
nationality string,
url string
)
using json
options (path "/mnt/raw/constructors.json" ,header true)

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## drivers

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId INT,
driverRef string,
number INT,
name struct<forename string,surname string>,
dob date,
nationality string,
url string
)
using json
options (path "/mnt/raw/drivers.json" ,header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##results

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId INT
,raceId INT
,driverId INT
,constructorId INT
,number INT
,grid INT
,position INT
,positionText string
,positionOrder INT
,points double
,laps INT
,time string
,milliseconds INT
,fastestLap INT
,rank INT
,fastestLapTime string
,fastestLapSpeed string
,statusId INT
)
using json
options (path "/mnt/raw/results.json" ,header true)

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##pitstops

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId INT
,driverId INT
,stop string
,lap INT
,time string
,duration string
,milliseconds INT
)
using json
options (path "/mnt/raw/pit_stops.json" ,header true,multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## lap times

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId INT
,driverId INT
,lap INT
,position INT
,time string
,milliseconds INT
)
using csv
options (path "/mnt/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## qualifying

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId INT
,raceId INT
,driverId INT
,constructorId INT
,number INT
,position INT
,q1 string
,q2 string
,q3 string
)
using json
options (path "/mnt/raw/qualifying",multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

