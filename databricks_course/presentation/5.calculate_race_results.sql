-- Databricks notebook source
use f1_processed

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet 
as
select races.race_year,constructors.name as team_name,drivers.name as driver_name,results.position,results.points,
11-results.position as f_points
from results
join drivers on drivers.driver_id=results.driver_id
join constructors on constructors.constructor_id=results.constructor_id
join races on races.race_id=results.race_id
where results.position <=10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

