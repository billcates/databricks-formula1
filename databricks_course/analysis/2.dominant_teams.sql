-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##dominant teams

-- COMMAND ----------

select * from f1_presentation.calculated_race_results


-- COMMAND ----------

select team_name,sum(f_points) as total_points,
  avg(f_points) as avg_points
  from f1_presentation.calculated_race_results
  group by team_name
  order by avg_points desc

-- COMMAND ----------

select team_name,count(1)as total_races,sum(f_points) as total_points,
  avg(f_points) as avg_points
  from f1_presentation.calculated_race_results
  group by team_name
  having count(1)>=50
  order by avg_points desc

-- COMMAND ----------

select team_name,count(1)as total_races,sum(f_points) as total_points,
  avg(f_points) as avg_points
  from f1_presentation.calculated_race_results
  where race_year between 1990 and 2000
  group by team_name
  having count(1)>=50
  order by avg_points desc

-- COMMAND ----------

