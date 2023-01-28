-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###visualization of dominant drivers

-- COMMAND ----------

select driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points,
rank() over(order by avg(f_points) desc) rank1
from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

select race_year,driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points
from f1_presentation.calculated_race_results
group by race_year,driver_name
--having count(1)>=50
order by race_year,avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##cte

-- COMMAND ----------

with cte as
(select driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points,
rank() over(order by avg(f_points) desc) driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by avg_points desc
)
select race_year,driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from cte where driver_rank <=10)
group by race_year,driver_name
--having count(1)>=50
order by race_year,avg_points desc

-- COMMAND ----------

with cte as
(select driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points,
rank() over(order by avg(f_points) desc) driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by avg_points desc
)
select race_year,driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from cte where driver_rank <=10)
group by race_year,driver_name
--having count(1)>=50
order by race_year,avg_points desc

-- COMMAND ----------

with cte as
(select driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points,
rank() over(order by avg(f_points) desc) driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by avg_points desc
)
select race_year,driver_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from cte where driver_rank <=10)
group by race_year,driver_name
--having count(1)>=50
order by race_year,avg_points desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##dominant teams

-- COMMAND ----------

with cte as
(select team_name,count(1) as total_races,sum(f_points) as total_points,avg(f_points) as avg_points,
rank() over(order by avg(f_points) desc) team_rank
from f1_presentation.calculated_race_results
group by team_name
having count(1)>=100
order by avg_points desc
)
select race_year,team_name,count(1)as total_races,sum(f_points) as total_points,
  avg(f_points) as avg_points
  from f1_presentation.calculated_race_results
  where team_name in (select team_name from cte where team_rank<=5)
  group by race_year,team_name
  order by race_year,avg_points desc

-- COMMAND ----------

