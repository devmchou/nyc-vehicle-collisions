/* @bruin

name: reports.crash_trend_detail
type: bq.sql
description: Crash trend data

materialization:
  type: table
  strategy: create+replace

depends:
  - marts.fct_collisions
  - marts.dim_date

columns:
  - name: year
    type: SMALLINT
  - name: month
    type: SMALLINT

  - name: total_crashes
    type: INT64
  
  - name: total_injuries
    type: INT64  
  - name: total_deaths
    type: INT64

  - name: total_motorist_injuries
    type: INT64  
  - name: total_motorist_deaths
    type: INT64

  - name: total_pedestrian_injuries
    type: INT64  
  - name: total_pedestrian_deaths
    type: INT64

  - name: total_cyclist_injuries
    type: INT64  
  - name: total_cyclist_deaths
    type: INT64
    
    
@bruin */

SELECT
    d.year,
    d.month,
    COUNT(*) AS total_crashes,
    SUM(f.number_of_persons_injured) AS total_injuries,
    SUM(f.number_of_persons_killed) AS total_deaths,
    SUM(f.number_of_motorist_injured) AS total_motorist_injuries,
    SUM(f.number_of_motorist_killed) AS total_motorist_deaths,
    SUM(f.number_of_pedestrians_injured) AS total_pedestrian_injuries,
    SUM(f.number_of_pedestrians_killed) AS total_pedestrian_deaths,
    SUM(f.number_of_cyclist_injured) AS total_cyclist_injuries,
    SUM(f.number_of_cyclist_killed) AS total_cyclist_deaths
FROM marts.fct_collisions f
JOIN marts.dim_date d ON f.crash_date_id = d.crash_date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;