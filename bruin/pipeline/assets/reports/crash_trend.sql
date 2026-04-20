/* @bruin

name: reports.crash_trend
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
@bruin */

SELECT
    d.year,
    d.month,
    COUNT(*) AS total_crashes,
    SUM(f.number_of_persons_injured) AS total_injuries,
    SUM(f.number_of_persons_killed) AS total_deaths
FROM marts.fct_collisions f
INNER JOIN marts.dim_date d ON f.crash_date_id = d.crash_date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;