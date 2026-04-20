/* @bruin

name: reports.crash_time_of_day
type: bq.sql
description: Crash time of day data

materialization:
  type: table
  strategy: create+replace

depends:
  - marts.fct_collisions
  - marts.dim_time

columns:
  - name: hour
    type: SMALLINT
  - name: crash_count
    type: int64
@bruin */

SELECT
    t.crash_hour,
    COUNT(*) AS crash_count
FROM marts.fct_collisions f
JOIN marts.dim_time t ON f.crash_time_id = t.crash_time_id
GROUP BY t.crash_hour
ORDER BY crash_count DESC;