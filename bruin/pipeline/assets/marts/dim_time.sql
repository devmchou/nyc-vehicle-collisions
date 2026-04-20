/* @bruin

name: marts.dim_time
connection: gcp-default
type: bq.sql
description: Time dimension table for collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions

columns:
  - name: crash_time_id
    type: INT64
    description: Unique identifier for each crash time
    checks:
      - name: not_null
      - name: unique

  - name: crash_time
    type: TIME
    checks:
      - name: not_null
      - name: unique
    
  - name: crash_hour
    type: INT64
    description: Hour of the crash

  - name: rush_hour_flag
    type: BOOLEAN
    description: Flag indicating if the crash occurred during rush hour

  - name: time_of_day
    type: STRING
    description: Time of day (morning, evening, etc.)

@bruin */

SELECT ROW_NUMBER() OVER() AS crash_time_id,
*
FROM (
  SELECT DISTINCT crash_time,
  EXTRACT(HOUR FROM crash_time) AS crash_hour,
  CASE 
    WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 7 AND 9 THEN TRUE
    WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 16 AND 18 THEN TRUE
    ELSE FALSE
  END AS rush_hour_flag,
  CASE 
    WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 5 AND 11 THEN 'morning'
    WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 12 AND 17 THEN 'afternoon'
    WHEN EXTRACT(HOUR FROM crash_time) BETWEEN 18 AND 21 THEN 'evening'
    ELSE 'night'
  END AS time_of_day
  FROM staging.stg_collisions
  ORDER BY crash_time ASC
)