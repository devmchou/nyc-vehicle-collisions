/* @bruin

name: marts.dim_date
connection: gcp-default
type: bq.sql
description: Date dimension table

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions

columns:
  - name: crash_date_id
    type: INT64
    description: Unique identifier for each crash date
    checks:
      - name: not_null
      - name: unique

  - name: crash_date
    type: DATE
    description: Date of the collision.
    checks:
      - name: not_null
      - name: unique

  - name: day_of_week
    type: SMALLINT

  - name: month
    type: SMALLINT

  - name: year
    type: SMALLINT

  - name: is_weekend
    type: BOOLEAN

  - name: holiday_flag
    type: BOOLEAN
 
@bruin */

select ROW_NUMBER() OVER() AS crash_date_id,
*
FROM (
  SELECT DISTINCT crash_date,
  EXTRACT(DAYOFWEEK FROM crash_date) AS day_of_week,
  EXTRACT(MONTH FROM crash_date) AS month,
  EXTRACT(YEAR FROM crash_date) AS year,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM crash_date) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend,
  CASE
    WHEN crash_date IN (SELECT observed_date FROM `seeds.city_holidays`) THEN TRUE
    ELSE FALSE
  END AS holiday_flag
  FROM staging.stg_collisions
  ORDER BY crash_date ASC
)
