/* @bruin

name: marts.dim_contributing_factor
connection: gcp-default
type: bq.sql
description: Contributing factor dimension table for collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions

columns:
  - name: contributing_factor_id
    type: INT64
    description: Unique identifier for each contributing factor
    checks:
      - name: not_null
      - name: unique

  - name: contributing_factor
    type: STRING
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT 
  ROW_NUMBER() OVER (ORDER BY contributing_factor) AS contributing_factor_id,
  contributing_factor FROM (
SELECT DISTINCT contributing_factor
FROM (
  SELECT contributing_factor_vehicle_1 AS contributing_factor FROM staging.stg_collisions
  UNION ALL
  SELECT contributing_factor_vehicle_2 AS contributing_factor FROM staging.stg_collisions
  UNION ALL
  SELECT contributing_factor_vehicle_3 AS contributing_factor FROM staging.stg_collisions
  UNION ALL
  SELECT contributing_factor_vehicle_4 AS contributing_factor FROM staging.stg_collisions
  UNION ALL
  SELECT contributing_factor_vehicle_5 AS contributing_factor FROM staging.stg_collisions
) WHERE contributing_factor IS NOT NULL
ORDER BY contributing_factor
);