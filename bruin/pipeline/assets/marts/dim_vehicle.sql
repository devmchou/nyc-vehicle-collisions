/* @bruin

name: marts.dim_vehicle
connection: gcp-default
type: bq.sql
description: Vehicle dimension table for collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions

columns:
  - name: vehicle_type_code_id
    type: INT64
    description: Unique identifier for each vehicle type code 
    checks:
      - name: unique
      - name: not_null

  - name: vehicle_type_code
    type: STRING
    checks:
      - name: not_null
      - name: unique
    
@bruin */

SELECT
  ROW_NUMBER() OVER (ORDER BY vehicle_type_code) AS vehicle_type_code_id,
  *
FROM (
SELECT DISTINCT vehicle_type_code, 
FROM (
  SELECT vehicle_type_code1 AS vehicle_type_code FROM staging.stg_collisions
  UNION ALL
  SELECT vehicle_type_code2 AS vehicle_type_code FROM staging.stg_collisions 
  UNION ALL
  SELECT vehicle_type_code3 AS vehicle_type_code FROM staging.stg_collisions
  UNION ALL
  SELECT vehicle_type_code4 AS vehicle_type_code FROM staging.stg_collisions
  UNION ALL
  SELECT vehicle_type_code5 AS vehicle_type_code FROM staging.stg_collisions
) WHERE vehicle_type_code IS NOT NULL
ORDER BY vehicle_type_code
)