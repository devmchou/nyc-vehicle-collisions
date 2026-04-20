/* @bruin

name: marts.fct_vehicle_involvement
connection: gcp-default
type: bq.sql
description: Collision vehicle bridge table to link the fact table with the vehicle dimension table

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions
  - marts.dim_vehicle

columns:
  - name: collision_id
    type: INT64
    checks:
      - name: not_null

  - name: vehicle_type_code_id
    type: INT64
    description: Foreign key to the dim_vehicle table

  - name: contributing_factor_id
    type: INT64
    description: Foreign key to the dim_contributing_factor table
  @bruin */

  SELECT collision_id, c.vehicle_type_code_id, cf.contributing_factor_id
    FROM staging.stg_collisions stg
    LEFT JOIN marts.dim_vehicle c
      ON stg.vehicle_type_code1 = c.vehicle_type_code
    LEFT JOIN marts.dim_contributing_factor cf
      ON stg.contributing_factor_vehicle_1 = cf.contributing_factor
    WHERE stg.vehicle_type_code1 IS NOT NULL OR stg.contributing_factor_vehicle_1 IS NOT NULL
    UNION ALL
    SELECT collision_id, c.vehicle_type_code_id, cf.contributing_factor_id
        FROM staging.stg_collisions stg
        LEFT JOIN marts.dim_vehicle c
        ON stg.vehicle_type_code2 = c.vehicle_type_code
        LEFT JOIN marts.dim_contributing_factor cf
        ON stg.contributing_factor_vehicle_2 = cf.contributing_factor
        WHERE stg.vehicle_type_code2 IS NOT NULL OR stg.contributing_factor_vehicle_2 IS NOT NULL
    UNION ALL
    SELECT collision_id, c.vehicle_type_code_id, cf.contributing_factor_id
        FROM staging.stg_collisions stg
        LEFT JOIN marts.dim_vehicle c
        ON stg.vehicle_type_code3 = c.vehicle_type_code
        LEFT JOIN marts.dim_contributing_factor cf
        ON stg.contributing_factor_vehicle_3 = cf.contributing_factor
        WHERE stg.vehicle_type_code3 IS NOT NULL OR stg.contributing_factor_vehicle_3 IS NOT NULL
    UNION ALL
    SELECT collision_id, c.vehicle_type_code_id, cf.contributing_factor_id
        FROM staging.stg_collisions stg
        LEFT JOIN marts.dim_vehicle c
        ON stg.vehicle_type_code4 = c.vehicle_type_code  
        LEFT JOIN marts.dim_contributing_factor cf
        ON stg.contributing_factor_vehicle_4 = cf.contributing_factor
        WHERE stg.vehicle_type_code4 IS NOT NULL OR stg.contributing_factor_vehicle_4 IS NOT NULL
    UNION ALL
    SELECT collision_id, c.vehicle_type_code_id, cf.contributing_factor_id
        FROM staging.stg_collisions stg
        LEFT JOIN marts.dim_vehicle c
        ON stg.vehicle_type_code5 = c.vehicle_type_code
        LEFT JOIN marts.dim_contributing_factor cf
        ON stg.contributing_factor_vehicle_5 = cf.contributing_factor
        WHERE stg.vehicle_type_code5 IS NOT NULL OR stg.contributing_factor_vehicle_5 IS NOT NULL