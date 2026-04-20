/* @bruin

name: marts.fct_collisions
connection: gcp-default
type: bq.sql
description: Collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions
  - marts.dim_date
  - marts.dim_time
  - marts.dim_location
  - marts.dim_contributing_factor

columns:
  - name: collision_id
    type: INT64
    checks:
      - name: not_null
      - name: unique

  - name: number_of_persons_injured
    type: SMALLINT  
  - name: number_of_persons_killed
    type: SMALLINT  
  - name: number_of_pedestrians_injured
    type: SMALLINT  
  - name: number_of_pedestrians_killed
    type: SMALLINT  
  - name: number_of_cyclist_injured
    type: SMALLINT  
  - name: number_of_cyclist_killed
    type: SMALLINT  
  - name: number_of_motorist_injured
    type: SMALLINT  
  - name: number_of_motorist_killed
    type: SMALLINT

  - name: crash_date_id  
    type: INT64
    description: Foreign key to the dim_date table
    checks:
      - name: not_null
  - name: crash_time_id
    type: INT64
    description: Foreign key to the dim_time table
    checks:
      - name: not_null
  - name: location_id
    type: INT64
    description: Foreign key to the dim_location table
    checks:
      - name: not_null

custom_checks:
  - name: no_extra_rows
    query: "SELECT (SELECT COUNT(*) FROM marts.fct_collisions) - (SELECT COUNT(*) FROM staging.stg_collisions) <= 0"
    value: 1
  @bruin */

  WITH DedupedRows AS (
    SELECT * from
    (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY collision_id 
            ORDER BY (SELECT NULL)
        ) AS rn
    FROM staging.stg_collisions
    )
    WHERE rn = 1
  )
  SELECT r.collision_id,
        r.number_of_persons_injured,
        r.number_of_persons_killed,
        r.number_of_pedestrians_injured,
        r.number_of_pedestrians_killed,
        r.number_of_cyclist_injured,
        r.number_of_cyclist_killed,
        r.number_of_motorist_injured,
        r.number_of_motorist_killed,
        d.crash_date_id,
        t.crash_time_id,
        l1.location_id AS location_id
  from DedupedRows r
  INNER JOIN marts.dim_date d
    ON r.crash_date = d.crash_date
  INNER JOIN marts.dim_time t
    ON r.crash_time = t.crash_time
  INNER JOIN marts.dim_location l1
    ON  
    (
    IFNULL(r.latitude, 0) = IFNULL(l1.latitude, 0) 
    AND IFNULL(r.longitude, 0) = IFNULL(l1.longitude, 0)
    AND IFNULL(r.on_street_name, '') = IFNULL(l1.on_street_name, '')
    AND IFNULL(r.off_street_name, '') = IFNULL(l1.off_street_name, '')
    AND IFNULL(r.cross_street_name, '') = IFNULL(l1.cross_street_name, '')
    )
