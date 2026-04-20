/* @bruin

name: staging.stg_collisions
connection: gcp-default
type: bq.sql
description: Materializes the raw collisions data

materialization:
  type: table
  strategy: create+replace
  partition_by: crash_date
  cluster_by:
    - zip_code

depends:
  - ingestion.collisions_raw

columns:
  - name: collision_id
    type: INT64
    checks:
      - name: not_null

  - name: crash_date
    type: DATE
    description: Date of the collision.
    checks:
      - name: not_null

  - name: crash_time
    type: TIME

  - name: borough
    type: STRING

  - name: zip_code
    type: SMALLINT
    checks:
      - name: not_null

  - name: latitude
    type: FLOAT
    checks:
      - name: not_null

  - name: longitude
    type: FLOAT
    checks:
      - name: not_null

  - name: on_street_name
    type: STRING
  - name: off_street_name
    type: STRING
  - name: cross_street_name
    type: STRING

  - name: geo_point
    type: GEOGRAPHY

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

  - name: contributing_factor_vehicle_1
    type: STRING
  - name: vehicle_type_code1
    type: STRING
  - name: contributing_factor_vehicle_2
    type: STRING
  - name: vehicle_type_code2
    type: STRING
  - name: contributing_factor_vehicle_3
    type: STRING
  - name: vehicle_type_code3
    type: STRING
  - name: contributing_factor_vehicle_4
    type: STRING
  - name: vehicle_type_code4
    type: STRING
  - name: contributing_factor_vehicle_5
    type: STRING
  - name: vehicle_type_code5
    type: STRING

custom_checks:
   - name: check_count
     query: "SELECT count(*) > 0 FROM staging.stg_collisions"
     value: 1
@bruin */


WITH raw AS (
  SELECT
    collision_id,
    DATE(crash_date) as crash_date, 
    PARSE_TIME('%H:%M', crash_time) as crash_time,
    borough,
    zip_code,
    latitude,
    longitude,    
    on_street_name AS on_street_name,
    off_street_name AS off_street_name,
    cross_street_name AS cross_street_name,
    ST_GEOGPOINT(longitude,latitude) AS geo_point,
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,
    contributing_factor_vehicle_1,
    vehicle_type_code1,
    contributing_factor_vehicle_2,
    vehicle_type_code2,
    contributing_factor_vehicle_3,
    vehicle_type_code_3 AS vehicle_type_code3,
    contributing_factor_vehicle_4,
    vehicle_type_code_4 AS vehicle_type_code4,
    contributing_factor_vehicle_5,
    vehicle_type_code_5 AS vehicle_type_code5
  FROM ingestion.collisions_raw
    WHERE collision_id IS NOT NULL
    AND crash_date IS NOT NULL
    AND crash_time IS NOT NULL
    AND zip_code IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
)
SELECT * from raw
