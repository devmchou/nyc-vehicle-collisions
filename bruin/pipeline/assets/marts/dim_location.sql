/* @bruin

name: marts.dim_location
connection: gcp-default
type: bq.sql
description: Location dimension table

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.stg_collisions
  - seeds.zipcode_borough

columns:
  - name: location_id
    type: STRING
    checks:
      - name: not_null
      - name: unique

  - name: borough
    type: STRING
    checks:
      - name: not_null

  - name: zip_code
    type: STRING
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

custom_checks:
  - name: no_duplicate_locations
    query: "SELECT count(*) FROM (SELECT latitude, longitude, on_street_name, off_street_name, cross_street_name, COUNT(*) FROM marts.dim_location GROUP BY latitude, longitude, on_street_name, off_street_name, cross_street_name HAVING COUNT(*) > 1) AS duplicates "
    value: 0  
@bruin */

SELECT GENERATE_UUID() AS location_id,
*,
ST_GEOGPOINT(longitude, latitude) AS geo_point
FROM (
  SELECT 
  DISTINCT
  z.borough,
  s.zip_code,
  s.latitude,
  s.longitude,
  s.on_street_name,
  s.off_street_name,
  s.cross_street_name,
  FROM staging.stg_collisions s
  INNER JOIN seeds.zipcode_borough z
    ON s.zip_code = z.zipcode
  WHERE s.latitude IS NOT NULL
    AND s.longitude IS NOT NULL
  ORDER BY z.borough, s.zip_code, s.latitude, s.longitude, s.on_street_name, s.off_street_name, s.cross_street_name
)