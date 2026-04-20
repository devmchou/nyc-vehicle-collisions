/* @bruin

name: reports.crash_geo_hotspots
connection: gcp-default
type: bq.sql
description: Geographic hotspots for collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - marts.fct_collisions
  - marts.dim_location


columns:
  - name: lat_bin
    type: FLOAT
  - name: lon_bin
    type: FLOAT
  
  - name: location
    type: STRING
  - name: geog_point
    type: GEOGRAPHY

  - name: crash_count
    type: INT64
    checks:
      - name: not_null
  
@bruin */

select
    lat_bin, lon_bin, 
    CONCAT(lat_bin, ', ', lon_bin) AS location,
    ST_GEOGPOINT(lon_bin, lat_bin) as geog_point,
    crash_count
FROM 
(
SELECT
    ROUND(l.latitude, 3) AS lat_bin,
    ROUND(l.longitude, 3) AS lon_bin,
    COUNT(*) AS crash_count
FROM marts.fct_collisions f
INNER JOIN marts.dim_location l ON f.location_id = l.location_id
GROUP BY lat_bin, lon_bin
ORDER BY crash_count DESC
LIMIT 50
)
WHERE lat_bin != 0 AND lon_bin != 0