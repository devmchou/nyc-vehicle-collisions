/* @bruin

name: reports.reasons_highsev_collisions_byyear
connection: gcp-default
type: bq.sql
description: Reasons for high-severity collisions

materialization:
  type: table
  strategy: create+replace

depends:
  - marts.fct_collisions
  - marts.fct_vehicle_involvement
  - marts.dim_date
  - marts.dim_contributing_factor


columns:
  - name: crash_year
    type: SMALLINT
  - name: contributing_factor_vehicle_1
    type: STRING

  - name: num_collisions
    type: INT64
    checks:
      - name: not_null

  
@bruin */


WITH collisions_severity AS 
(
  SELECT 
  collision_id, crash_year, severity_score,
  NTILE(4) OVER (PARTITION BY crash_year ORDER BY severity_score DESC) AS quartile
  FROM
  (
  SELECT collision_id, d.year AS crash_year,
  number_of_persons_killed * 10 + number_of_persons_injured AS severity_score,
  from marts.fct_collisions c
  INNER JOIN marts.dim_date d ON c.crash_date_id = d.crash_date_id
  WHERE number_of_persons_injured + number_of_persons_killed > 0
  )
)
SELECT crash_year, contributing_factor, COUNT(*) AS num_collisions
FROM collisions_severity
INNER JOIN marts.fct_vehicle_involvement vi ON collisions_severity.collision_id = vi.collision_id
INNER JOIN marts.dim_contributing_factor cf ON vi.contributing_factor_id = cf.contributing_factor_id
WHERE quartile = 1 AND contributing_factor IS NOT NULL AND contributing_factor != 'Unspecified'
GROUP BY crash_year, contributing_factor
ORDER BY num_collisions DESC