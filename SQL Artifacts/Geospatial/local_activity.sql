/*
Query: Identify user activity in a specific location and time
Requirements: Dataset must contain timestamp, latitude, longitude

Input = Single GPS coordinate (Params CTE: target_lat, target_lon)
Output = All rows whose lat/lng fall within a 100ft radius of the input

Optional Search Criteria:
- Event time between x and y
- Vehicle make, model, color, license plate, partial license plate, description of driver, etc.

Use Case:
User says they entered a Lyft/Uber who was working 'off the clock' for cash or offering free service. User alleges
that the driver partner assaulted them. No trip information is available, preventing the initial investigators from
identifying a person of interest (POI) to speak with. 

This query allows investigators to identify all user activity in a particular time and location. In most scenarios,
the accused party will be included in the output if they truly were a driver partner and operating in the area. The 
search criteria can be optimized depending on the use case to narrow down a list of POIs.

*/

WITH params AS (
  SELECT
    37.7749 AS target_lat,     -- your latitude
    -122.4194 AS target_lon,   -- your longitude
    100.0 / 5280.0 AS distance_miles,  -- 100 ft in miles
    FLOOR(37.7749 / 0.009) AS target_lat_grid,
    FLOOR(-122.4194 / 0.009) AS target_lon_grid
)
SELECT
  u.user_id,
  u.latitude,
  u.longitude,
  u.event_time,
  (3959 * acos(
      cos(radians(p.target_lat)) * cos(radians(u.latitude)) *
      cos(radians(u.longitude) - radians(p.target_lon)) +
      sin(radians(p.target_lat)) * sin(radians(u.latitude))
  )) AS distance_miles
FROM
  user_location_data u
CROSS JOIN params p
WHERE
  -- Time window filter first
  u.event_time BETWEEN '2025-11-07 10:00:00' AND '2025-11-07 11:00:00'

  -- Prefilter: nearby 1 km grid cells (3×3 window covers ~3 km area)
  AND u.lat_grid BETWEEN p.target_lat_grid - 1 AND p.target_lat_grid + 1
  AND u.lon_grid BETWEEN p.target_lon_grid - 1 AND p.target_lon_grid + 1

  -- Final precise distance filter: within 100 feet
  AND (3959 * acos(
      cos(radians(p.target_lat)) * cos(radians(u.latitude)) *
      cos(radians(u.longitude) - radians(p.target_lon)) +
      sin(radians(p.target_lat)) * sin(radians(u.latitude))
  )) <= p.distance_miles;
