SELECT
  latitude,
  longitude,
  valid_time,
  SQRT(POWER(u10, 2) + POWER(v10, 2)) AS wind_speed,
  ((ATAN2(u10, v10) * (180 / 3.141592653589793) + 180) - 360 * FLOOR((ATAN2(u10, v10) * (180 / 3.141592653589793) + 180) / 360)) AS wind_direction,
FROM
  `strong-ward-437213-j6.bigdata_20241.other_data_2024_main`
ORDER BY
  valid_time, latitude, longitude