SELECT
  latitude,
  longitude,
  valid_time,
  ((EXP((17.625 * (d2m - 273.15)) / ((d2m - 273.15) + 243.04)) / EXP((17.625 * (t2m - 273.15)) / ((t2m - 273.15) + 243.04))) * 100) AS relative_humidity
FROM
  `strong-ward-437213-j6.bigdata_20241.other_data_2024_main`
ORDER BY
  valid_time, latitude, longitude
