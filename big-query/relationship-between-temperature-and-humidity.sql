SELECT
  t2m - 273.15 AS temperature_celsius,
  ((EXP((17.625 * (d2m - 273.15)) / ((d2m - 273.15) + 243.04)) / EXP((17.625 * (t2m - 273.15)) / ((t2m - 273.15) + 243.04))) * 100) AS relative_humidity
FROM
  `strong-ward-437213-j6.bigdata_20241.other_data_2024_main`
WHERE
  t2m IS NOT NULL AND d2m IS NOT NULL
