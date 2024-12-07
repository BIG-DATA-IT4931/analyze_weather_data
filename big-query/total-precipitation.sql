SELECT
  DATE(valid_time) AS date,
  SUM(tp) AS total_precipitation
FROM
  `strong-ward-437213-j6.bigdata_20241.rain_data_2024_main`
GROUP BY
  date
ORDER BY
  date;
