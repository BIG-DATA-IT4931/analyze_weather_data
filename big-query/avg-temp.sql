SELECT
  EXTRACT(YEAR FROM DATE(valid_time)) AS year,
  EXTRACT(MONTH FROM DATE(valid_time)) AS month,
  AVG(t2m) AS avg_temperature
FROM
  `strong-ward-437213-j6.bigdata_20241.other_data_2024_main`
GROUP BY
  year, month
ORDER BY
  year, month
