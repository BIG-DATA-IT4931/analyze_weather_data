WITH WindSpeed AS (
    SELECT 
        time,
        latitude, 
        longitude, 
        u10, 
        v10,
        SQRT(u10 * u10 + v10 * v10) AS wind_speed
    FROM 
        `strong-ward-437213-j6.bigdata_20241.other_data_2024_main`
),
ContinuousRegions AS (
    SELECT 
        time,
        latitude, 
        longitude, 
        wind_speed,
        LAG(latitude, 1) OVER (ORDER BY time, longitude, latitude) AS prev_latitude,
        LAG(longitude, 1) OVER (ORDER BY time, latitude, longitude) AS prev_longitude
    FROM 
        WindSpeed
    WHERE 
        wind_speed > 10
)
SELECT 
    time,
    latitude, 
    longitude, 
    wind_speed
FROM 
    ContinuousRegions
WHERE 
    longitude = prev_longitude + 0.25
    Or latitude = prev_latitude + 0.25
ORDER BY 
    time,
    latitude, 
    longitude;
