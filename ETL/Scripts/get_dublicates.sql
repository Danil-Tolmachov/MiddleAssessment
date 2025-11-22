SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COUNT(*) AS cnt
FROM ride_records
GROUP BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
HAVING COUNT(*) > 1
ORDER BY cnt DESC;