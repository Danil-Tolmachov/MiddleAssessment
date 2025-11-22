WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
               ORDER BY (SELECT 0)
           ) AS rn
    FROM ride_records
)
DELETE FROM CTE
WHERE rn > 1;