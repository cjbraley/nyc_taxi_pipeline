{{ config(materialized='table') }}

WITH yellow_trips AS (
    SELECT 
        *,
        'Yellow' as service_type 
    FROM {{ ref('staging_yellow_trips') }}
),
green_trips AS (
    SELECT 
        *,
        'Green' as service_type 
    FROM {{ ref('staging_green_trips') }}
),
trips_union AS (
    SELECT * FROM yellow_trips
    UNION ALL
    SELECT * FROM green_trips
),
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
)
SELECT
    t.id,
    t.year,
    t.month,
    t.vendorid,
    t.service_type,
    t.pickup_locationid, 
    t.dropoff_locationid,


    pz.borough AS pickup_borough,
    pz.zone AS pickup_zone,
    dz.borough AS dropoff_borough,
    dz.zone AS dropoff_zone,

    t.pickup_datetime, 
    t.dropoff_datetime, 
    t.rate_code,
    t.store_and_fwd_flag, 
    t.passenger_count, 
    t.trip_distance, 
    t.trip_type, 
    t.fare_amount, 
    t.extra, 
    t.mta_tax, 
    t.tip_amount, 
    t.tolls_amount, 
    t.ehail_fee, 
    t.improvement_surcharge, 
    t.total_amount, 
    t.payment_type, 
    t.congestion_surcharge
FROM trips_union t
INNER JOIN dim_zones pz ON t.pickup_locationid = pz.id
INNER JOIN dim_zones dz ON t.dropoff_locationid = dz.id
WHERE 1=1
-- Clean false outlier records
AND t.dropoff_datetime > t.pickup_datetime -- dropoff needs to be after pickup
AND t.pickup_datetime >= t.start_of_month -- pickup can't be before som
AND t.pickup_datetime <= t.end_of_month -- pickup can't be after eom
AND t.dropoff_datetime >= t.start_of_month -- dropoff can't be before som
AND t.dropoff_datetime <= t.end_of_month -- dropoff can't be after eom