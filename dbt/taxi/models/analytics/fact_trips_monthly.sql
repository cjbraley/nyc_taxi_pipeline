

{{ config(materialized='table') }}


WITH trips_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
)

SELECT
    -- Reveneue grouping 
    pickup_zone AS revenue_zone,
    year,
    month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(ehail_fee) AS revenue_monthly_ehail_fee,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    COUNT(id) AS total_monthly_trips,
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM trips_data
GROUP BY
    year,
    month,
    revenue_zone,
    service_type