{{ config(materialized=("view" if target.name == 'dev' else "ephemeral")) }}

WITH trips AS (
    SELECT
        *,
        row_number() over(partition by vendorid, tpep_pickup_datetime) as trip_key_number -- used to make pk distinct
    FROM {{ source("trips_data","yellow_trips_external") }}
    WHERE vendorid IS NOT NULL
)
SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as id, -- create a surrogate key on vendor + pickup. This is our trip identifier
    vendorid,
    year,
    month,
    pulocationid AS pickup_locationid,
    dolocationid AS dropoff_locationid,
    
    -- timestamps
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,


    -- tripinfo
    rate_code,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    "Street-hail" as trip_type, -- yellow cabs are always street-hail

    -- payment info
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    cast(0 as FLOAT64) as ehail_fee, -- missing in yellow
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,

    -- other
    CAST(DATE(year, month, 1) AS TIMESTAMP) AS start_of_month,
    TIMESTAMP_ADD(CAST(DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH) AS TIMESTAMP), INTERVAL -1 SECOND) as end_of_month,

FROM trips
WHERE trip_key_number = 1 -- ensure uniqueness
{% if target.name == 'dev' %}
    LIMIT 10
{% endif %}