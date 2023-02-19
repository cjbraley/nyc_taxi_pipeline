{{ config(materialized='table') }}

SELECT
    id,
    borough,
    zone,
    replace("service_zone","Boro","Green") as service_zone -- before this change we have yellow zones and boro zones
FROM {{ ref('staging_zones') }}
WHERE borough != 'Unknown'