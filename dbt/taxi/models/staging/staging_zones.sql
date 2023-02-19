{{ config(materialized=("view" if target.name == 'dev' else "ephemeral")) }}


SELECT
    id,
    borough,
    zone,
    service_zone
FROM {{ source("trips_data","zones_external") }}
{% if target.name == 'dev' %}
    LIMIT 10
{% endif %}