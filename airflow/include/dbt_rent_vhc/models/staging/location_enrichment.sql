{{ config(
    materialized='table',
    schema='seed_dataset'
) }}

SELECT
    CAST(location_id AS STRING) AS location_id,
    location_name,
    address,
    city,
    state,
    CAST(zip_code AS STRING) AS zip_code,
    latitude,
    longitude
FROM {{ ref('raw_location_enrichment') }}
