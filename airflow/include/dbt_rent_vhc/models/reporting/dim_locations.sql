{{ config(
    materialized='table', 
    schema="reporting_dataset" 
)
}}

SELECT
    location_id,
    location_name,
    address,
    city,
    state,
    zip_code,
    latitude,
    longitude
FROM {{ ref('location_enrichment') }}