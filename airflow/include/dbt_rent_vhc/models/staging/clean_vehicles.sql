{{ config(
    materialized='view',
    schema='staging_dataset'
) }}

WITH source AS (
    SELECT
        vehicle_id, active, vehicle_license_number, registration_name, license_type, expiration_date, permit_license_number, 
        certification_date, vehicle_year, base_telephone_number, base_address, last_update_timestamp, brand, vehicle_type
    FROM {{ ref('casted_vehicles') }}
    WHERE vehicle_id IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY last_update_timestamp DESC) AS row_num
    FROM source
)

SELECT
    vehicle_id, active, vehicle_license_number, registration_name, license_type, expiration_date, permit_license_number, 
    certification_date, vehicle_year, base_telephone_number, base_address, last_update_timestamp, brand, vehicle_type
FROM deduplicate
WHERE row_num = 1