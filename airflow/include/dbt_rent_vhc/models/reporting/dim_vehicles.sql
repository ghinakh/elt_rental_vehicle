{{ config(
    materialized='table', 
    schema="reporting_dataset" 
)
}}

WITH vehicles AS (
    SELECT
        vehicle_id, active, vehicle_license_number, registration_name, license_type, expiration_date, permit_license_number, 
        certification_date, vehicle_year, base_telephone_number, base_address, last_update_timestamp, brand, vehicle_type,
        DATE_DIFF(current_date, certification_date, YEAR) AS age_vehicle
    FROM {{ ref('snapshot_vehicles') }}
    WHERE dbt_valid_to IS NULL 
    -- Ambil versi data terbaru untuk setiap vehicle:
    -- (1) Jika vehicle belum pernah berubah -> barisnya tetap dan dbt_valid_to NULL
    -- (2) Jika vehicle pernah berubah -> versi terbarunya punya dbt_valid_to NULL, yang lama ada isinya 
)

SELECT *,
    CASE 
        WHEN age_vehicle <=2 THEN 'New'
        WHEN age_vehicle <=5 THEN 'Medium'
        ELSE 'Old'
    END AS age_vehicle_segment
FROM vehicles

