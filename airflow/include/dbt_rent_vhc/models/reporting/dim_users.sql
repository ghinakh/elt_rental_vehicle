{{ config(
    materialized='table', 
    schema="reporting_dataset" 
)
}}

WITH users as (
    SELECT
        user_id,
        first_name,
        last_name,
        CONCAT(first_name,' ',last_name) AS full_name,
        email,
        phone_number,
        driver_license_number,
        driver_license_expiry,
        creation_date,
        date_diff(current_date, creation_date, day) as age_account,
        is_active
    FROM {{ ref('snapshot_users') }}
    WHERE dbt_valid_to IS NULL 
    -- Ambil versi data terbaru untuk setiap user:
    -- (1) Jika user belum pernah berubah -> barisnya tetap dan dbt_valid_to NULL
    -- (2) Jika user pernah berubah -> versi terbarunya punya dbt_valid_to NULL, yang lama ada isinya 
)

SELECT *,
    CASE 
        WHEN age_account <= 30 THEN 'New'
        WHEN age_account <= 180 THEN 'Medium'
        ELSE 'Old'
    END AS age_account_segment
FROM users