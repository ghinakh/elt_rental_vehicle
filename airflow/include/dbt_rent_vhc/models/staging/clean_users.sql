{{ config(
    materialized='view',
    schema='staging_dataset'
) }}

WITH source AS (
    SELECT
        user_id,
        trim(lower(first_name)) AS first_name,
        trim(lower(last_name)) AS last_name,
        email,
        phone_number,
        driver_license_number,
        driver_license_expiry,
        creation_date,
        CAST(is_active AS BOOLEAN) AS is_active
    FROM `staging_dataset.raw_users`
    WHERE user_id IS NOT NULL
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY user_id ORDER BY creation_date DESC) AS row_num
    FROM source
)

SELECT
    user_id, first_name, last_name, email, phone_number, 
    driver_license_number, driver_license_expiry, creation_date, 
    is_active
FROM deduplicate
WHERE row_num = 1

