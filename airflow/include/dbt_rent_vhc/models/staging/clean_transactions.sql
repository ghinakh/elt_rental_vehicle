{{ config(
    materialized='incremental',
    schema='staging_dataset',
    unique_key='rental_id',
    strategy='merge',
) }}

WITH raw AS (
    SELECT
        rental_id,
        user_id,
        vehicle_id,
        DATETIME(TIMESTAMP_MICROS(CAST(rental_start_time / 1000 AS INT64))) AS rental_start_time,
        DATETIME(TIMESTAMP_MICROS(CAST(rental_end_time / 1000 AS INT64))) AS rental_end_time,
        pickup_location,
        dropoff_location,
        total_amount
    FROM `staging_dataset.raw_transactions`
    WHERE rental_id IS NOT NULL
),

filtered AS (
    SELECT *
    FROM raw
    {% if is_incremental() %}
    WHERE rental_end_time > (SELECT MAX(rental_end_time) FROM {{ this }})
    {% endif %}
),

deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY rental_id ORDER BY rental_end_time DESC) AS row_num
    FROM filtered
)

SELECT
    rental_id,
    user_id,
    vehicle_id,
    rental_start_time,
    rental_end_time,
    pickup_location,
    dropoff_location,
    total_amount
FROM deduplicate
WHERE row_num = 1