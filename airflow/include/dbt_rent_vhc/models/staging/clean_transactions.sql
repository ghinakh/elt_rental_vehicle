{{ config(
    materialized='incremental',
    schema='staging_dataset',
    unique_key='rental_id',
    strategy="merge",
) }}

WITH source AS (
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

    {% if is_incremental() %}
      -- Ambil data yang rental_end_time lebih baru dari yang terakhir ada di tabel incremental ini
      AND rental_end_time > (SELECT MAX(rental_end_time) FROM {{ this }})
    {% endif %}
),

deduplicate AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY rental_id ORDER BY rental_end_time DESC) AS row_num
    FROM source
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