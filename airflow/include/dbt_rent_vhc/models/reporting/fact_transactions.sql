{{ config(
    materialized='incremental',
    unique_key='rental_id',
    incremental_strategy='merge'
) }}


WITH valid_transactions AS (
    SELECT *
    FROM {{ ref('staging_dataset.clean_transactions') }} t
    INNER JOIN {{ ref('reporting_dataset.dim_users') }} u ON t.user_id = u.user_id
    INNER JOIN {{ ref('reporting_dataset.dim_vehicles') }} v ON t.vehicle_id = v.vehicle_id
    INNER JOIN {{ ref('reporting_dataset.dim_locations') }} lp ON t.pickup_location = lp.location_id
    INNER JOIN {{ ref('reporting_dataset.dim_locations') }} ld ON t.dropoff_location = ld.location_id

    {% if is_incremental() %}
      -- Kalau incremental, hanya ambil data baru
      WHERE t.rental_end_time > (SELECT MAX(rental_end_time) FROM {{ this }})
    {% endif %}
)


SELECT
    rental_id,
    user_id,
    vehicle_id,
    rental_start_time,
    rental_end_time,
    DATE_DIFF(rental_end_time, rental_start_time, DAY) AS rental_duration_days,
    pickup_location,
    dropoff_location,
    total_amount
FROM valid_transactions

