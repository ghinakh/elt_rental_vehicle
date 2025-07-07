{{ config(
    materialized='view',
    cluster_by=["user_id", "vehicle_id"],
    schema="transform_dataset"
)
}}

SELECT
    t.rental_id,
    t.user_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.email,
    u.phone_number,
    u.driver_license_number,
    u.driver_license_expiry,
    u.creation_date,
    u.age_account,
    u.age_account_segment,
    u.is_active,
    u.update_at,
    t.vehicle_id,
    v.active,
    v.vehicle_license_number,
    v.registration_name,
    v.license_type,
    v.expiration_date,
    v.permit_license_number,
    v.certification_date,
    v.vehicle_year,
    v.base_telephone_number,
    v.base_address,
    v.last_update_timestamp AS vehicle_last_update_timestamp,
    v.brand AS vehicle_brand,
    v.vehicle_type,
    v.age_vehicle,
    v.age_vehicle_segment,
    t.rental_start_time,
    t.rental_end_time,
    t.rental_duration_days,
    EXTRACT(YEAR FROM t.rental_end_time) AS rental_year,
    EXTRACT(MONTH FROM t.rental_end_time) AS rental_month,
    EXTRACT(DAY FROM t.rental_end_time) AS rental_day,
    EXTRACT(DAYOFWEEK FROM t.rental_end_time) AS rental_dayofweek,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM t.rental_end_time) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    t.pickup_location AS pickup_location_id,
    lp.location_name AS pickup_location_name,
    lp.address AS pickup_address,
    lp.city AS pickup_city,
    lp.state AS pickup_state,
    lp.zip_code AS pickup_zip_code,
    lp.latitude AS pickup_latitude,
    lp.longitude AS pickup_longitude,
    t.dropoff_location AS dropoff_location_id,
    ld.location_name AS dropoff_location_name,
    ld.address AS dropoff_address,
    ld.city AS dropoff_city,
    ld.state AS dropoff_state,
    ld.zip_code AS dropoff_zip_code,
    ld.latitude AS dropoff_latitude,
    ld.longitude AS dropoff_longitude,
    t.total_amount
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_users') }} u ON t.user_id = u.user_id
INNER JOIN {{ ref('dim_vehicles') }} v ON t.vehicle_id = v.vehicle_id
INNER JOIN {{ ref('dim_locations') }} lp ON t.pickup_location = lp.location_id
INNER JOIN {{ ref('dim_locations') }} ld ON t.dropoff_location = ld.location_id
