{{ config(
    materialized='table',
    schema='reporting_dataset'
) }}

WITH group_by_vehicle AS (
    SELECT
        vehicle_id,
        ANY_VALUE(vehicle_license_number) AS vehicle_license_number,
        ANY_VALUE(active) AS active,
        ANY_VALUE(vehicle_brand) AS vehicle_brand,
        ANY_VALUE(vehicle_type) AS vehicle_type,
        ANY_VALUE(age_vehicle) AS age_vehicle,
        ANY_VALUE(age_vehicle_segment) AS age_vehicle_segment,
        COUNT(*) AS total_transactions,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS average_revenue,
        MAX(total_amount) AS max_revenue,
        MIN(total_amount) AS min_revenue,
        MIN(rental_start_time) AS first_rented,
        MAX(rental_end_time) AS last_rented,
        SUM(rental_duration_days) AS total_days_rented,
        AVG(rental_duration_days) AS avg_days_rented
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY vehicle_id
),
-- Pickup City Usage
pickup_city_usage_base AS (
    SELECT vehicle_id, pickup_city, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY vehicle_id, pickup_city
),
pickup_city_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY freq DESC) AS rn
    FROM pickup_city_usage_base
),
-- Dropoff City Usage
dropoff_city_usage_base AS (
    SELECT vehicle_id, dropoff_city, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY vehicle_id, dropoff_city
),
dropoff_city_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY freq DESC) AS rn
    FROM dropoff_city_usage_base
),
-- Users Usage
users_usage_base AS (
    SELECT
        vehicle_id,
        user_id,
        ANY_VALUE(full_name) AS full_name,
        COUNT(*) AS freq,
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY vehicle_id, user_id
),
users_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY freq DESC) AS rn
),
most_frequent_data AS (
    SELECT
        v.vehicle_id,
        p.pickup_city AS most_frequent_pickup_city,
        d.dropoff_city AS most_frequent_dropoff_city,
        u.user_id AS most_frequent_user_id,
        u.full_name AS most_frequent_user_name
    FROM (SELECT DISTINCT vehicle_id FROM {{ ref('transform_dataset.denormalized_transactions') }}) v
    LEFT JOIN pickup_city_usage p ON v.vehicle_id = p.vehicle_id AND p.rn = 1
    LEFT JOIN dropoff_city_usage d ON v.vehicle_id = d.vehicle_id AND d.rn = 1
    LEFT JOIN users_usage u ON v.vehicle_id = u.vehicle_id AND u.rn = 1
)

SELECT
    gbv.*,
    most.most_frequent_pickup_city,
    most.most_frequent_dropoff_city,
    most.most_frequent_user_id,
    most.most_frequent_user_name
FROM group_by_vehicle gbv
LEFT JOIN most_frequent_data most ON gbv.vehicle_id = most.vehicle_id