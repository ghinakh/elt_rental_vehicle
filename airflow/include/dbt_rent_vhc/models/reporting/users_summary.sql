{{ config(
    materialized='table',
    schema='reporting_dataset'
) }}


WITH group_by_user AS (
    SELECT
        user_id,
        ANY_VALUE(full_name) AS full_name,
        ANY_VALUE(age_account) AS age_account,
        ANY_VALUE(age_account_segment) AS age_account_segment,
        ANY_VALUE(is_active) AS is_active,
        COUNT(*) AS total_transactions,
        SUM(total_amount) AS total_spending,
        AVG(total_amount) AS average_spending,
        MAX(total_amount) AS max_spending,
        MIN(total_amount) AS min_spending,
        MIN(rental_start_time) AS first_transaction_date,
        MAX(rental_end_time) AS last_transaction_date,
        SUM(rental_duration_days) AS total_rental_days,
        AVG(rental_duration_days) AS avg_rental_duration_days
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id
),
-- Vehicle Usage
vehicle_usage_base AS (
    SELECT user_id, vehicle_id, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id, vehicle_id
),
vehicle_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY freq DESC) AS rn
    FROM vehicle_usage_base
),
-- Brand Usage
brand_usage_base AS (
    SELECT user_id, vehicle_brand, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id, vehicle_brand
),
brand_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY freq DESC) AS rn
    FROM brand_usage_base
),
-- Vehicle Type Usage
v_type_usage_base AS (
    SELECT user_id, vehicle_type, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id, vehicle_type
),
v_type_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY freq DESC) AS rn
    FROM v_type_usage_base
),
-- Pickup City Usage
pickup_city_usage_base AS (
    SELECT user_id, pickup_city, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id, pickup_city
),
pickup_city_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY freq DESC) AS rn
    FROM pickup_city_usage_base
),
-- Dropoff City Usage
dropoff_city_usage_base AS (
    SELECT user_id, dropoff_city, COUNT(*) AS freq
    FROM {{ ref('transform_dataset.denormalized_transactions') }}
    GROUP BY user_id, dropoff_city
),
dropoff_city_usage AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY freq DESC) AS rn
    FROM dropoff_city_usage_base
),
most_frequent_data AS (
    SELECT
        u.user_id,
        v.vehicle_id AS most_used_vehicle_id,
        b.vehicle_brand AS most_used_vehicle_brand,
        vt.vehicle_type AS most_used_vehicle_type,
        p.pickup_city AS most_used_pickup_city,
        d.dropoff_city AS most_used_dropoff_city
    FROM (SELECT DISTINCT user_id FROM {{ ref('transform_dataset.denormalized_transactions') }}) u
    LEFT JOIN vehicle_usage v ON u.user_id = v.user_id AND v.rn = 1
    LEFT JOIN brand_usage b ON u.user_id = b.user_id AND b.rn = 1
    LEFT JOIN v_type_usage vt ON u.user_id = vt.user_id AND vt.rn = 1
    LEFT JOIN pickup_city_usage p ON u.user_id = p.user_id AND p.rn = 1
    LEFT JOIN dropoff_city_usage d ON u.user_id = d.user_id AND d.rn = 1
)

SELECT
    gbu.*,
    most.most_used_vehicle_id,
    most.most_used_vehicle_brand,
    most.most_used_vehicle_type,
    most.most_used_pickup_city,
    most.most_used_dropoff_city
FROM group_by_user gbu
LEFT JOIN most_frequent_data most ON gbu.user_id = most.user_id