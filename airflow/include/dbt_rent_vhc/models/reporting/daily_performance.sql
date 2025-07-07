{{ config(
    materialized='table',
    schema='reporting_dataset'
) }}

WITH group_by_daily AS (
    SELECT
        rental_year, 
        rental_month, 
        rental_day,
        MAX(DATE(rental_end_time)) AS rental_date,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_spent,
        MIN(total_amount) AS min_spent,
        MAX(total_amount) AS max_spent,
        SUM(rental_duration_days) AS total_rental_days,
        AVG(rental_duration_days) AS avg_rental_duration_days,
        COUNT(DISTINCT pickup_city) AS unique_pickup_cities,
        COUNT(DISTINCT dropoff_city) AS unique_dropoff_cities
    FROM {{ ref('denormalized_transactions') }}
    GROUP BY rental_year, rental_month, rental_day
)

SELECT gbd.*
FROM group_by_daily gbd