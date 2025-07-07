{{ config(
    materialized='table',
    schema='reporting_dataset'
) }}

WITH denorm AS (
  SELECT *
  FROM {{ ref('denormalized_transactions') }}
),
--
-- RANK 1 users per day
--
daily_user_rank_base AS (
  SELECT
    DATE(rental_end_time) AS rental_date,
    user_id,
    ANY_VALUE(full_name) AS full_name,
    COUNT(*) AS transactions_per_user
  FROM denorm
  GROUP BY rental_date, user_id
),
daily_user_rank AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY rental_date ORDER BY transactions_per_user DESC) AS user_rank
  FROM daily_user_rank_base
),
daily_top1_user AS (
  SELECT
    *
  FROM daily_user_rank
  WHERE user_rank = 1
),
top_users_agg AS (
  SELECT
    rental_date,
    ARRAY_AGG(STRUCT(user_id, full_name, transactions_per_user)) AS top_users
  FROM daily_top1_user
  GROUP BY rental_date
),
--
-- RANK 1 vehicles per day
--
daily_vehicle_rank_base AS (
  SELECT
    DATE(rental_end_time) AS rental_date,
    vehicle_id,
    ANY_VALUE(vehicle_brand) AS vehicle_brand,
    ANY_VALUE(vehicle_type) AS vehicle_type,
    COUNT(*) AS transactions_per_vehicle
  FROM denorm
  GROUP BY rental_date, vehicle_id
), 
daily_vehicle_rank AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY rental_date ORDER BY transactions_per_vehicle DESC) AS vehicle_rank
  FROM daily_vehicle_rank_base
),
daily_top1_vehicle AS (
  SELECT
    *
  FROM daily_vehicle_rank
  WHERE vehicle_rank = 1
),
top_vehicles_agg AS (
  SELECT
    rental_date,
    ARRAY_AGG(STRUCT(vehicle_id, vehicle_brand, vehicle_type, transactions_per_vehicle)) AS top_vehicles
  FROM daily_top1_vehicle
  GROUP BY rental_date
)
--
-- Final Join
--
SELECT
  u.rental_date AS rental_date,
  u.top_users,
  v.top_vehicles
FROM top_users_agg u
INNER JOIN top_vehicles_agg v ON u.rental_date = v.rental_date
ORDER BY rental_date