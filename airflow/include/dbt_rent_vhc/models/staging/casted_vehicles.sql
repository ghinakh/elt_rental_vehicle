{{ config(
    materialized='view',
    schema='staging_dataset'
) }}

SELECT
  vehicle_id,
  CAST(active AS BOOLEAN) AS active, 
  CAST(vehicle_license_number AS STRING) AS vehicle_license_number,
  registration_name,
  license_type,
  PARSE_DATE('%d-%m-%Y', expiration_date) AS expiration_date,
  permit_license_number,
  certification_date,
  vehicle_year,
  base_telephone_number,
  base_address,
  DATETIME(PARSE_TIMESTAMP('%d-%m-%Y %H:%M:%S', last_update_timestamp)) AS last_update_timestamp,
  brand,
  vehicle_type
FROM `staging_dataset.raw_vehicles`