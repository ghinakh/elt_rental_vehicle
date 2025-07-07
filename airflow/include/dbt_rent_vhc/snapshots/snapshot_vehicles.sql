{% snapshot snapshot_vehicles %}
{{ config(unique_key='vehicle_id', strategy='check', check_cols=['active', 'vehicle_license_number', 'registration_name', 'license_type', 'expiration_date', 'permit_license_number', 'base_telephone_number', 'base_address', 'last_update_timestamp', 'vehicle_type']) }}

SELECT
    vehicle_id, active, vehicle_license_number, registration_name, license_type, expiration_date, permit_license_number, 
    certification_date, vehicle_year, base_telephone_number, base_address, last_update_timestamp, brand, vehicle_type
FROM {{ ref('clean_vehicles') }}

{% endsnapshot %}