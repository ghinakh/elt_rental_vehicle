{% snapshot snapshot_users %}
{{ config(unique_key='user_id', strategy='check', check_cols=['first_name', 'last_name', 'email','phone_number','driver_license_number','driver_license_expiry','is_active']) }}

SELECT
    user_id,
    first_name,
    last_name,
    email,
    phone_number,
    driver_license_number,
    driver_license_expiry,
    creation_date,
    is_active
FROM  {{ ref('clean_users') }}

{% endsnapshot %}
