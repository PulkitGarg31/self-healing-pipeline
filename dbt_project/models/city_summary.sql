{{ config(
    materialized='incremental',
    unique_key='city'
) }}

SELECT
    city,
    COUNT(*) as total_users,
    ROUND(AVG(age)::numeric, 1) as avg_age
FROM {{ ref('cleaned_users') }}
GROUP BY city