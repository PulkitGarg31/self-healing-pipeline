

SELECT
    city,
    COUNT(*) as total_users,
    ROUND(AVG(age)::numeric, 1) as avg_age
FROM "airflow"."public"."cleaned_users"
GROUP BY city