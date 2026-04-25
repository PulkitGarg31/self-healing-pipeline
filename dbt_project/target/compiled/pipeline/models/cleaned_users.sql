

SELECT
    id,
    INITCAP(name) as name,
    age,
    INITCAP(city) as city,
    ingested_at
FROM raw_users
WHERE age BETWEEN 0 AND 120
  AND name IS NOT NULL


  AND ingested_at > (
    SELECT COALESCE(MAX(ingested_at) - INTERVAL '2 hours', '2000-01-01')
    FROM "airflow"."public"."cleaned_users"
  )
