SELECT
    id,
    INITCAP(name) as name,
    age,
    INITCAP(city) as city,
    ingested_at
FROM raw_users
WHERE age BETWEEN 0 AND 120
  AND name IS NOT NULL