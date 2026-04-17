
  
    

  create  table "airflow"."public"."city_summary__dbt_tmp"
  
  
    as
  
  (
    SELECT
    city,
    COUNT(*) as total_users,
    AVG(age) as avg_age
FROM "airflow"."public"."cleaned_users"
GROUP BY city
  );
  