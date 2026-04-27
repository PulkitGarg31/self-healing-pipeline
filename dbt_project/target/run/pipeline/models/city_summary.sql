
      
        
            delete from "airflow"."public"."city_summary"
            where (
                city) in (
                select (city)
                from "city_summary__dbt_tmp094834671482"
            );

        
    

    insert into "airflow"."public"."city_summary" ("city", "total_users", "avg_age")
    (
        select "city", "total_users", "avg_age"
        from "city_summary__dbt_tmp094834671482"
    )
  