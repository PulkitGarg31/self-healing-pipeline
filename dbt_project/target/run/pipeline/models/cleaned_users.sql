
      
        
            delete from "airflow"."public"."cleaned_users"
            where (
                id) in (
                select (id)
                from "cleaned_users__dbt_tmp094834470363"
            );

        
    

    insert into "airflow"."public"."cleaned_users" ("id", "name", "age", "city", "ingested_at")
    (
        select "id", "name", "age", "city", "ingested_at"
        from "cleaned_users__dbt_tmp094834470363"
    )
  