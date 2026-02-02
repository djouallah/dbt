
        
            delete from "ducklake"."aemo"."dim_calendar"
            where (
                date) in (
                select (date)
                from "dim_calendar__dbt_tmp20260202112814017910"
            );

        
    

    insert into "ducklake"."aemo"."dim_calendar" ("date", "year", "month")
    (
        select "date", "year", "month"
        from "dim_calendar__dbt_tmp20260202112814017910"
    )
  