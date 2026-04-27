-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into ALFA_PROJEKT.SILVER_GOLD.mart_hourly_traffic_conversion as DBT_INTERNAL_DEST
        using ALFA_PROJEKT.SILVER_GOLD.mart_hourly_traffic_conversion__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.event_hour = DBT_INTERNAL_DEST.event_hour
            )

    
    when matched then update set
        "EVENT_HOUR" = DBT_INTERNAL_SOURCE."EVENT_HOUR","HOUR_OF_DAY" = DBT_INTERNAL_SOURCE."HOUR_OF_DAY","TOTAL_VISITS" = DBT_INTERNAL_SOURCE."TOTAL_VISITS","TOTAL_CARTS" = DBT_INTERNAL_SOURCE."TOTAL_CARTS","TOTAL_ORDERS" = DBT_INTERNAL_SOURCE."TOTAL_ORDERS"
    

    when not matched then insert
        ("EVENT_HOUR", "HOUR_OF_DAY", "TOTAL_VISITS", "TOTAL_CARTS", "TOTAL_ORDERS")
    values
        ("EVENT_HOUR", "HOUR_OF_DAY", "TOTAL_VISITS", "TOTAL_CARTS", "TOTAL_ORDERS")

;
    commit;