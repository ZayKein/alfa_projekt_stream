-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into ALFA_PROJEKT.SILVER_GOLD.mart_monthly_product_sales as DBT_INTERNAL_DEST
        using ALFA_PROJEKT.SILVER_GOLD.mart_monthly_product_sales__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.month_prod_id = DBT_INTERNAL_DEST.month_prod_id
            )

    
    when matched then update set
        "MONTH_PROD_ID" = DBT_INTERNAL_SOURCE."MONTH_PROD_ID","SALES_MONTH" = DBT_INTERNAL_SOURCE."SALES_MONTH","SALES_YEAR" = DBT_INTERNAL_SOURCE."SALES_YEAR","CATEGORY" = DBT_INTERNAL_SOURCE."CATEGORY","SUBCATEGORY" = DBT_INTERNAL_SOURCE."SUBCATEGORY","PRODUCT_NAME" = DBT_INTERNAL_SOURCE."PRODUCT_NAME","TOTAL_QTY" = DBT_INTERNAL_SOURCE."TOTAL_QTY","TOTAL_REVENUE" = DBT_INTERNAL_SOURCE."TOTAL_REVENUE","TOTAL_MARGIN" = DBT_INTERNAL_SOURCE."TOTAL_MARGIN"
    

    when not matched then insert
        ("MONTH_PROD_ID", "SALES_MONTH", "SALES_YEAR", "CATEGORY", "SUBCATEGORY", "PRODUCT_NAME", "TOTAL_QTY", "TOTAL_REVENUE", "TOTAL_MARGIN")
    values
        ("MONTH_PROD_ID", "SALES_MONTH", "SALES_YEAR", "CATEGORY", "SUBCATEGORY", "PRODUCT_NAME", "TOTAL_QTY", "TOTAL_REVENUE", "TOTAL_MARGIN")

;
    commit;