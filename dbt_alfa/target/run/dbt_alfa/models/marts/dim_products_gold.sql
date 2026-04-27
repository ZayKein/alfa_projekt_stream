
  
    

        create or replace transient table ALFA_PROJEKT.SILVER_GOLD.dim_products_gold
         as
        (
SELECT * FROM ALFA_PROJEKT.SILVER_SILVER.stg_products
        );
      
  