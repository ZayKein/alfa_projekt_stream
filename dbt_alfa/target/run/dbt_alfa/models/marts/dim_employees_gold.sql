
  
    

        create or replace transient table ALFA_PROJEKT.SILVER_GOLD.dim_employees_gold
         as
        (
SELECT * FROM ALFA_PROJEKT.SILVER_SILVER.stg_employees
        );
      
  