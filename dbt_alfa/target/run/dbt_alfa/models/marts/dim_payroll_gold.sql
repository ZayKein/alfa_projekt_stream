
  
    

        create or replace transient table ALFA_PROJEKT.SILVER_GOLD.dim_payroll_gold
         as
        (
SELECT * FROM ALFA_PROJEKT.SILVER_SILVER.stg_payroll
        );
      
  