{{ config(
    materialized='incremental',
    unique_key='date_day'
) }}

WITH date_spine AS (
    SELECT DATEADD(day, seq4(), '2021-01-01'::date) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4000))
)

SELECT
    date_day,
    YEAR(date_day)                                             AS year,
    QUARTER(date_day)                                          AS quarter,
    MONTH(date_day)                                            AS month,
    DAY(date_day)                                              AS day,
    TO_CHAR(date_day, 'YYYY') || '-Q' || QUARTER(date_day)    AS year_quarter,
    TO_CHAR(date_day, 'YYYY-MM')                               AS year_month,
    TO_CHAR(date_day, 'Mon')                                   AS month_name
FROM date_spine

{% if is_incremental() %}
  WHERE date_day > (SELECT MAX(date_day) FROM {{ this }})
{% endif %}
