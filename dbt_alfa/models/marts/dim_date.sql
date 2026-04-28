{{ config(
    materialized='incremental',
    unique_key='date_day'
) }}

WITH date_spine AS (
    SELECT DATEADD(day, seq4(), '2023-01-01'::date) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))
)

SELECT
    date_day,
    YEAR(date_day)                                    AS year,
    QUARTER(date_day)                                 AS quarter,
    MONTH(date_day)                                   AS month,
    DAY(date_day)                                     AS day,
    DAYOFWEEKISO(date_day)                            AS day_of_week,
    DAYNAME(date_day)                                 AS day_name,
    MONTHNAME(date_day)                               AS month_name,
    TO_CHAR(date_day, 'YYYY-MM')                      AS year_month,
    DATE_TRUNC('week', date_day)::date                AS week_start,
    DATE_TRUNC('month', date_day)::date               AS month_start,
    DATE_TRUNC('quarter', date_day)::date             AS quarter_start,
    DATE_TRUNC('year', date_day)::date                AS year_start,
    CASE WHEN DAYOFWEEKISO(date_day) IN (6, 7)
         THEN TRUE ELSE FALSE END                     AS is_weekend
FROM date_spine

{% if is_incremental() %}
  WHERE date_day > (SELECT MAX(date_day) FROM {{ this }})
{% endif %}
