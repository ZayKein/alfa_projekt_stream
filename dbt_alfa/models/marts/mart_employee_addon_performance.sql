{{ config(
    materialized='incremental',
    unique_key='month_employee_id'
) }}

SELECT
    MD5(CONCAT(DATE_TRUNC('month', o.order_timestamp)::varchar, o.employee_id::varchar)) AS month_employee_id,
    DATE_TRUNC('month', o.order_timestamp)::date      AS performance_month,
    o.employee_id,
    e.employee_name,
    e.gender,
    e.hire_date,
    DATEDIFF('month', e.hire_date,
        DATE_TRUNC('month', o.order_timestamp))       AS tenure_months,
    CASE
        WHEN DATEDIFF('month', e.hire_date,
            DATE_TRUNC('month', o.order_timestamp)) < 6   THEN '0-6 months'
        WHEN DATEDIFF('month', e.hire_date,
            DATE_TRUNC('month', o.order_timestamp)) < 12  THEN '6-12 months'
        WHEN DATEDIFF('month', e.hire_date,
            DATE_TRUNC('month', o.order_timestamp)) < 36  THEN '1-3 years'
        ELSE '3+ years'
    END                                               AS tenure_bracket,
    COUNT(o.order_id)                                 AS total_orders,
    COUNT(CASE WHEN o.service_type IS NOT NULL
               THEN 1 END)                            AS addon_orders,
    ROUND(
        COUNT(CASE WHEN o.service_type IS NOT NULL THEN 1 END)
        / NULLIF(COUNT(o.order_id), 0) * 100
    , 2)                                              AS addon_attach_rate_pct,
    SUM(o.addon_revenue)                              AS total_addon_revenue,
    ROUND(
        SUM(o.addon_revenue)
        / NULLIF(COUNT(CASE WHEN o.service_type IS NOT NULL THEN 1 END), 0)
    , 2)                                              AS avg_addon_value
FROM {{ ref('fact_orders_gold') }} o
LEFT JOIN {{ ref('dim_employees_gold') }} e ON o.employee_id = e.employee_id

{% if is_incremental() %}
  WHERE DATE_TRUNC('month', o.order_timestamp) >= (SELECT MAX(performance_month) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
