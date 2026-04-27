{{ config(materialized='incremental', unique_key='month_prod_id') }}

SELECT
    MD5(CONCAT(DATE_TRUNC('month', o.order_timestamp), p.product_id)) as month_prod_id,
    DATE_TRUNC('month', o.order_timestamp) as sales_month,
    EXTRACT(YEAR FROM o.order_timestamp) as sales_year,
    p.category,
    p.subcategory,
    p.product_name,
    SUM(o.quantity) as total_qty,
    SUM(o.quantity * p.base_price) as total_revenue,
    SUM(o.quantity * (p.base_price - p.unit_cost)) as total_margin
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id

{% if is_incremental() %}
  WHERE o.order_timestamp > (SELECT MAX(sales_month) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
