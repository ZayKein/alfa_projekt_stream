{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT 
    o.order_id,
    o.order_timestamp,
    o.order_timestamp::DATE as order_date,
    o.employee_id,
    o.product_id,
    o.quantity,
    p.base_price as unit_price,
    (o.quantity * p.base_price) as product_revenue,
    o.service_price as addon_revenue,
    -- Celková tržba za řádek (produkty + addon)
    (o.quantity * p.base_price) + o.service_price as total_order_value,
    o.service_type
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id

{% if is_incremental() %}
  WHERE o.order_timestamp > (SELECT MAX(order_timestamp) FROM {{ this }})
{% endif %}