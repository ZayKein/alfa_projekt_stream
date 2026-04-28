{{ config(
    materialized='incremental',
    unique_key='month_product_id'
) }}

SELECT
    MD5(CONCAT(DATE_TRUNC('month', t.event_timestamp)::varchar, t.product_id::varchar)) AS month_product_id,
    DATE_TRUNC('month', t.event_timestamp)::date      AS traffic_month,
    t.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    COUNT(DISTINCT t.traffic_id)                      AS total_views,
    COUNT(CASE WHEN t.item_in_cart = 'yes' THEN 1 END) AS total_carts,
    COUNT(DISTINCT t.order_id)                        AS total_orders,
    ROUND(
        COUNT(CASE WHEN t.item_in_cart = 'yes' THEN 1 END)
        / NULLIF(COUNT(DISTINCT t.traffic_id), 0) * 100
    , 2)                                              AS cart_rate_pct,
    ROUND(
        COUNT(DISTINCT t.order_id)
        / NULLIF(COUNT(DISTINCT t.traffic_id), 0) * 100
    , 2)                                              AS conversion_rate_pct
FROM {{ ref('stg_traffic') }} t
LEFT JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id

{% if is_incremental() %}
  WHERE DATE_TRUNC('month', t.event_timestamp) >= (SELECT MAX(traffic_month) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
