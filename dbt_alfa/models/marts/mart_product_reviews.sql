{{ config(
    materialized='incremental',
    unique_key='month_prod_id'
) }}

SELECT
    MD5(CONCAT(DATE_TRUNC('month', o.order_timestamp), o.product_id)) as month_prod_id,
    DATE_TRUNC('month', o.order_timestamp)::DATE as review_month,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    COUNT(o.order_id)                            as review_count,
    ROUND(AVG(o.review), 2)                      as avg_review,
    MIN(o.review)                                as min_review,
    MAX(o.review)                                as max_review,
    SUM(CASE WHEN o.review = 5 THEN 1 ELSE 0 END) as five_star_count,
    SUM(CASE WHEN o.review = 1 THEN 1 ELSE 0 END) as one_star_count,
    ROUND(
        CASE WHEN AVG(o.review) >= 4
            THEN  SUM(CASE WHEN o.review >= 4 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(o.order_id), 0)
            ELSE -SUM(CASE WHEN o.review >= 4 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(o.order_id), 0)
        END, 4)                                    as pct_positive,
    SUM(o.total_order_value)                       as total_revenue
FROM {{ ref('fact_orders_gold') }} o
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id

{% if is_incremental() %}
  WHERE o.order_timestamp > (SELECT MAX(review_month) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
