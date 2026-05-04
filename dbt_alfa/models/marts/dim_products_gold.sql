{{ config(materialized='table') }}

WITH product_reviews AS (
    SELECT
        product_id,
        ROUND(AVG(review), 2) AS avg_review_score
    FROM {{ ref('fact_orders_gold') }}
    WHERE review IS NOT NULL
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.base_price,
    p.unit_cost,
    COALESCE(r.avg_review_score, 0) AS avg_review_score
FROM {{ ref('stg_products') }} p
LEFT JOIN product_reviews r ON p.product_id = r.product_id
