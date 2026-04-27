

SELECT
    MD5(CONCAT(DATE_TRUNC('month', o.order_timestamp), p.product_id)) as month_prod_id,
    DATE_TRUNC('month', o.order_timestamp) as sales_month,
    p.category,
    p.subcategory,
    p.product_name,
    SUM(o.quantity) as total_qty,
    -- Tržba jen za produkty
    SUM(o.quantity * p.base_price) as product_revenue,
    -- Tržba za doplňkové služby
    SUM(o.service_price) as addon_revenue,
    -- Celková tržba
    SUM((o.quantity * p.base_price) + o.service_price) as total_revenue,
    -- Marže (pouze z produktů)
    SUM(o.quantity * (p.base_price - p.unit_cost)) as product_margin
FROM ALFA_PROJEKT.SILVER_SILVER.stg_orders o
JOIN ALFA_PROJEKT.SILVER_SILVER.stg_products p ON o.product_id = p.product_id



GROUP BY 1, 2, 3, 4, 5