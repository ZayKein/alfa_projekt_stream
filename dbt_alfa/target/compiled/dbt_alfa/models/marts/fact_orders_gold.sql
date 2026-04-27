

SELECT 
    o.order_id,
    o.order_timestamp,
    o.employee_id,
    o.product_id,
    o.quantity,
    p.base_price as unit_price,
    (o.quantity * p.base_price) as product_revenue,
    o.service_price as addon_revenue,
    -- Celková tržba za řádek (produkty + addon)
    (o.quantity * p.base_price) + o.service_price as total_order_value,
    o.service_type
FROM ALFA_PROJEKT.SILVER_SILVER.stg_orders o
LEFT JOIN ALFA_PROJEKT.SILVER_SILVER.stg_products p ON o.product_id = p.product_id

