

SELECT
    -- Unikátní klíč pro inkrementální merge (kombinace hodiny a kategorie)
    MD5(CONCAT(DATE_TRUNC('hour', t.event_timestamp), COALESCE(p.category, 'Unknown'))) as hour_cat_id,
    
    DATE_TRUNC('hour', t.event_timestamp) as event_hour,
    EXTRACT(HOUR FROM t.event_timestamp) as hour_of_day, -- Pro snadné filtrování peaků v PBI
    p.category,
    p.subcategory,
    
    COUNT(DISTINCT t.traffic_id) as total_visits,
    COUNT(CASE WHEN t.item_in_cart = 'yes' THEN 1 END) as total_carts,
    COUNT(DISTINCT t.order_id) as total_orders
FROM ALFA_PROJEKT.SILVER_SILVER.stg_traffic t
LEFT JOIN ALFA_PROJEKT.SILVER_SILVER.stg_products p ON t.product_id = p.product_id



GROUP BY 1, 2, 3, 4, 5