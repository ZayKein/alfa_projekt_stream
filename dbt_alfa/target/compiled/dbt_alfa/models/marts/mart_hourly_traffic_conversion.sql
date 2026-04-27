

SELECT
    DATE_TRUNC('hour', t.event_timestamp) as event_hour,
    EXTRACT(HOUR FROM t.event_timestamp) as hour_of_day,
    COUNT(DISTINCT t.traffic_id) as total_visits,
    COUNT(CASE WHEN t.item_in_cart = 'yes' THEN 1 END) as total_carts,
    COUNT(DISTINCT t.order_id) as total_orders
FROM ALFA_PROJEKT.SILVER_SILVER.stg_traffic t


  WHERE t.event_timestamp > (SELECT MAX(event_hour) FROM ALFA_PROJEKT.SILVER_GOLD.mart_hourly_traffic_conversion)


GROUP BY 1, 2