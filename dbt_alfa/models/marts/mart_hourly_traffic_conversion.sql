{{ config(
    materialized='incremental',
    unique_key='event_hour'
) }}

SELECT
    DATE_TRUNC('hour', t.event_timestamp) as event_hour,
    DATE_TRUNC('day', t.event_timestamp)::DATE as event_date,
    EXTRACT(HOUR FROM t.event_timestamp) as hour_of_day,
    COUNT(DISTINCT t.traffic_id) as total_visits,
    COUNT(CASE WHEN t.item_in_cart = 'yes' THEN 1 END) as total_carts,
    COUNT(DISTINCT t.order_id) as total_orders
FROM {{ ref('stg_traffic') }} t

{% if is_incremental() %}
  WHERE t.event_timestamp > (SELECT MAX(event_hour) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3