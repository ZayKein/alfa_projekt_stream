{{ config(
    materialized='incremental',
    unique_key='city_day_id'
) }}

SELECT
    MD5(CONCAT(o.city, DATE_TRUNC('day', o.order_timestamp)::TEXT)) as city_day_id,
    o.city,
    CASE o.city
        WHEN 'Praha'              THEN 'Praha'
        WHEN 'Brno'               THEN 'Jihomoravský'
        WHEN 'Ostrava'            THEN 'Moravskoslezský'
        WHEN 'Plzeň'              THEN 'Plzeňský'
        WHEN 'Liberec'            THEN 'Liberecký'
        WHEN 'Olomouc'            THEN 'Olomoucký'
        WHEN 'České Budějovice'   THEN 'Jihočeský'
        WHEN 'Hradec Králové'     THEN 'Královéhradecký'
        WHEN 'Pardubice'          THEN 'Pardubický'
        WHEN 'Ústí nad Labem'     THEN 'Ústecký'
        WHEN 'Havířov'            THEN 'Moravskoslezský'
        WHEN 'Zlín'               THEN 'Zlínský'
        WHEN 'Kladno'             THEN 'Středočeský'
        WHEN 'Most'               THEN 'Ústecký'
        WHEN 'Frýdek-Místek'      THEN 'Moravskoslezský'
        WHEN 'Opava'              THEN 'Moravskoslezský'
        WHEN 'Karviná'            THEN 'Moravskoslezský'
        WHEN 'Jihlava'            THEN 'Vysočina'
        WHEN 'Děčín'              THEN 'Ústecký'
        WHEN 'Teplice'            THEN 'Ústecký'
    END                                            as region,
    DATE_TRUNC('year', o.order_timestamp)::DATE  as sales_year,
    DATE_TRUNC('month', o.order_timestamp)::DATE as sales_month,
    DATE_TRUNC('day', o.order_timestamp)::DATE   as sales_day,
    COUNT(DISTINCT o.order_id)                   as total_orders,
    SUM(o.quantity)                              as total_qty,
    SUM(o.product_revenue)                           as product_revenue,
    SUM(o.addon_revenue)                             as addon_revenue,
    SUM(o.total_order_value)                         as total_revenue,
    SUM(o.quantity * (p.base_price - p.unit_cost))   as total_margin
FROM {{ ref('fact_orders_gold') }} o
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id

{% if is_incremental() %}
  WHERE o.order_timestamp > (SELECT MAX(sales_day) FROM {{ this }})
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
