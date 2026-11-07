-- Current order state includes tombstones; only active orders contribute.
SELECT
    orders.order_date,
    products.category,
    COUNT(*) AS order_count,
    CAST(SUM(orders.quantity * orders.unit_price) AS DECIMAL(16, 2)) AS revenue
FROM pipeline_current_orders AS orders
JOIN pipeline_products AS products ON orders.product_id = products.product_id
WHERE NOT orders.is_deleted
GROUP BY orders.order_date, products.category
ORDER BY orders.order_date, products.category
