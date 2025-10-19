SELECT p.product_name, SUM(oi.quantity * oi.price) as total
FROM order_items as oi
JOIN products as p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total DESC
LIMIT 25


