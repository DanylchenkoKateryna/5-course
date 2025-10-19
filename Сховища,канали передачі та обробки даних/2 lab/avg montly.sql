SELECT 
	order_id,
    DATE_TRUNC('month', order_date) AS month,
    ROUND(AVG(total_price) OVER (PARTITION BY DATE_TRUNC('month', order_date)), 2) AS avg_check
FROM orders
ORDER BY month;
