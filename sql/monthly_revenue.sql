WITH monthly_seller_revenue AS (
	SELECT 
	DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP)) as sales_month,
	oi.seller_id as seller_id, 
	CAST(oi.price AS NUMERIC) as price 
	FROM stg_orders o
	JOIN stg_order_items oi ON o.order_id = oi.order_id
	WHERE
	o.order_status = 'delivered'
)
SELECT sales_month, seller_id, SUM(price) AS monthly_revenue
FROM monthly_seller_revenue
GROUP BY seller_id, sales_month
ORDER BY seller_id DESC, sales_month DESC;