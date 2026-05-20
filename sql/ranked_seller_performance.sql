WITH base_data AS (
	SELECT 
		DATE_TRUNC('month', CAST(o.order_purchase_timestamp AS TIMESTAMP)) as sales_month,
		oi.seller_id, 
		CAST(oi.price AS NUMERIC) as price
	FROM stg_orders o
	JOIN stg_order_items oi ON o.order_id = oi.order_id
	WHERE o.order_status = 'delivered'
),
monthly_seller_revenue AS (
	SELECT 
		sales_month, 
		seller_id, 
		SUM(price) AS monthly_revenue
	FROM base_data
	GROUP BY seller_id, sales_month
),
ranked_seller_performance AS (
	SELECT sales_month, 
		seller_id, 
		monthly_revenue,
		DENSE_RANK() OVER (
			PARTITION BY sales_month
			ORDER BY monthly_revenue DESC
		) AS seller_rank,
		LAG(monthly_revenue, 1, 0) OVER (
			PARTITION BY seller_id
			ORDER BY sales_month ASC
		) AS prev_month_revenue
	FROM monthly_seller_revenue
)
SELECT * FROM ranked_seller_performance
WHERE seller_rank <= 3