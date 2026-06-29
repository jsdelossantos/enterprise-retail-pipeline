SELECT
    dc.customer_state,
    SUM(fs.price) AS total_revenue,
    SUM(fs.freight_value) AS freight_costs
FROM fact_sales fs
LEFT JOIN dim_customers dc
    ON fs.customer_id = dc.customer_id
WHERE fs.order_status = 'delivered'
GROUP BY dc.customer_state
ORDER BY total_revenue DESC
LIMIT 3