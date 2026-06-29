SELECT
    dp.category_name_english,
    SUM(fs.price) AS total_revenue
FROM fact_sales fs
LEFT JOIN dim_products dp
    ON fs.product_id = dp.product_id
WHERE fs.order_status = 'delivered'
GROUP BY dp.category_name_english
ORDER BY total_revenue DESC
LIMIT 5