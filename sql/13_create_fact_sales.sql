DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
    order_id TEXT,
    order_item_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    price NUMERIC,
    freight_value NUMERIC,
    total_payment_value NUMERIC,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO fact_sales (
    order_id,
    order_item_id,
    customer_id,
    product_id,
    seller_id,
    price,
    freight_value,
    total_payment_value,
    order_status,
    order_purchase_timestamp
)
WITH order_payments_agg AS (
    SELECT
        op.order_id,
        SUM(CAST(op.payment_value AS NUMERIC)) as total_payment_value
    FROM stg_order_payments op
    GROUP BY order_id
)
SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    CAST(oi.price AS NUMERIC) as price,
    CAST(oi.freight_value AS NUMERIC) as freight_value,
    op.total_payment_value,
    o.order_status,
    CAST(o.order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp
FROM stg_order_items oi
LEFT JOIN order_payments_agg op
    ON oi.order_id = op.order_id
LEFT JOIN stg_orders o
    ON oi.order_id = o.order_id
