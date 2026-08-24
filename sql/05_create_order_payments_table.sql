CREATE TABLE stg_order_payments (
    order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT,
    PRIMARY KEY (order_id, payment_sequential)
);