DROP TABLE IF EXISTS stg_order_payments;

CREATE TABLE stg_order_payments (
    order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT
);