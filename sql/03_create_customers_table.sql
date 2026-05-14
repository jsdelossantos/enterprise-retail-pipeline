DROP TABLE IF EXISTS stg_customers;

CREATE TABLE stg_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);