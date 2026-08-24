CREATE INDEX IF NOT EXISTS idx_stg_geolocation_zip_code_prefix ON stg_geolocation (geolocation_zip_code_prefix);
CREATE INDEX IF NOT EXISTS idx_stg_seller_zip_code_prefix ON stg_sellers (seller_zip_code_prefix);
CREATE INDEX IF NOT EXISTS idx_stg_customer_zip_code_prefix ON stg_customers(customer_zip_code_prefix);
CREATE INDEX IF NOT EXISTS idx_stg_products_category_name ON stg_products (product_category_name);