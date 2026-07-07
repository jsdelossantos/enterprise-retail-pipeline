CREATE INDEX idx_stg_orders_id ON stg_orders (order_id);
CREATE INDEX idx_stg_order_payments_id ON stg_order_payments (order_id);
CREATE INDEX idx_stg_order_items_id ON stg_order_items (order_id);
CREATE INDEX idx_stg_geolocation_zip_code_prefix ON stg_geolocation (geolocation_zip_code_prefix);
CREATE INDEX idx_stg_seller_zip_code_prefix ON stg_sellers (seller_zip_code_prefix);
CREATE INDEX idx_stg_customer_zip_code_prefix ON stg_customers(customer_zip_code_prefix);
CREATE INDEX idx_stg_products_category_name ON stg_products (product_category_name);
CREATE INDEX idx_stg_translation_category_name ON stg_category_name_translation(product_category_name);