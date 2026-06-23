-- 1. The DDL: Create the target OLAP Dimension Table
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    geolocation_lat NUMERIC,
    geolocation_lng NUMERIC
);

INSERT INTO dim_customers (
    customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geolocation_lat,
    geolocation_lng
)
WITH geolocation_data AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(CAST(geolocation_lat AS NUMERIC)) AS avg_geo_lat,
        AVG(CAST(geolocation_lng AS NUMERIC)) AS avg_geo_lng
    FROM stg_geolocation
    GROUP BY geolocation_zip_code_prefix
)
SELECT
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.avg_geo_lat,
    g.avg_geo_lng
FROM stg_customers c
LEFT JOIN geolocation_data g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix