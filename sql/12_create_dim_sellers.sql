-- 1. The DDL: Create the target OLAP Dimension Table
DROP TABLE IF EXISTS dim_sellers;

CREATE TABLE dim_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT,
    geolocation_lat NUMERIC,
    geolocation_lng NUMERIC
);

INSERT INTO dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
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
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.avg_geo_lat,
    g.avg_geo_lng
FROM stg_sellers s
LEFT JOIN geolocation_data g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix