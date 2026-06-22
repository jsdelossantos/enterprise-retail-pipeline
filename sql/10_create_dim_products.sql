-- 1. The DDL: Create the target OLAP Dimension Table
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
	product_id TEXT PRIMARY KEY,
	category_name_english TEXT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
);

-- 2. The DML: Clean, transform, and load the data from staging
INSERT INTO dim_products (
    product_id,
    category_name_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT 
    p.product_id,
    COALESCE(t.product_category_name_english, 'Unknown') AS category_name_english,
    CAST(CAST(NULLIF(p.product_weight_g, 'NaN') AS NUMERIC) AS INT) AS product_weight_g,
    CAST(CAST(NULLIF(p.product_length_cm, 'NaN') AS NUMERIC) AS INT) AS product_length_cm,
    CAST(CAST(NULLIF(p.product_height_cm, 'NaN') AS NUMERIC) AS INT) AS product_height_cm,
    CAST(CAST(NULLIF(p.product_width_cm, 'NaN') AS NUMERIC) AS INT) AS product_width_cm
FROM stg_products p
LEFT JOIN stg_category_name_translation t 
    ON p.product_category_name = t.product_category_name;