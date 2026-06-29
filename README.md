# Olist E-Commerce Data Warehouse

This is an end-to-end ELT pipeline I built to wrangle the Brazilian E-Commerce Public Dataset by Olist. 

The goal of this project was to take 9 highly normalized, disconnected CSV files (containing over 100,000+ orders) and transform them into a clean, centralized Star Schema in PostgreSQL that analysts can actually use for Business Intelligence.

## Tech Stack
* **Python 3.11:** Data extraction and memory-efficient chunk loading via Pandas.
* **PostgreSQL:** Staging environment and final Data Warehouse.
* **Docker:** Containerized the pipeline to ensure it runs anywhere without environment headaches.
* **Architecture:** ELT (Extract, Load, Transform), Dimensional Modeling.

## How It Works

The pipeline is managed by a single entry point (`src/pipeline.py`) and runs in two distinct phases:

### Phase 1: Staging (Extract & Load)
The Python script extracts the raw CSV files, handles missing values, and dynamically generates `CREATE TABLE` and `INSERT` statements to push the data into PostgreSQL staging tables (`stg_*`). 

### Phase 2: The Star Schema (Transform)
Once the data is in Postgres, the pipeline triggers a series of SQL scripts to build the Star Schema. 
* **The Problem:** The raw Olist data is heavily normalized. Trying to join items, orders, and payments directly causes a Cartesian explosion, resulting in massively duplicated revenue numbers.
* **The Solution:** I built CTEs to pre-aggregate payment data at the `order_id` level before joining it back to the base `order_items` table. This ensures the grain of the `fact_sales` table stays strictly at "1 row per item sold" while accurately capturing the total order value.

**The final schema includes:**
* `fact_sales` (Core transaction data)
* `dim_products` (Product details with Portuguese-to-English translations)
* `dim_customers` (Customer details and aggregated geolocation coordinates)
* `dim_sellers` (Seller details and aggregated geolocation coordinates)

## How to Run This Locally

### Prerequisites
* Docker Desktop installed and running.
* A local PostgreSQL instance running on port 5432.
* The raw Olist CSV files placed in the `data/` directory.

### 1. Setup your environment
Create a `.env` file in the root folder of the project. Because the script runs inside a Docker container, we use `host.docker.internal` to allow the container to bridge out to your local machine's database.

```env
DB_HOST=host.docker.internal
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432
```

### 2. Build the Docker Image
Take a snapshot of the code and build the container:

```bash
docker build -t olist-pipeline .
```

### 3. Run the Pipeline
Execute the container. The script will automatically parse the data, build the staging tables, and execute the final SQL transformations.

```bash
docker run --env-file .env olist-pipeline
```

## Example Analytics

Here are a few examples of how the final Star Schema can be queried to answer real business questions without needing complex, messy joins.

**Top 5 Product Categories by Revenue (Delivered Orders Only):**

```sql
SELECT
    dp.category_name_english,
    SUM(fs.price) AS total_revenue
FROM fact_sales fs
LEFT JOIN dim_products dp
    ON fs.product_id = dp.product_id
WHERE fs.order_status = 'delivered'
GROUP BY dp.category_name_english
ORDER BY total_revenue DESC
LIMIT 5;
```

**Top 3 Customer States by Freight Costs:**

```sql
SELECT
    dc.customer_state,
    SUM(fs.price) AS total_revenue,
    SUM(fs.freight_value) AS freight_costs
FROM fact_sales fs
LEFT JOIN dim_customers dc
    ON fs.customer_id = dc.customer_id
WHERE fs.order_status = 'delivered'
GROUP BY dc.customer_state
ORDER BY total_revenue DESC
LIMIT 3;
```