import duckdb
import pandas as pd
import json
from datetime import datetime
import logging

logging.basicConfig(
    filename='logs/ingestion.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ingest_orders(conn):
    logging.info("Starting orders ingestion")
    orders_df = pd.read_csv('data/raw/orders.csv')
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("DROP TABLE IF EXISTS raw.orders")
    conn.execute("CREATE TABLE raw.orders AS SELECT * FROM orders_df")
    row_count = conn.execute("SELECT COUNT(*) FROM raw.orders").fetchone()[0]
    logging.info(f"Ingested {row_count} orders")
    print(f"Loaded {row_count} orders into raw.orders")

def ingest_product_catalog(conn):
    logging.info("Starting product catalog ingestion")
    with open('data/raw/product_catalog.json') as f:
        products = json.load(f)
    products_df = pd.DataFrame(products)
    conn.execute("DROP TABLE IF EXISTS raw.product_catalog")
    conn.execute("CREATE TABLE raw.product_catalog AS SELECT * FROM products_df")
    row_count = conn.execute("SELECT COUNT(*) FROM raw.product_catalog").fetchone()[0]
    logging.info(f"Ingested {row_count} products")
    print(f"Loaded {row_count} products into raw.product_catalog")

def main():
    print("Running ingestion...")
    conn = duckdb.connect('data/ecommerce_pipeline.duckdb')
    ingest_orders(conn)
    ingest_product_catalog(conn)
    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    main()

