import duckdb

# Connect to DuckDB database file
con = duckdb.connect('data/ecommerce_pipeline.duckdb')

# Create raw schema
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

# Load orders
con.execute("""
    CREATE OR REPLACE TABLE raw.orders AS
    SELECT * FROM read_csv_auto('data/raw/orders.csv');
""")

# Load product catalog
con.execute("""
    CREATE OR REPLACE TABLE raw.product_catalog AS
    SELECT * FROM read_json_auto('data/raw/product_catalog.json');
""")

print("Database setup complete.")

