import duckdb
from datetime import datetime

def main():
    print("Running data cleaning...")
    con = duckdb.connect('data/ecommerce_pipeline.duckdb')

    # Create cleaned schema
    con.execute("CREATE SCHEMA IF NOT EXISTS cleaned")

    # 1. Remove duplicates (keep first)
    con.execute("""
        CREATE OR REPLACE TABLE cleaned.orders AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
            FROM raw.orders
        )
        WHERE rn = 1
    """)

    # 2. Fix NULL customer_ids
    con.execute("""
        UPDATE cleaned.orders
        SET customer_id = 'CUST_UNKNOWN_' || order_id
        WHERE customer_id IS NULL
    """)

    # 3. Fix invalid emails (_AT_ → @)
    con.execute("""
        UPDATE cleaned.orders
        SET customer_email = REPLACE(customer_email, '_AT_', '@')
        WHERE customer_email LIKE '%_AT_%'
    """)

    # 4. Fix negative order_amount (absolute value)
    con.execute("""
        UPDATE cleaned.orders
        SET order_amount = ABS(order_amount)
        WHERE order_amount < 0
    """)

    # 5. Fix invalid states ("ZZ" etc → "UNKNOWN")
    con.execute("""
        UPDATE cleaned.orders
        SET shipping_state = 'UNKNOWN'
        WHERE shipping_state NOT IN ('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI')
    """)

    # 6. Standardize marketing_source casing
    con.execute("""
        UPDATE cleaned.orders
        SET marketing_source = UPPER(SUBSTR(marketing_source, 1, 1)) ||
    LOWER(SUBSTR(marketing_source, 2, LENGTH(marketing_source) - 1));
    """)

    # 7. Remove orphaned product_ids (cannot join to catalog)
    con.execute("""
        DELETE FROM cleaned.orders
        WHERE product_id NOT IN (
            SELECT product_id FROM raw.product_catalog
        )
    """)

    # 8. Standardize order_date format
    con.execute("""
        ALTER TABLE cleaned.orders ADD COLUMN order_date_clean DATE
    """)

    # Try YYYY-MM-DD
    con.execute("""
        UPDATE cleaned.orders
        SET order_date_clean = TRY_CAST(order_date AS DATE)
        WHERE order_date_clean IS NULL
    """)

    # Try MM/DD/YYYY
    con.execute("""
        UPDATE cleaned.orders
        SET order_date_clean = STRPTIME(order_date, '%m/%d/%Y')
        WHERE order_date_clean IS NULL
    """)

    # Replace column
    con.execute("ALTER TABLE cleaned.orders DROP COLUMN order_date")
    con.execute("ALTER TABLE cleaned.orders RENAME COLUMN order_date_clean TO order_date")

    # Log completion info
    total_cleaned = con.execute("SELECT COUNT(*) FROM cleaned.orders").fetchone()[0]

    con.close()

    print("")
    print("Cleaning complete.")
    print(f"Remaining rows after cleaning: {total_cleaned}")

if __name__ == "__main__":
    main()

