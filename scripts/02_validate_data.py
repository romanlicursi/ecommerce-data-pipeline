import duckdb
import logging
from datetime import datetime

logging.basicConfig(
    filename='logs/validation.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("Running data validation...")
    con = duckdb.connect('data/ecommerce_pipeline.duckdb')

    # Make sure validated schema exists
    con.execute("CREATE SCHEMA IF NOT EXISTS validated")

    # Total records
    total_orders = con.execute("SELECT COUNT(*) FROM raw.orders").fetchone()[0]

    # Duplicate order_ids
    duplicate_order_ids = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id
            FROM raw.orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # NULL customer_ids
    null_customer_ids = con.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE customer_id IS NULL
    """).fetchone()[0]

    # Orphaned product_ids (no match in product_catalog)
    orphaned_product_ids = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT o.order_id
            FROM raw.orders o
            LEFT JOIN raw.product_catalog p
                ON o.product_id = p.product_id
            WHERE p.product_id IS NULL
        )
    """).fetchone()[0]

    # Negative order_amount
    negative_amounts = con.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE order_amount < 0
    """).fetchone()[0]

    # Invalid emails containing "_AT_"
    invalid_emails = con.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE customer_email LIKE '%_AT_%'
    """).fetchone()[0]

    # Invalid states (not in allowed list)
    invalid_states = con.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE shipping_state NOT IN ('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI')
    """).fetchone()[0]

    # Store a one row summary table
    con.execute("""
        CREATE OR REPLACE TABLE validated.validation_summary AS
        SELECT
            ?::VARCHAR AS validation_timestamp,
            ?::BIGINT  AS total_orders,
            ?::BIGINT  AS duplicate_order_ids,
            ?::BIGINT  AS null_customer_ids,
            ?::BIGINT  AS orphaned_product_ids,
            ?::BIGINT  AS negative_amounts,
            ?::BIGINT  AS invalid_emails,
            ?::BIGINT  AS invalid_states
    """, [
        datetime.now().isoformat(),
        total_orders,
        duplicate_order_ids,
        null_customer_ids,
        orphaned_product_ids,
        negative_amounts,
        invalid_emails,
        invalid_states,
    ])

    con.close()

    # Print a readable summary
    print("")
    print("Validation summary:")
    print(f"  Total orders            : {total_orders}")
    print(f"  Duplicate order_ids     : {duplicate_order_ids}")
    print(f"  NULL customer_ids       : {null_customer_ids}")
    print(f"  Orphaned product_ids    : {orphaned_product_ids}")
    print(f"  Negative order_amount   : {negative_amounts}")
    print(f"  Invalid emails (_AT_)   : {invalid_emails}")
    print(f"  Invalid shipping_state  : {invalid_states}")
    print("")
    print("Summary saved in table validated.validation_summary")

if __name__ == "__main__":
    main()

