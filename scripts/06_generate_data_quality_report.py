import duckdb
import pandas as pd
import json
from datetime import datetime

OUTPUT_PATH = "data/analytics_ready/data_quality_report.json"

def main():
    con = duckdb.connect("data/ecommerce_pipeline.duckdb")

    print("Running data quality report...")
    report = {}

    # Total cleaned rows
    total_rows = con.execute("SELECT COUNT(*) FROM cleaned.orders").fetchone()[0]
    report["total_rows"] = total_rows

    # Completeness checks
    completeness = {}
    for col in ["customer_id", "customer_email", "product_id", "order_amount"]:
        non_null = con.execute(
            f"SELECT COUNT(*) FROM cleaned.orders WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        completeness[col] = round(non_null / total_rows * 100, 2)

    report["completeness"] = completeness

    # Validity checks
    validity = {}

    # Valid email pattern
    valid_emails = con.execute("""
        SELECT COUNT(*)
        FROM cleaned.orders
        WHERE customer_email LIKE '%@%' AND customer_email LIKE '%.%'
    """).fetchone()[0]
    validity["valid_email_format"] = round(valid_emails / total_rows * 100, 2)

    # Positive amounts
    positive_amounts = con.execute("""
        SELECT COUNT(*) FROM cleaned.orders WHERE order_amount > 0
    """).fetchone()[0]
    validity["positive_amounts"] = round(positive_amounts / total_rows * 100, 2)

    # Valid state codes
    valid_states = con.execute("""
        SELECT COUNT(*)
        FROM cleaned.orders
        WHERE shipping_state IN ('CA','NY','TX','FL','IL','PA','OH','GA','NC','MI','UNKNOWN')
    """).fetchone()[0]
    validity["valid_states"] = round(valid_states / total_rows * 100, 2)

    report["validity"] = validity

    # Uniqueness
    unique_orders = con.execute("""
        SELECT COUNT(DISTINCT order_id) FROM cleaned.orders
    """).fetchone()[0]
    report["uniqueness"] = {
        "order_id_uniqueness": round(unique_orders / total_rows * 100, 2)
    }

    # Consistency
    consistency = {}

    product_matches = con.execute("""
        SELECT COUNT(*)
        FROM cleaned.orders o
        JOIN raw.product_catalog p ON o.product_id = p.product_id
    """).fetchone()[0]
    consistency["product_catalog_match"] = round(product_matches / total_rows * 100, 2)

    report["consistency"] = consistency

    # Save report
    report["generated_at"] = datetime.now().isoformat()
    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Saved data quality report to {OUTPUT_PATH}")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()

