import duckdb
import pandas as pd
import json
from datetime import datetime

def main():
    print("Running data quality monitoring...")

    con = duckdb.connect("data/ecommerce_pipeline.duckdb")

    # Load cleaned table to pandas
    df = con.execute("SELECT * FROM cleaned.orders").df()
    total = len(df)

    # COMPLETENESS
    completeness_customer_id = df["customer_id"].notnull().mean() * 100
    completeness_product_id = df["product_id"].notnull().mean() * 100
    completeness_customer_email = df["customer_email"].notnull().mean() * 100

    # VALIDITY
    validity_email = df["customer_email"].str.contains("@", na=False).mean() * 100
    validity_amount = (df["order_amount"] > 0).mean() * 100
    validity_state = (df["shipping_state"] != "UNKNOWN").mean() * 100

    # UNIQUENESS
    uniqueness_order_ids = df["order_id"].nunique() / total * 100

    # CONSISTENCY
    product_ids_catalog = con.execute("SELECT product_id FROM raw.product_catalog").df()["product_id"].unique()
    consistency_product = df["product_id"].isin(product_ids_catalog).mean() * 100

    metrics = {
        "completeness_customer_id": completeness_customer_id,
        "completeness_product_id": completeness_product_id,
        "completeness_customer_email": completeness_customer_email,
        "validity_email": validity_email,
        "validity_amount": validity_amount,
        "validity_state": validity_state,
        "uniqueness_order_ids": uniqueness_order_ids,
        "consistency_product": consistency_product,
    }

    all_scores = list(metrics.values())
    quality_score = sum(all_scores) / len(all_scores)

    metrics_json = json.dumps(metrics)

    # Create schema and table if needed
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics_ready")
    con.execute(
        "CREATE TABLE IF NOT EXISTS analytics_ready.data_quality_history (timestamp VARCHAR, total_records INTEGER, quality_score DOUBLE, metrics_json VARCHAR)"
    )

    # Insert row
    con.execute(
        "INSERT INTO analytics_ready.data_quality_history VALUES (?, ?, ?, ?)",
        [datetime.now().isoformat(), int(total), float(quality_score), metrics_json],
    )

    con.close()

    print("Quality score:", round(quality_score, 2))
    print("Total records:", total)
    print("Saved to analytics_ready.data_quality_history")

if __name__ == "__main__":
    main()
