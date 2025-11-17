import duckdb
import os

EXPORT_DIR = "data/analytics_ready/"

def main():
    os.makedirs(EXPORT_DIR, exist_ok=True)

    con = duckdb.connect("data/ecommerce_pipeline.duckdb")

    tables = [
        "raw.orders",
        "cleaned.orders",
        "transformed.fct_orders",
        "transformed.dim_customers",
        "transformed.product_performance",
        "transformed.marketing_attribution",
        "transformed.daily_metrics",
        "analytics_ready.data_quality_history"
    ]

    print("Exporting tables for Tableau...")

    for table in tables:
        name = table.split(".")[1]
        out = f"{EXPORT_DIR}{name}.csv"

        con.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{out}'
            WITH (HEADER, DELIMITER ',')
        """)

        print(f"  Exported {table} → {out}")

    print("All exports complete.")

if __name__ == "__main__":
    main()

