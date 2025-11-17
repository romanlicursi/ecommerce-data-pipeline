import duckdb

def main():
    con = duckdb.connect("data/ecommerce_pipeline.duckdb")

    print("=== ALL TABLES IN DATABASE ===")
    tables = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        ORDER BY table_schema, table_name
    """).fetchall()

    for schema, table in tables:
        print(f"\n--- {schema}.{table} ---")

        # Row count
        count = con.execute(
            f"SELECT COUNT(*) FROM {schema}.{table}"
        ).fetchone()[0]
        print(f"Rows: {count}")

        # Schema
        schema_info = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}'
        """).fetchall()

        print("Columns:")
        for col, dtype in schema_info:
            print(f"  - {col}: {dtype}")

if __name__ == "__main__":
    main()

