import duckdb

def main():
    print("Running SQL transformations...")

    con = duckdb.connect('data/ecommerce_pipeline.duckdb')

    # Load SQL file
    with open('sql/01_transformations.sql', 'r') as f:
        sql_script = f.read()

    # Execute all SQL
    con.execute(sql_script)

    # Verify tables created
    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'transformed'
    """).fetchall()

    print("")
    print("Created transformed tables:")
    for t in tables:
        name = t[0]
        count = con.execute(f"SELECT COUNT(*) FROM transformed.{name}").fetchone()[0]
        print(f"  {name}: {count} rows")

    con.close()

if __name__ == "__main__":
    main()

