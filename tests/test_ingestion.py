import psycopg2

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="postgres",
        password="postgres"
    )

def test_db_connection():
    conn = get_conn()
    assert conn is not None
    conn.close()

def test_staging_tables_exist():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='staging'
    """)
    tables = [t[0] for t in cur.fetchall()]
    assert "customers" in tables
    conn.close()

def test_loaded_at_exists():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT loaded_at FROM staging.customers LIMIT 1
    """)
    assert cur.fetchone() is not None
    conn.close()
