import psycopg2

def conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="postgres",
        password="postgres"
    )

def test_fact_table_exists():
    c = conn()
    cur = c.cursor()
    cur.execute("""
        SELECT to_regclass('warehouse.fact_sales')
    """)
    assert cur.fetchone()[0] is not None
    c.close()

def test_fact_grain():
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    assert cur.fetchone()[0] > 0
    c.close()
