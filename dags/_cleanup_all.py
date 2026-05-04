"""
Full reset script — drops all Snowflake objects, Postgres tables, and raw CSVs.
Run via: docker exec alfa_stream_airflow python /opt/airflow/dags/_cleanup_all.py
"""
import os
import glob
import psycopg2
import snowflake.connector

if __name__ != '__main__':
    raise SystemExit(0)

# ── Snowflake ────────────────────────────────────────────────────────────────

SF_OBJECTS = {
    'RAW':    ['FACT_TRAFFIC', 'FACT_ORDERS', 'DIM_EMPLOYEES', 'DIM_PRODUCTS', 'FACT_PAYROLL'],
    'SILVER': ['STG_ORDERS', 'STG_PRODUCTS', 'STG_EMPLOYEES', 'STG_PAYROLL', 'STG_TRAFFIC'],
    'GOLD':   [
        'FACT_ORDERS_GOLD',
        'DIM_EMPLOYEES_GOLD',
        'DIM_PAYROLL_GOLD',
        'DIM_PRODUCTS_GOLD',
        'MART_HOURLY_TRAFFIC_CONVERSION',
        'MART_MONTHLY_PRODUCT_SALES',
        'MART_CITY_SALES',
        'MART_PRODUCT_REVIEWS',
    ],
}

print("=== SNOWFLAKE CLEANUP ===")
sf = snowflake.connector.connect(
    account='lraixsh-yh49291',
    user='ZAYKEIN',
    password=os.environ['SF_PASSWORD'],
    database='ALFA_PROJEKT',
    warehouse='ALFA_WH',
    role='ACCOUNTADMIN',
)
cur = sf.cursor()

for schema, objects in SF_OBJECTS.items():
    for obj in objects:
        full = f"ALFA_PROJEKT.{schema}.{obj}"
        dropped = False
        for kind in ('TABLE', 'VIEW'):
            try:
                cur.execute(f"DROP {kind} IF EXISTS {full}")
                print(f"  OK Dropped {kind}: {full}")
                dropped = True
                break
            except Exception:
                pass
        if not dropped:
            print(f"  -- Skipped: {full} (not found)")

cur.close()
sf.close()
print("Snowflake cleanup done.\n")

# ── PostgreSQL ────────────────────────────────────────────────────────────────

PG_TABLES = ['fact_traffic', 'fact_orders', 'dim_employees', 'fact_payroll', 'dim_products']

print("=== POSTGRES CLEANUP ===")
pg = psycopg2.connect(
    host='alfa_stream_postgres',
    port=5432,
    dbname='airflow',
    user='airflow',
    password='airflow',
)
pg.autocommit = True
pgcur = pg.cursor()

for table in PG_TABLES:
    try:
        pgcur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        print(f"  OK Dropped: {table}")
    except Exception as e:
        print(f"  -- {table}: {e}")

pgcur.close()
pg.close()
print("Postgres cleanup done.\n")

# ── Raw CSVs ──────────────────────────────────────────────────────────────────

CSV_DIR = "/opt/airflow/data"
KEEP = {'employees_master.csv', 'employees_payroll.csv', 'products_raw.test.csv'}

print("=== CSV CLEANUP ===")
for f in glob.glob(f"{CSV_DIR}/*.csv"):
    fname = os.path.basename(f)
    if fname not in KEEP:
        os.remove(f)
        print(f"  Deleted: {fname}")
    else:
        print(f"  Kept:    {fname}")

print("\n=== ALL DONE — ready to rerun pipeline ===")
