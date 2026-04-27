from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd


def transfer_table_with_progress(table_name, target_table, incremental=False, date_col=None):
    # 1. Inicializace Hooků
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    sf_engine = sf_hook.get_sqlalchemy_engine()
    pg_engine = pg_hook.get_sqlalchemy_engine()

    print(f"--- START PŘENOSU: {table_name} -> {target_table} ---", flush=True)

    # 2. Rozhodnutí o strategii (Manual Clean vs Incremental)
    where_clause = ""

    if not incremental:
        # --- FIX PRO TVOU CHYBU: Ruční smazání tabulky místo replace v pandas ---
        print(
            f"INFO: Full Refresh - mažu tabulku RAW.{target_table.upper()}...", flush=True)
        sf_hook.run(f"DROP TABLE IF EXISTS RAW.{target_table.upper()}")
    else:
        # Inkrementální logika: Zjistíme, kam až jsme se dostali
        try:
            query_max = f"SELECT MAX({date_col.upper()}) FROM RAW.{target_table.upper()}"
            max_date = sf_hook.get_first(query_max)[0]
            if max_date:
                print(
                    f"INFO: Nalezeno maximum v SF: {max_date}. Filtruji zdroj...", flush=True)
                where_clause = f"WHERE {date_col} > '{max_date}'"
        except Exception as e:
            print(
                f"INFO: Tabulka v SF neexistuje nebo je prázdná, začínám od nuly. ({e})", flush=True)

    # 3. Načtení dat z lokálního Postgresu
    query = f"SELECT * FROM {table_name} {where_clause}"
    df = pd.read_sql(query, pg_engine)
    df.columns = [x.upper() for x in df.columns]

    if df.empty:
        print(f"✅ HOTOVO: Žádná nová data k přenosu.", flush=True)
        return

    print(
        f"INFO: Zahajuji nahrávání {len(df)} řádků do Snowflake...", flush=True)

    # 4. Nahrávání do Snowflake (Vždy append, protože jsme drop vyřešili ručně)
    chunk_size = 50000
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i: i + chunk_size]

        # Pandas si tabulku sám vytvoří (podle RAW schématu), pokud po DROPu neexistuje
        chunk.to_sql(
            target_table.upper(),
            sf_engine,
            schema='RAW',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000
        )
        print(
            f"PROGRES: {target_table} -> nahráno {min(i + chunk_size, len(df))} z {len(df)}...", flush=True)

    print(f"✅ ÚSPĚCH: Tabulka {target_table} je v cloudu.", flush=True)


# --- KONFIGURACE DAGU ---
default_args = {
    'owner': 'alfa_projekt',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    '03_Postgres_To_Snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt']
) as dag:

    # Definice tabulek a jejich režimů
    # Full Refresh (Dimenze a Payroll)
    dims = [
        {'pg': 'dim_employees', 'sf': 'DIM_EMPLOYEES', 'inc': False, 'col': None},
        {'pg': 'fact_payroll', 'sf': 'FACT_PAYROLL', 'inc': False, 'col': None},
        {'pg': 'dim_products', 'sf': 'DIM_PRODUCTS', 'inc': False, 'col': None},
    ]

    # Incremental (Fakta)
    facts = [
        {'pg': 'fact_traffic', 'sf': 'FACT_TRAFFIC',
            'inc': True, 'col': 'event_time'},
        {'pg': 'fact_orders', 'sf': 'FACT_ORDERS',
            'inc': True, 'col': 'order_date'},
    ]

    for t in dims + facts:
        PythonOperator(
            task_id=f'move_{t["pg"]}',
            python_callable=transfer_table_with_progress,
            op_kwargs={
                'table_name': t['pg'],
                'target_table': t['sf'],
                'incremental': t['inc'],
                'date_col': t['col']
            }
        )
