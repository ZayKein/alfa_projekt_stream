from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd


def transfer_table_with_progress(table_name, target_table):
    # 1. Připojení k lokálnímu Postgresu
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_engine = pg_hook.get_sqlalchemy_engine()

    print(f"--- START PŘENOSU: {table_name} ---", flush=True)

    # Načtení dat do Pandas DataFrame
    df = pd.read_sql(f"SELECT * FROM {table_name}", pg_engine)
    # Snowflake vyžaduje VELKÁ PÍSMENA u názvů sloupců
    df.columns = [x.upper() for x in df.columns]

    total_rows = len(df)
    print(f"INFO: Načteno {total_rows} řádků z Postgresu.", flush=True)

    # 2. Připojení k Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sf_engine = sf_hook.get_sqlalchemy_engine()

    # Nastavení velikosti dávky (chunku)
    chunk_size = 50000
    rows_uploaded = 0
    first_chunk = True

    print(f"INFO: Zahajuji nahrávání do Snowflake (schema RAW)...", flush=True)

    # 3. Nahrávání po částech s logováním progresu
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i: i + chunk_size]

        # První balík tabulku vytvoří (REPLACE), další už jen přidávají (APPEND)
        mode = 'replace' if first_chunk else 'append'

        chunk.to_sql(
            target_table.upper(),
            sf_engine,
            schema='RAW',
            if_exists=mode,
            index=False,
            method='multi',  # Zrychluje nahrávání více řádků najednou
            chunksize=10000
        )

        first_chunk = False
        rows_uploaded += len(chunk)
        print(
            f"PROGRES: {target_table} -> nahráno {rows_uploaded} z {total_rows} řádků...", flush=True)

    print(
        f"✅ HOTOVO: Tabulka {target_table} je kompletně v Snowflake.", flush=True)


# --- DEFINICE DAGU ---
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

    # Mapování tabulek: Zdroj v PG -> Cíl v Snowflake RAW
    tables_to_move = {
        'dim_employees': 'DIM_EMPLOYEES',
        'fact_payroll': 'FACT_PAYROLL',
        'dim_products': 'DIM_PRODUCTS',
        'fact_traffic': 'FACT_TRAFFIC',
        'fact_orders': 'FACT_ORDERS'
    }

    for pg_table, sf_table in tables_to_move.items():
        PythonOperator(
            task_id=f'move_{pg_table}',
            python_callable=transfer_table_with_progress,
            op_kwargs={'table_name': pg_table, 'target_table': sf_table}
        )
