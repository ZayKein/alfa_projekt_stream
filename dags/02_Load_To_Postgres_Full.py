from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


def upload_to_pg(table_name, file_name, incremental=False, date_col=None):
    path = f"/opt/airflow/data/{file_name}"
    if not os.path.exists(path):
        print(f"⚠️ Soubor {file_name} nenalezen, přeskakuji.")
        return

    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(path)

    # 1. Převod datových sloupců
    date_cols = [
        col for col in df.columns if 'date' in col or 'month_year' in col or 'event_time' in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 2. Inkrementální logika
    if incremental and date_col:
        try:
            # Zjistíme poslední timestamp v databázi
            max_date_query = f"SELECT MAX({date_col}) FROM {table_name}"
            max_date = hook.get_first(max_date_query)[0]

            if max_date:
                max_date = pd.to_datetime(max_date)
                # Vyfiltrujeme pouze novější data z CSV
                df = df[df[date_col] > max_date]

                if df.empty:
                    print(
                        f"ℹ️ Tabulka {table_name}: Žádná nová data k nahrání.")
                    return

                print(
                    f"🚀 Inkrementální nahrávání: Nalezeno {len(df)} nových řádků pro {table_name}.")
                df.to_sql(table_name, hook.get_sqlalchemy_engine(),
                          if_exists='append', index=False)
            else:
                # Pokud je tabulka prázdná, uděláme replace (první naplnění)
                df.to_sql(table_name, hook.get_sqlalchemy_engine(),
                          if_exists='replace', index=False)
        except Exception as e:
            print(
                f"⚠️ Chyba při inkrementálním nahrávání (tabulka pravděpodobně neexistuje): {e}")
            df.to_sql(table_name, hook.get_sqlalchemy_engine(),
                      if_exists='replace', index=False)
    else:
        # Full Refresh pro dimenze (produkty, zaměstnanci)
        df.to_sql(table_name, hook.get_sqlalchemy_engine(),
                  if_exists='replace', index=False)

    print(f"✅ Tabulka {table_name} zpracována.")


default_args = {
    'owner': 'alfa_projekt',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    '02_Load_All_To_Postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. HR Data (Full Refresh - malé tabulky, chceme aktuální stav)
    load_employees = PythonOperator(
        task_id='load_employees',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'dim_employees',
                   'file_name': 'employees_master.csv'}
    )

    load_payroll = PythonOperator(
        task_id='load_payroll',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'fact_payroll',
                   'file_name': 'employees_payroll.csv'}
    )

    # 2. Katalog produktů (Full Refresh)
    load_products = PythonOperator(
        task_id='load_products',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'dim_products',
                   'file_name': 'products_raw.test.csv'}
    )

    # 3. Traffic (INCREMENTAL - klíčové pro výkon)
    load_traffic = PythonOperator(
        task_id='load_traffic',
        python_callable=upload_to_pg,
        op_kwargs={
            'table_name': 'fact_traffic',
            'file_name': 'traffic_raw.test.csv',
            'incremental': True,
            'date_col': 'event_time'
        }
    )

    # 4. Orders (INCREMENTAL)
    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=upload_to_pg,
        op_kwargs={
            'table_name': 'fact_orders',
            'file_name': 'orders_raw.test.csv',
            'incremental': True,
            'date_col': 'order_date'
        }
    )

    [load_employees, load_payroll, load_products, load_traffic, load_orders]
