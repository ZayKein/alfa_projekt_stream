from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


def upload_to_pg(table_name, file_name):
    path = f"/opt/airflow/data/{file_name}"
    if not os.path.exists(path):
        print(f"⚠️ Soubor {file_name} nenalezen, přeskakuji.")
        return

    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(path)

    # Automatický převod datových sloupců, aby v PG byly správné typy
    date_cols = [
        col for col in df.columns if 'date' in col or 'month_year' in col or 'event_time' in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # replace = přepíše tabulku novými daty (včetně tvých sezónních peaků)
    df.to_sql(table_name, hook.get_sqlalchemy_engine(),
              if_exists='replace', index=False)
    print(f"✅ Tabulka {table_name} nahrána. Celkem {len(df)} řádků.")


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

    # 1. HR Data
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

    # 2. Katalog produktů
    load_products = PythonOperator(
        task_id='load_products',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'dim_products',
                   'file_name': 'products_raw.test.csv'}
    )

    # 3. Tvůj nový Traffic se sezónností
    load_traffic = PythonOperator(
        task_id='load_traffic',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'fact_traffic',
                   'file_name': 'traffic_raw.test.csv'}
    )

    # 4. Finální Orders se službami a prodejci
    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=upload_to_pg,
        op_kwargs={'table_name': 'fact_orders',
                   'file_name': 'orders_raw.test.csv'}
    )

    # Spustíme vše paralelně, ať je to hned
    [load_employees, load_payroll, load_products, load_traffic, load_orders]
