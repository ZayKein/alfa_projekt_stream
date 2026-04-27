from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import random
import os


def generate_orders_from_traffic():
    path = "/opt/airflow/data"
    traffic_file = f"{path}/traffic_raw.test.csv"
    orders_file = f"{path}/orders_raw.test.csv"
    products_file = f"{path}/products_raw.test.csv"
    employees_file = f"{path}/employees_master.csv"

    print(f"START: Načítám traffic data z {traffic_file}", flush=True)
    if not os.path.exists(traffic_file):
        print("CHYBA: Traffic file neexistuje! Spusť nejdříve 01_B.", flush=True)
        return

    # 1. Načtení dat
    df_t = pd.read_csv(traffic_file)
    df_new = df_t[df_t['order_id'].notnull()].copy()

    total_to_process = len(df_new)
    if total_to_process == 0:
        print("INFO: V trafficu nejsou žádné objednávky (order_id IS NULL).", flush=True)
        return

    print(
        f"INFO: Načteno {total_to_process} objednávek ke zpracování.", flush=True)

    # Načtení dimenzí
    df_p = pd.read_csv(products_file).set_index('product_id')
    df_e = pd.read_csv(employees_file, parse_dates=['hire_date'])
    # Převedeme exit_date na datetime pro rychlejší porovnávání
    df_e['exit_date'] = pd.to_datetime(df_e['exit_date'])

    new_orders_list = []
    counter = 0

    print("INFO: Spouštím byznys logiku (prodejci a služby)...", flush=True)

    # 2. Hlavní cyklus zpracování
    for _, row in df_new.iterrows():
        oid = int(row['order_id'])
        pid = int(row['product_id'])
        o_date = row['event_time']
        curr_dt = pd.to_datetime(o_date)

        # Rychlejší filtrování aktivních zaměstnanců
        mask = (df_e['hire_date'] <= curr_dt) & (
            (df_e['exit_date'].isna()) | (df_e['exit_date'] > curr_dt))
        active_staff = df_e[mask]

        emp_id, st, sp = 0, 'None', 0
        qty = random.randint(1, 2)

        # Logika doplňkových služeb
        if not active_staff.empty and random.random() < 0.45:
            # Vybereme náhodného aktivního prodejce
            e = active_staff.sample(n=1).iloc[0]
            emp_id = int(e['employee_id'])
            yrs = (curr_dt - e['hire_date']).days // 365

            # Pravděpodobnost prodeje služby podle seniority
            chance = 0.35 if yrs >= 3 else (0.20 if yrs >= 1 else 0.10)

            # Služby prodáváme jen pro elektroniku a mobily
            p_cat = df_p.loc[pid, 'category']
            if p_cat in ['Mobily', 'Laptops', 'Gaming', 'Elektronika', 'Bílé zboží'] and random.random() < chance:
                st = random.choice(['Záruka+1', 'Pojištění'])
                sp = round(df_p.loc[pid, 'base_price'] * 0.12)

        new_orders_list.append([oid, pid, qty, st, sp, emp_id, o_date])

        # LOGOVÁNÍ PROGRESE (aby Airflow log "nezamrzl")
        counter += 1
        if counter % 5000 == 0:
            print(
                f"PROGRES: Zpracováno {counter} z {total_to_process} objednávek...", flush=True)

    # 3. Uložení výsledku
    print(f"INFO: Ukládám {len(new_orders_list)} řádků do CSV...", flush=True)
    df_final = pd.DataFrame(new_orders_list, columns=[
                            'order_id', 'product_id', 'quantity', 'service_type', 'service_price', 'employee_id', 'order_date'])
    df_final.to_csv(orders_file, index=False)

    print(f"FINISH: Objednávky úspěšně uloženy do {orders_file}.", flush=True)


# --- AIRFLOW DEFINICE ---
default_args = {
    'owner': 'alfa_projekt',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    '01_C_Orders_Final',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt']
) as dag:

    PythonOperator(
        task_id='gen_orders_csv',
        python_callable=generate_orders_from_traffic
    )
