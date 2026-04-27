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

    # 1. INKREMENTÁLNÍ LOGIKA: Zjistíme, co už máme hotové
    last_processed_oid = 0
    if os.path.exists(orders_file):
        df_existing = pd.read_csv(orders_file)
        if not df_existing.empty:
            last_processed_oid = df_existing['order_id'].max()

    print(
        f"INFO: Poslední zpracované Order ID v cílovém souboru: {last_processed_oid}", flush=True)

    # 2. Načtení trafficu a filtrace pouze NOVÝCH záznamů
    df_t = pd.read_csv(traffic_file)

    # --- OPRAVA: Nejdříve filtrujeme notnull, pak převádíme na int ---
    # Odstraníme řádky bez order_id
    df_new = df_t[df_t['order_id'].notnull()].copy()
    # Převedeme order_id na int (aby šlo porovnávat)
    df_new['order_id'] = df_new['order_id'].astype(int)
    # Vybereme jen ty, které jsou vyšší než naše poslední ID
    df_new = df_new[df_new['order_id'] > last_processed_oid]

    total_to_process = len(df_new)
    if total_to_process == 0:
        print("INFO: Žádné nové objednávky k vyřízení. Vše je aktuální.", flush=True)
        return

    print(
        f"INFO: Nalezeno {total_to_process} nových objednávek ke zpracování.", flush=True)

    # Načtení dimenzí
    df_p = pd.read_csv(products_file).set_index('product_id')
    df_e = pd.read_csv(employees_file, parse_dates=['hire_date'])
    df_e['exit_date'] = pd.to_datetime(df_e['exit_date'])

    new_orders_list = []
    counter = 0

    print("INFO: Spouštím byznys logiku (prodejci a služby)...", flush=True)

    # 3. Hlavní cyklus zpracování (jen pro nové řádky)
    for _, row in df_new.iterrows():
        oid = int(row['order_id'])
        pid = int(row['product_id'])
        o_date = row['event_time']
        curr_dt = pd.to_datetime(o_date)

        # Filtrování aktivních zaměstnanců pro daný čas objednávky
        mask = (df_e['hire_date'] <= curr_dt) & (
            (df_e['exit_date'].isna()) | (df_e['exit_date'] > curr_dt))
        active_staff = df_e[mask]

        emp_id, st, sp = 0, 'None', 0
        qty = random.randint(1, 2)

        if not active_staff.empty and random.random() < 0.45:
            # Vybereme náhodného aktivního prodejce
            e = active_staff.sample(n=1).iloc[0]
            emp_id = int(e['employee_id'])
            yrs = (curr_dt - e['hire_date']).days // 365

            chance = 0.35 if yrs >= 3 else (0.20 if yrs >= 1 else 0.10)

            p_cat = df_p.loc[pid, 'category']
            if p_cat in ['Mobily', 'Laptops', 'Gaming', 'Elektronika', 'Bílé zboží'] and random.random() < chance:
                st = random.choice(['Záruka+1', 'Pojištění'])
                sp = round(df_p.loc[pid, 'base_price'] * 0.12)

        new_orders_list.append([oid, pid, qty, st, sp, emp_id, o_date])

        counter += 1
        if counter % 5000 == 0:
            print(
                f"PROGRES: Zpracováno {counter} z {total_to_process} nových objednávek...", flush=True)

    # 4. Uložení výsledku (APPEND MÓD)
    df_final = pd.DataFrame(new_orders_list, columns=[
                            'order_id', 'product_id', 'quantity', 'service_type', 'service_price', 'employee_id', 'order_date'])

    file_exists = os.path.exists(orders_file)
    print(
        f"INFO: Přidávám {len(df_final)} nových řádků do {orders_file}...", flush=True)

    # mode='a' = append, header=False pokud už soubor existuje
    df_final.to_csv(orders_file, mode='a', index=False, header=not file_exists)

    print(f"FINISH: Inkrementální dávka úspěšně uložena.", flush=True)


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
