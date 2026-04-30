from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os


def generate_traffic():
    path = "/opt/airflow/data"
    traffic_file = f"{path}/traffic_raw.test.csv"
    prod_file = f"{path}/products_raw.test.csv"

    # 1. Kontrola katalogu produktů
    df_p = pd.read_csv(prod_file)
    df_p['weight'] = df_p['base_price'].apply(
        lambda x: 5 if x < 5000 else (3 if x < 20000 else 1))
    product_pool = df_p['product_id'].tolist()
    product_weights = df_p['weight'].tolist()

    # 2. Inkrementální logika: Zjistíme, kde jsme skončili
    start_date = datetime(2021, 1, 1)
    oid_counter = 800001

    if os.path.exists(traffic_file):
        df_old = pd.read_csv(traffic_file)
        if not df_old.empty:
            start_date = pd.to_datetime(
                df_old['event_time']).max() + timedelta(seconds=1)
            # Pokračujeme v číslování objednávek od poslední známé
            if 'order_id' in df_old.columns:
                last_oid = df_old['order_id'].max()
                if pd.notnull(last_oid):
                    oid_counter = int(last_oid) + 1
            print(
                f"INFO: Pokračuji v generování od {start_date} s Order ID od {oid_counter}", flush=True)

    end_date = datetime.now()
    if start_date >= end_date:
        print("INFO: Data jsou aktuální, není co generovat.", flush=True)
        return

    # Definice růstu kliků pro období
    growth = {2021: (100, 200), 2022: (200, 400), 2023: (400, 700), 2024: (
        700, 1200), 2025: (1200, 2000), 2026: (1500, 2500)}

    # Full seasonal curve — realistic e-commerce pattern
    month_mult = {
        1: 0.70,   # post-holiday slump
        2: 0.78,   # Valentine's pickup
        3: 0.88,   # spring start
        4: 0.92,
        5: 1.05,   # Mother's Day
        6: 0.88,   # early summer dip
        7: 0.80,   # summer trough
        8: 0.90,   # back-to-school
        9: 1.00,   # back-to-school peak
        10: 1.10,  # pre-holiday buildup
        11: 2.50,  # Black Friday / Cyber Monday
        12: 3.50,  # Christmas
    }

    # Day-of-week multiplier (ISO: 1=Mon … 7=Sun)
    dow_mult = {1: 1.0, 2: 1.05, 3: 1.10, 4: 1.08, 5: 1.15, 6: 1.25, 7: 0.85}

    traffic_data = []
    curr = start_date

    # 3. Hlavní smyčka generování
    while curr <= end_date:
        year_config = growth.get(curr.year, (1500, 2500))
        season_mult = month_mult[curr.month]
        dow = curr.isoweekday()
        event_mult = random.uniform(1.8, 3.0) if random.random() < 0.03 else 1.0
        daily_clicks = int(
            random.randint(year_config[0], year_config[1])
            * season_mult
            * dow_mult[dow]
            * event_mult
        )

        for _ in range(daily_clicks):
            hour = random.choice([random.randint(10, 14), random.randint(
                18, 22)]) if random.random() < 0.75 else random.randint(0, 23)
            t_time = curr.replace(hour=hour, minute=random.randint(0, 59))

            traffic_id = f"T-{random.getrandbits(24)}"
            pid = int(random.choices(product_pool,
                      weights=product_weights, k=1)[0])

            in_cart, final_oid = 'no', None
            if random.random() < random.uniform(0.65, 0.90):
                in_cart = 'yes'
                if random.random() < random.uniform(0.30, 0.50):
                    final_oid = oid_counter
                    oid_counter += 1

            traffic_data.append(
                [t_time.strftime('%Y-%m-%d %H:%M:%S'), traffic_id, in_cart, pid, final_oid])

        curr += timedelta(days=1)

    # 4. Zápis (Append, pokud existuje)
    df_new = pd.DataFrame(traffic_data, columns=[
                          'event_time', 'traffic_id', 'item_in_cart', 'product_id', 'order_id'])
    file_exists = os.path.exists(traffic_file)
    df_new.to_csv(traffic_file, mode='a', index=False, header=not file_exists)

    print(
        f"FINISH: Inkrementální dávka uložena ({len(df_new)} řádků).", flush=True)


with DAG('01_B_Traffic_Final', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    PythonOperator(task_id='gen_traffic', python_callable=generate_traffic)
