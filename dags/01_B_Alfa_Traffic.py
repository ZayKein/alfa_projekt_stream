from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import os


def generate_traffic():
    path = "/opt/airflow/data"
    df_p = pd.read_csv(f"{path}/products_raw.test.csv")

    df_p['weight'] = df_p['base_price'].apply(
        lambda x: 5 if x < 5000 else (3 if x < 20000 else 1))
    product_pool = df_p['product_id'].tolist()
    product_weights = df_p['weight'].tolist()

    # Roky od 2021 do dnes
    growth = {2021: (100, 200), 2022: (200, 400), 2023: (400, 700), 2024: (
        700, 1200), 2025: (1200, 2000), 2026: (1500, 2500)}
    traffic_data, oid_counter = [], 800001

    for year, (min_c, max_c) in growth.items():
        curr = datetime(year, 1, 1)
        end_y = datetime(year, 12, 31) if year < 2026 else datetime.now()
        print(f"Generuji rok {year}...", flush=True)

        while curr <= end_y:
            season_mult = 3.5 if curr.month == 12 else (
                2.5 if curr.month == 11 else 1.0)
            daily_clicks = int(random.randint(min_c, max_c) * season_mult)

            for _ in range(daily_clicks):
                # Peak Hours (10-14h a 18-22h)
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

    pd.DataFrame(traffic_data, columns=['event_time', 'traffic_id', 'item_in_cart', 'product_id', 'order_id']).to_csv(
        f"{path}/traffic_raw.test.csv", index=False)
    print(f"FINISH: CSV uloženo ({len(traffic_data)} řádků).", flush=True)


with DAG('01_B_Traffic_CSV', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    PythonOperator(task_id='gen_traffic', python_callable=generate_traffic)
