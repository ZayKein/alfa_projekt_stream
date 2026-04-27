from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import random
import os


def generate_products():
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    file_path = f"{path}/products_raw.test.csv"
    target_count = 300

    # --- POJISTKA PROTI PŘEPISU ---
    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        if len(df_existing) >= target_count:
            print(
                f"INFO: Katalog produktů již existuje ({len(df_existing)} položek). Přeskakuji generování.")
            return

    print(f"INFO: Generuji fixní katalog {target_count} produktů...")

    # Seed zajišťuje, že product_id 1 bude vždy ten samý model se stejnou cenou
    random.seed(42)

    models_dict = {
        'iPhony': ['iPhone 13', 'iPhone 14', 'iPhone 15', 'iPhone 15 Pro Max'],
        'Android telefony': ['Samsung Galaxy S24', 'Samsung A54', 'Xiaomi 14', 'Google Pixel 8', 'Motorola Edge', 'Nothing Phone'],
        'Příslušenství': ['AirPods Pro', 'Pouzdro MagSafe', 'Nabíječka 20W', 'Samsung Galaxy Buds', 'Powerbanka 20k', 'Kabel USB-C'],
        'MacBooky': ['MacBook Air M2', 'MacBook Air M3', 'MacBook Pro 14"', 'MacBook Pro 16"'],
        'Herní notebooky': ['ASUS ROG Zephyrus', 'Lenovo Legion 5', 'MSI Katana', 'Acer Predator', 'Razer Blade'],
        'Kancelářské': ['HP Pavilion', 'Dell Latitude', 'Lenovo ThinkPad', 'ASUS Vivobook', 'Acer Swift'],
        'Konzole': ['PlayStation 5', 'Xbox Series X', 'Nintendo Switch OLED', 'Steam Deck', 'ASUS ROG Ally'],
        'Hry': ['Elden Ring', 'FIFA 24', 'Spider-Man 2', 'God of War Ragnarok', 'Starfield', 'Tekken 8'],
        'Parfémy': ['Dior Sauvage', 'Chanel No. 5', 'Hugo Boss Bottled', 'Armani Acqua di Gio', 'Versace Eros'],
        'Elektro pro krásu': ['Dyson Airwrap', 'Philips Lumea', 'Braun Series 9', 'Oral-B iO', 'Kulma Rowenta'],
        'Bílé elektro': ['Lednice Samsung', 'Pračka LG DirectDrive', 'Myčka Bosch Serie 4', 'Sušička Beko', 'Sporák Mora'],
        'Domácí elektro': ['OLED TV LG', 'QLED TV Samsung', 'Soundbar Sony', 'Projektor Epson', 'Apple TV 4K']
    }

    cat_mapping = {
        'Mobily': ['iPhony', 'Android telefony', 'Příslušenství'],
        'Laptops': ['MacBooky', 'Herní notebooky', 'Kancelářské'],
        'Gaming': ['Konzole', 'Hry'],
        'Beauty': ['Parfémy', 'Elektro pro krásu'],
        'Bílé zboží': ['Bílé elektro'],
        'Elektronika': ['Domácí elektro']
    }

    prods = []
    for i in range(1, target_count + 1):
        m_cat = random.choice(list(cat_mapping.keys()))
        s_cat = random.choice(cat_mapping[m_cat])
        base_model_name = random.choice(models_dict[s_cat])
        model_full_name = f"{base_model_name} (v.{random.randint(1, 9)})"

        if s_cat in ['iPhony', 'MacBooky', 'Herní notebooky', 'Domácí elektro']:
            price = random.randint(25000, 75000)
        elif s_cat in ['Android telefony', 'Konzole', 'Bílé elektro', 'Kancelářské']:
            price = random.randint(8000, 24000)
        else:
            price = random.randint(500, 6000)

        prods.append([i, model_full_name, m_cat, s_cat,
                     price, round(price * 0.75)])

    df_final = pd.DataFrame(prods, columns=[
                            'product_id', 'name', 'category', 'subcategory', 'base_price', 'unit_cost'])
    df_final.to_csv(file_path, index=False)
    print(f"✅ HOTOVO: Katalog {target_count} produktů uložen do {file_path}")


# --- DEFINICE DAGU ---
default_args = {
    'owner': 'alfa_stream',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    '01_A_Products_Test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['alfa_stream']
) as dag:

    PythonOperator(
        task_id='gen_prods_test',
        python_callable=generate_products
    )
