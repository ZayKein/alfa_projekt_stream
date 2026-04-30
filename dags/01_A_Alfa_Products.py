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

    if os.path.exists(file_path):
        df_existing = pd.read_csv(file_path)
        if len(df_existing) >= target_count:
            print(f"INFO: Catalog exists ({len(df_existing)} items). Skipping.")
            return

    print(f"INFO: Generating catalog of {target_count} products...")
    random.seed(42)

    # catalog[category][subcategory][brand] = [model, ...]
    catalog = {
        'Mobile': {
            'Smartphones - iPhone': {
                'Apple': [
                    'iPhone 13', 'iPhone 13 Pro', 'iPhone 14', 'iPhone 14 Pro',
                    'iPhone 15', 'iPhone 15 Pro', 'iPhone 15 Pro Max',
                    'iPhone 16', 'iPhone 16 Pro', 'iPhone 16 Pro Max', 'iPhone SE',
                ],
            },
            'Smartphones - Android': {
                'Samsung': ['Galaxy S24', 'Galaxy S24+', 'Galaxy S24 Ultra', 'Galaxy A54', 'Galaxy A35'],
                'Google':  ['Pixel 8', 'Pixel 8 Pro', 'Pixel 9', 'Pixel 9 Pro', 'Pixel 8a'],
                'Xiaomi':  ['14', '14 Pro', '14 Ultra', 'Redmi Note 13 Pro', 'POCO F6 Pro'],
                'OnePlus': ['12', '12R', 'Nord 4', 'Nord CE 4', 'Open'],
                'Sony':    ['Xperia 1 VI', 'Xperia 5 VI', 'Xperia 10 VI', 'Xperia 1 V', 'Xperia 10 V'],
            },
            'Phone Accessories': {
                'Apple':   ['AirPods Pro 2', 'AirPods 4', 'AirPods Max', 'MagSafe Charger 15W', 'USB-C to Lightning Cable 1m'],
                'Samsung': ['Galaxy Buds 2 Pro', 'Galaxy Buds FE', 'Galaxy Buds Live', '45W USB-C Charger', 'Wireless Charger Duo'],
                'Anker':   ['PowerCore 20000', '737 Power Bank', '67W Nano Pro Charger', 'PowerLine III USB-C 2m', '543 USB-C Hub 6-in-1'],
                'Spigen':  ['MagSafe Armor Shell Case', 'Ultra Hybrid Case', 'Crystal Flex Case', 'Rugged Armor Case', 'Tough Armor Case'],
                'Belkin':  ['MagSafe Wireless Charger 15W', '3-in-1 MagSafe Charger', 'USB-C Hub 7-in-1', 'ScreenForce Pro Protector', 'Boost Charge Pro 25W'],
            },
        },
        'Laptops': {
            'Premium Laptops': {
                'Apple':     ['MacBook Air 13" M2', 'MacBook Air 15" M2', 'MacBook Air 13" M3', 'MacBook Air 15" M3', 'MacBook Pro 14" M3', 'MacBook Pro 16" M3', 'MacBook Pro 14" M3 Pro', 'MacBook Pro 16" M3 Max'],
                'Dell':      ['XPS 13 Plus 9320', 'XPS 15 9530', 'XPS 17 9730', 'Precision 5490', 'XPS 13 9315'],
                'Microsoft': ['Surface Pro 9', 'Surface Laptop 5 13"', 'Surface Laptop 5 15"', 'Surface Laptop Studio 2', 'Surface Pro 10'],
                'LG':        ['Gram 14 2024', 'Gram 16 2024', 'Gram 17 2024', 'Gram 2-in-1 14"', 'Gram Style 16"'],
                'ASUS':      ['ZenBook Pro 14 OLED', 'ZenBook S 13 OLED', 'ProArt Studiobook Pro 16', 'ZenBook 14X OLED', 'ExpertBook B9 OLED'],
            },
            'Gaming Laptops': {
                'ASUS':   ['ROG Zephyrus G14 2024', 'ROG Zephyrus G16', 'ROG Strix G16', 'TUF Gaming F15', 'TUF Gaming A15'],
                'Lenovo': ['Legion 5 Pro Gen 8', 'Legion 7i Gen 9', 'Legion Pro 7i Gen 9', 'Legion Slim 5i', 'LOQ 15IAX9I'],
                'MSI':    ['Katana 15 B13V', 'Raider GE78 HX', 'Titan GT77 HX', 'Stealth 16 Studio', 'Crosshair 17 HX'],
                'Acer':   ['Predator Helios 16', 'Predator Helios Neo 16', 'Nitro 5 AN515', 'Nitro 16 AN16', 'Predator Triton Neo 16'],
                'Razer':  ['Blade 14 2024', 'Blade 15 2024', 'Blade 16 2024', 'Blade 18 2024'],
                'HP':     ['Omen 16 2024', 'Omen 17 2024', 'Victus 15 2024', 'Victus 16 2024', 'Omen Transcend 16'],
            },
            'Office Laptops': {
                'Lenovo': ['ThinkPad L14 Gen 4', 'ThinkPad X1 Carbon Gen 12', 'ThinkPad T14s Gen 5', 'IdeaPad 5 Pro 16', 'ThinkBook 14 G6'],
                'HP':     ['EliteBook 840 G11', 'ProBook 450 G10', 'HP 255 G10', 'HP Pavilion 15', 'HP 14s-fq2xxx'],
                'Dell':   ['Latitude 5540', 'Latitude 7440', 'Inspiron 15 3535', 'Vostro 3520', 'Inspiron 14 5440'],
                'ASUS':   ['Vivobook 16X M3604', 'ExpertBook B1 B1502', 'Vivobook Pro 15 OLED', 'Chromebook Flip C436', 'Vivobook S 15 OLED'],
                'Acer':   ['Swift 3 SF314-56', 'Aspire 5 A515-58M', 'Extensa 15 EX215-55', 'Swift Go 14 SFG14-71', 'Swift X 14 SFX14-72G'],
            },
        },
        'Gaming': {
            'Consoles': {
                'Sony':      ['PlayStation 5 Standard', 'PlayStation 5 Slim', 'PlayStation 5 Digital Edition', 'PlayStation 5 Slim Digital'],
                'Microsoft': ['Xbox Series X 1TB', 'Xbox Series S 512GB', 'Xbox Series S 1TB', 'Xbox Series X Special Edition'],
                'Nintendo':  ['Switch OLED White', 'Switch OLED Splatoon Edition', 'Switch Lite', 'Switch V2'],
                'Valve':     ['Steam Deck 512GB LCD', 'Steam Deck 1TB OLED', 'Steam Deck 512GB OLED'],
                'ASUS':      ['ROG Ally RC71L', 'ROG Ally X RC72LA', 'ROG Ally Z1 Extreme'],
                'Lenovo':    ['Legion Go 512GB', 'Legion Go S 256GB', 'Legion Go 1TB'],
            },
            'Games': {
                'EA':         ['FIFA 25', 'FC 24', 'Dead Space Remake', 'Need for Speed Unbound', 'The Sims 4 City Living'],
                'Sony':       ["Marvel's Spider-Man 2", 'God of War Ragnarok', 'Horizon Forbidden West', 'Ghost of Tsushima DC', 'Gran Turismo 7'],
                'Microsoft':  ['Starfield', 'Forza Horizon 5', 'Halo Infinite', 'Fable 2024', 'Microsoft Flight Simulator 2024'],
                'CD Projekt': ['Cyberpunk 2077 Phantom Liberty', 'The Witcher 3 Complete Edition', 'Cyberpunk 2077', 'Gwent Card Game Bundle'],
                'Bandai Namco': ['Elden Ring', 'Tekken 8', 'Dark Souls III', "Dragon's Dogma 2", 'Tales of Arise'],
                'Ubisoft':    ["Assassin's Creed Mirage", 'Far Cry 6', 'Rainbow Six Siege Y9', 'The Crew Motorfest', "Prince of Persia: The Lost Crown"],
            },
            'Gaming Accessories': {
                'Sony':       ['DualSense White Controller', 'DualSense Edge Controller', 'Pulse 3D Wireless Headset', 'PlayStation 5 HD Camera', 'DualSense Midnight Black'],
                'Microsoft':  ['Xbox Wireless Controller Carbon', 'Xbox Elite Controller Series 2', 'Xbox Wireless Headset', 'Xbox Play & Charge Kit', 'Xbox Controller Shock Blue'],
                'Razer':      ['Kraken V3 HyperSense', 'BlackWidow V4 Pro', 'DeathAdder V3 Pro', 'Wolverine V3 Pro', 'Viper V3 Pro'],
                'Logitech':   ['G502 X Plus', 'G Pro X Superlight 2', 'G915 TKL Keyboard', 'G733 Wireless Headset', 'G923 Racing Wheel'],
                'SteelSeries': ['Arctis Nova Pro Wireless', 'Arctis Nova 7', 'Apex Pro TKL 2023', 'Prime+ Gaming Mouse', 'Arctis 1 Wireless'],
                'HyperX':     ['Cloud III Wireless', 'CloudX Flight Wireless', 'Alloy Origins Core', 'Pulsefire Haste 2 Wireless', 'QuadCast S Microphone'],
            },
        },
        'Beauty': {
            'Fragrances': {
                'Dior':      ['Sauvage EDP 100ml', 'Miss Dior EDP 100ml', "J'adore EDP 75ml", 'Fahrenheit EDT 100ml', 'Dior Homme Intense EDP'],
                'Chanel':    ['No. 5 EDP 100ml', 'Bleu de Chanel EDP 100ml', 'Coco Mademoiselle EDP 100ml', 'Chance EDP 100ml', 'Allure Homme Sport EDT 100ml'],
                'Armani':    ['Acqua di Gio EDP 75ml', 'Si EDP 100ml', 'Code EDP 75ml', 'My Way EDP 50ml', 'Stronger With You EDP 100ml'],
                'Versace':   ['Eros EDT 100ml', 'Bright Crystal EDT 90ml', 'Dylan Blue EDT 100ml', 'Dylan Turquoise EDT 100ml', 'Yellow Diamond EDT 90ml'],
                'Hugo Boss': ['Boss Bottled EDT 100ml', 'Hugo Man EDT 125ml', 'The Scent EDP 100ml', 'Boss Alive EDP 80ml', 'Boss Intense EDP 50ml'],
                'YSL':       ['Black Opium EDP 90ml', 'Libre EDP 90ml', 'Y EDP 100ml', 'Mon Paris EDP 90ml', "L'Homme EDT 100ml"],
            },
            'Hair Care': {
                'Dyson':    ['Airwrap Multi-Styler Complete', 'Supersonic HD15 Hair Dryer', 'Corrale HS07 Straightener', 'Airwrap i.d. Complete', 'Airstrait HT02 Straightener'],
                'GHD':      ['Gold Professional Straightener', 'Platinum+ Straightener', 'Helios Professional Dryer', 'Rise Hot Brush', 'Curve Classic Curl Wand'],
                'BaByliss': ['Pro Titanium 235 Straightener', 'Hydro-Fusion Hair Dryer', 'MiraCurl Professional', 'Smooth Pro 235 Straightener', 'Curl Styler Luxe'],
                'Philips':  ['StyleCare 7000 BHH880', 'StyleCare Essential HP8232', 'ThermoProtect BHD350', 'MoistureProtect BHD630', 'DryCare Pro BHD350'],
                'Remington': ['Wet2Straight Pro S7970', 'Silk Straightener S9600', 'Curl & Straight Confidence', 'Hydraluxe Pro Straightener', 'Air3D Dryer D5408'],
            },
            'Grooming': {
                'Braun':    ['Series 9 Pro+ 9577cc', 'Series 8 8467cc', 'Series 7 7085cc', 'Silk-Expert Pro 5 IPL', 'CruZer Face 6 FPC5'],
                'Philips':  ['Norelco 9900 Prestige S9986', 'Lumea 9000 BRI958', 'OneBlade Pro 360 QP6550', 'Multigroom MG7940', 'Norelco 5000 S5585'],
                'Oral-B':   ['iO Series 9 Toothbrush', 'iO Series 8 Toothbrush', 'Smart 4 4000 Toothbrush', 'Genius X 20000 Toothbrush', 'Pro 3 3000 Toothbrush'],
                'Panasonic': ['ER-GP80 Hair Clipper', 'ES-LV97 Arc 5 Shaver', 'ER-GB62 Beard Trimmer', 'ES-RT77 Wet & Dry Shaver', 'ER-PA10 Premium Trimmer'],
                'Wahl':     ['Magic Clip Cordless', 'Sterling 4 Clipper', 'Elite Pro Clipper', 'Lithium Ion Plus Trimmer', 'Chromstyle Pro Clipper'],
            },
        },
        'Home Appliances': {
            'Refrigerators': {
                'Samsung':  ['RF65A977FSR French Door', 'RB38A7B5E22 Combi', 'RS6GA8522S9 Side-by-Side', 'RB34C7B5E22 Combi', 'RZ32C7AE5AP Upright'],
                'LG':       ['GSXV91MCAE InstaView Door-in-Door', 'GBV7180CPY Combi', 'GSJ761PZXV Side-by-Side', 'GBB92STBAP Combi', 'GSXV80PZAD InstaView'],
                'Bosch':    ['KGN86AIDR Serie 6 Combi', 'KAN93VBDT Side-by-Side', 'KGF56PIDP Serie 8', 'KGN56AIDA Combi', 'KGE36AWCA Freestanding'],
                'Miele':    ['KFN 7734 E Side-by-Side', 'K 7745 D Upright', 'KFN 4795 DD Combi', 'K 7304 D Freestanding', 'KS 4783 DD Undercounter'],
                'Liebherr': ['CBNef 7723 Biofresh', 'CNef 5715 Biofresh', 'CBNbdb 5758 Biofresh Plus', 'SRBsdd 5260 Monolith', 'CBef 7251 Biofresh'],
            },
            'Washing Machines': {
                'LG':       ['V9 F6V909WTSA DirectDrive', 'F4WV509S1E AI DD', 'F4T209WSTH Steam+', 'V7 F2T7008S3W DirectDrive', 'F4WR711S2HT TurboWash 360'],
                'Samsung':  ['WW90T986DSH EcoBubble AI', 'WW11BB944DGHS5 QuickDrive', 'WW90T634DHE EcoBubble', 'WW80T634DHH EcoBubble', 'WW11DG6B85AEU5 Bespoke'],
                'Bosch':    ['WAX32M40 Serie 8 i-Dos', 'WGB244040 Serie 8', 'WAX28EH0 Serie 8', 'WGG244F0 Serie 6', 'WAN28282GB Serie 4'],
                'Miele':    ['WCG660 WCS W1 PowerWash', 'WEA035 WCS W1', 'WCR870 WPS W1 TDos', 'WWF064WPS', 'WDD035 WPS Classic'],
                'Siemens':  ['WG56B2A40 iQ700 i-Dos', 'WM14UP70 iQ800', 'WG44G2090 iQ500', 'WM6HXM00 iQ700', 'WI14W542EU iQ700 Built-in'],
            },
            'Kitchen Appliances': {
                'Bosch':   ['SMV8YCX03E Serie 8 Dishwasher', 'HBG7721B1 Serie 8 Oven', 'PKF801B29E Serie 8 Induction Hob', 'SMD8YCX02E Serie 8 Dishwasher', 'HMH87MY61S Serie 8 Microwave'],
                'Siemens': ['SN65ZX49CE iQ500 Dishwasher', 'HB676GBS1 iQ700 Oven', 'EX875KYW1E iQ700 Induction', 'SX75ZX07ME iQ700 Dishwasher', 'HM876G0B1 iQ700 Microwave'],
                'Miele':   ['G 7985 SCVi AutoDos Dishwasher', 'H 7860 BPX ArtLine Oven', 'KM 7574 FL Induction Hob', 'G 7965 SCVi XXL Dishwasher', 'M 7244 TC Microwave'],
                'Neff':    ['Slide&Hide B57CR22N0 Oven', 'N70 B57CS22H0 Oven', 'T48FD23X2 Induction Hob', 'S255ECI16E Dishwasher', 'C17MS32N0 Microwave'],
                'AEG':     ['FSE83700P AutoDose Dishwasher', 'BPE745380M SteamPro Oven', 'HEB95410FB Induction Hob', 'FSB63719P Dishwasher', 'MBB1756SEM Microwave'],
            },
        },
        'Consumer Electronics': {
            'TVs': {
                'LG':       ['OLED C3 55"', 'OLED C3 65"', 'OLED C4 55"', 'OLED G3 65"', 'QNED85 75"'],
                'Samsung':  ['Neo QLED QN85C 55"', 'Neo QLED QN95C 65"', 'QLED Q80C 55"', 'Crystal UHD TU8500 65"', 'The Frame LS03B 65"'],
                'Sony':     ['Bravia XR A80L OLED 55"', 'Bravia XR A80L OLED 65"', 'Bravia XR X95L 65"', 'Bravia XR A90L OLED 83"', 'KD-55X80L'],
                'Philips':  ['OLED+908 65"', 'The One 55PUS8808', '75PUS8808 4K', 'OLED 706 48"', '65PUS8808 4K LED'],
                'Panasonic': ['TX-65MZ2000E OLED', 'TX-55MZ1500E OLED', 'TX-65LX800E 4K LED', 'TX-55MX800E', 'TX-65MX700E'],
            },
            'Sound Systems': {
                'Sonos':   ['Arc Premium Soundbar', 'Beam Gen 2 Soundbar', 'Sub Gen 3', 'Ray Compact Soundbar', 'Move 2 Portable Speaker'],
                'Samsung': ['HW-Q990C 11.1.4ch', 'HW-Q800C 5.1.2ch', 'HW-S800B 3.1.2ch', 'HW-Q600C 3.1.2ch', 'HW-B650 3.1ch'],
                'Sony':    ['HT-A7000 7.1.2ch', 'HT-A5000 5.1.2ch', 'HT-S2000 3.1ch', 'HT-A9 4.0.4ch', 'SA-SW5 Wireless Subwoofer'],
                'Bose':    ['Smart Soundbar 900', 'Smart Soundbar 600', 'Smart Ultra Soundbar', 'Smart Soundbar 300', 'Bass Module 700'],
                'Denon':   ['AVR-X3800H 9.4ch Receiver', 'AVR-X1800H 7.2ch Receiver', 'AVR-X4800H 9.4ch Receiver', 'DHT-S517 Soundbar', 'AVR-S970H 7.2ch Receiver'],
            },
            'Smart Home & Coffee': {
                'Apple':     ["Apple TV 4K 3rd Gen Wi-Fi", 'HomePod 2nd Gen Midnight', 'HomePod mini Space Gray', 'HomePod mini Yellow', "Apple TV 4K 3rd Gen Wi-Fi+Ethernet"],
                'Philips':   ['Hue Starter Kit E27 4 bulbs', 'Hue Gradient Lightstrip Plus 2m', 'Hue Play Light Bar Double Pack', 'Hue Go Portable Table Lamp', 'Hue Filament Globe Bulb G125'],
                "De'Longhi": ['Dinamica Plus ECAM370.95.T', 'Magnifica Start ECAM220.60.W', 'Eletta Explore ECAM450.65.S', 'Primadonna Soul ECAM610.74.MB', 'Nespresso Genio S Plus ENV155T'],
                'Jura':      ['E8 Chrome Coffee Machine', 'S8 Moonlight Silver', 'J8 Twin Coffee Machine', 'ENA 4 Metropolitan Black', 'A1 Piano White'],
                'Nespresso': ['Vertuo Next GCV1 Chrome', 'Vertuo Pop GDV2 White', 'Vertuo Creatista GCV1', 'Essenza Mini C30 White', 'Lattissima One F121'],
            },
        },
    }

    pricing = {
        'Smartphones - iPhone':  (25000, 75000, 0.10, 0.20),
        'Premium Laptops':       (28000, 85000, 0.10, 0.20),
        'Gaming Laptops':        (22000, 65000, 0.12, 0.22),
        'TVs':                   (12000, 75000, 0.12, 0.22),
        'Kitchen Appliances':    (8000,  45000, 0.15, 0.25),
        'Smartphones - Android': (6000,  28000, 0.18, 0.30),
        'Office Laptops':        (7000,  26000, 0.18, 0.30),
        'Consoles':              (7000,  20000, 0.18, 0.30),
        'Refrigerators':         (8000,  32000, 0.18, 0.28),
        'Washing Machines':      (5000,  22000, 0.18, 0.28),
        'Sound Systems':         (4000,  32000, 0.18, 0.30),
        'Hair Care':             (2000,  18000, 0.25, 0.42),
        'Smart Home & Coffee':   (1500,  28000, 0.25, 0.42),
        'Phone Accessories':     (500,   5000,  0.35, 0.60),
        'Games':                 (800,   2000,  0.40, 0.60),
        'Gaming Accessories':    (500,   7000,  0.35, 0.55),
        'Fragrances':            (1000,  6500,  0.45, 0.65),
        'Grooming':              (800,   9000,  0.30, 0.50),
    }

    prods = []
    for i in range(1, target_count + 1):
        m_cat   = random.choice(list(catalog.keys()))
        s_cat   = random.choice(list(catalog[m_cat].keys()))
        brand   = random.choice(list(catalog[m_cat][s_cat].keys()))
        model   = random.choice(catalog[m_cat][s_cat][brand])
        name    = f"{brand} {model}"

        p_min, p_max, m_min, m_max = pricing[s_cat]
        price      = random.randint(p_min, p_max)
        margin_pct = random.uniform(m_min, m_max)

        prods.append([i, name, m_cat, s_cat, brand, price, round(price * (1 - margin_pct))])

    df_final = pd.DataFrame(prods, columns=[
        'product_id', 'name', 'category', 'subcategory', 'brand', 'base_price', 'unit_cost'
    ])
    df_final.to_csv(file_path, index=False)
    print(f"DONE: {target_count} products saved to {file_path}")


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
