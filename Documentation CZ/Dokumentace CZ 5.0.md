# Alfa Stream v5.0 — Technická dokumentace (CZ)

**Autor:** David Urban  
**Verze:** 5.0 (finální)  
**Datum:** 2026-04

---

## O projektu

Alfa Stream je osobní portfolio projekt, který jsem navrhl a postavil celý od základů jako ukázku komplexního end-to-end datového projektu — kombinující **datové inženýrství** a **datovou analytiku** v jedné platformě.

Projekt pokrývá celý datový životní cyklus: simulace zdrojových dat, orchestrace pipeline, cloud warehouse, transformační modelování a analytická vrstva s byznysovými dashboardy.

Platforma simuluje reálný e-commerce provoz: produktový katalog, zaměstnanci, webový provoz a objednávky — vše automaticky protéká vícevrstvou pipeline do Snowflake, kde Power BI dashboardy zobrazují byznysové výstupy.

---

## Architektura

Systém je organizován do čtyř vrstev:

### 1. Generátory dat (Python)
Skripty v Pythonu (Pandas) simulují:
- **Produkty** — statický katalog s kategoriemi a cenami
- **Zaměstnanci** — HR master data s nástupními/výstupními daty
- **Traffic** — inkrementální události návštěvnosti s sezónností a peak-hour logikou
- **Objednávky** — inkrementální generování objednávek napojených na produkty a zaměstnance

### 2. Orchestrace (Apache Airflow)
Master Orchestrátor (DAG 05) sekvenčně spouští všechny kroky:

```
00_HR_Gen → 01A_Products → 01B_Traffic → 01C_Orders
         → 02_Postgres_Load → 03_Snowflake_Sync → 04_dbt_Transform
```

Každý DAG čeká na dokončení předchozího (`wait_for_completion` logika).

### 3. Datové vrstvy (PostgreSQL → Snowflake → dbt)

| Vrstva | Technologie | Popis |
|---|---|---|
| ODS | PostgreSQL (Docker) | Lokální staging před cloudem |
| RAW | Snowflake | Data jako přistála — žádné transformace |
| SILVER | Snowflake + dbt views | Vyčištěná a přejmenovaná data |
| GOLD | Snowflake + dbt incremental tables | Byznys dimenze, fakta a mart agregace |

### 4. Analytická vrstva (Power BI)

> **[PLACEHOLDER — bude doplněno po dokončení Power BI reportů]**
>
> Tato sekce bude obsahovat:
> - Popis kompozitního Power BI modelu (Import + DirectQuery)
> - Přehled pěti dashboard stránek a jejich byznysových otázek
> - DAX metriky a jejich interpretaci
> - Screenshot ukázky dashboardů

---

## Delta Load (inkrementální zpracování)

Všechny vrstvy zpracovávají pouze nové přírůstky:
- Generátory produkují pouze nové řádky od posledního běhu
- Postgres → Snowflake přenos kopíruje pouze nové záznamy
- dbt modely používají `incremental` materializaci s `unique_key` merge strategií
- Nejnovější měsíc je vždy znovu zpracován kvůli průběžným datům (`>= MAX(period_column)`)

---

## Datový model — GOLD vrstva

### Dimenze (Import v Power BI)

| Model | Grain | Klíčové sloupce |
|---|---|---|
| `dim_date` | jeden řádek na den (2023–2031) | `date_day`, `year`, `month`, `quarter`, `is_weekend` |
| `dim_products_gold` | jeden řádek na produkt | `product_id`, `name`, `category`, `subcategory`, `base_price`, `unit_cost` |
| `dim_employees_gold` | jeden řádek na zaměstnanec | `employee_id`, `name`, `gender`, `hire_date`, `exit_date` |
| `dim_payroll_gold` | zaměstnanec × měsíc | `employee_id`, `month_year`, `monthly_salary` |

### Fakt a marty (DirectQuery v Power BI)

| Model | Grain | Klíčové metriky |
|---|---|---|
| `fact_orders_gold` | jeden řádek na objednávku | `product_revenue`, `addon_revenue`, `total_order_value` |
| `mart_monthly_product_sales` | měsíc × produkt | `total_qty`, `total_revenue`, `product_margin` |
| `mart_hourly_traffic_conversion` | zkrácená hodina | `total_visits`, `total_carts`, `total_orders` |
| `mart_traffic_conversion_by_product` | měsíc × produkt | `total_views`, `cart_rate_pct`, `conversion_rate_pct` |
| `mart_employee_addon_performance` | měsíc × zaměstnanec | `addon_attach_rate_pct`, `total_addon_revenue`, `tenure_bracket` |

---

## Technický stack

| Vrstva | Technologie |
|---|---|
| Orchestrace | Apache Airflow 2.7.1 (Docker, LocalExecutor) |
| Generování dat | Python (Pandas) |
| Lokální staging | PostgreSQL 15 (Docker) |
| Cloud warehouse | Snowflake |
| Transformace | dbt Core (inkrementální materializace) |
| Reporting & Analytika | Power BI (kompozitní model, DirectQuery + Import) |

---

## Spuštění projektu lokálně

```bash
# Start infrastruktury
docker-compose up -d

# dbt transformace (z adresáře dbt_alfa/)
export SF_PASSWORD=<viz docker-compose.yaml>
cd dbt_alfa
dbt run --select staging        # SILVER views
dbt run --select marts          # GOLD tabulky
dbt test

# Spuštění celé pipeline
# Airflow UI → http://localhost:8082  (admin / admin)
# DAG: 05_Master_Orchestrator → spustit manuálně
```
