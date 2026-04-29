# Alfa Stream v5.0 — End-to-End Datová Platforma

> 🇬🇧 [English version](./README.md)

**Alfa Stream** je osobní portfolio projekt, který jsem navrhl a postavil celý od základů jako ukázku komplexního end-to-end datového projektu — kombinující **datové inženýrství** a **datovou analytiku** v jedné platformě.

Projekt pokrývá celý datový životní cyklus: simulace zdrojových dat, orchestrace pipeline, cloud warehouse, transformační modelování a analytická vrstva s byznysovými dashboardy. Platforma simuluje reálný e-commerce provoz — zákazníci procházejí katalogem, přidávají produkty do košíku, vytvářejí objednávky, zaměstnanci přidávají doplňkové služby. Vše automaticky protéká vícevrstvou pipeline do Snowflake, kde Power BI dashboardy zobrazují byznysové výstupy.

**Autor:** David Urban

---

## Co platforma dělá

1. **Generuje realistická data** — Python skripty simulují produktový katalog, HR data, webový provoz a objednávky se sezónností a peak-hour logikou.
2. **Ukládá lokálně** — všechna generovaná data přistávají v PostgreSQL kontejneru (ODS vrstva) před odesláním do cloudu.
3. **Načítá do Snowflake** — vektorizovaný DAG přesouvá data z Postgresu do Snowflake RAW schématu.
4. **Transformuje přes dbt** — dbt Core modely čistí, spojují a agregují surová data do SILVER (staging) a GOLD (byznys) vrstvy pomocí plně inkrementální (Delta Load) logiky.
5. **Reportuje v Power BI** — kompozitní model (Import + DirectQuery) se napojuje na GOLD vrstvu s předpřipravenými DAX metrikami a pěti dashboard stránkami pokrývajícími prodeje, produkty, traffic, hodinové vzorce a výkon zaměstnanců.

Vše běží automaticky přes Master Orchestrátor DAG v Apache Airflow.

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

## Klíčová designová rozhodnutí

- **Delta Load všude** — od generátorů po dbt modely se zpracovávají pouze nové řádky při každém běhu. Minimalizuje compute náklady na Snowflake.
- **Třívrstvá Snowflake architektura** — RAW (jako přistálo), SILVER (vyčištěné views), GOLD (byznys tabulky a mart agregace).
- **Předagregované marty** — pět mart tabulek slouží jako primární zdroje pro Power BI vizuály, snižuje zátěž DirectQuery a umožňuje aggregation awareness.
- **Kompozitní Power BI model** — malé dimenzionální tabulky se importují pro rychlost; velká fakta a marty zůstávají v DirectQuery pro aktuálnost dat.

---

## Struktura repozitáře

```
alfa_projekt_stream/
├── dags/
│   ├── 00_hr_generator.py              # Generátor HR master dat
│   ├── 01_A_Alfa_Products.py           # Master data produktů
│   ├── 01_B_Alfa_Traffic.py            # Inkrementální generátor traffic událostí
│   ├── 01_C_Alfa_Orders.py             # Inkrementální generátor objednávek
│   ├── 02_Load_To_Postgres_Full.py     # Načtení dat do lokálního Postgresu
│   ├── 03_Postgres_to_Snowflake.py     # Vektorizovaný přesun Postgres → Snowflake RAW
│   ├── 04_Snowflake_Transformation.py  # Spouští dbt run + dbt test
│   └── 05_Master_Orchestrator.py       # Sekvenční spuštění všech DAGů výše
├── dbt_alfa/
│   └── models/
│       ├── staging/                    # SILVER schéma — vyčištěné views
│       └── marts/                      # GOLD schéma — inkrementální tabulky
├── Documentation ENG/                  # Anglická dokumentace a diagramy
├── Documentation CZ/                   # Česká dokumentace
├── docker-compose.yaml
├── profiles.yml
└── README.md
```

---

## GOLD vrstva — datové modely

| Model | Grain | Účel |
|---|---|---|
| `dim_date` | jeden řádek na kalendářní den | Date spine pro time intelligence |
| `dim_products_gold` | jeden řádek na produkt | Produktová dimenze |
| `dim_employees_gold` | jeden řádek na zaměstnance | Zaměstnanecká dimenze |
| `dim_payroll_gold` | jeden řádek na zaměstnanec × měsíc | Mzdová dimenze |
| `fact_orders_gold` | jeden řádek na objednávku | Detailní faktová tabulka objednávek |
| `mart_monthly_product_sales` | měsíc × produkt | Agregace příjmů a marže |
| `mart_hourly_traffic_conversion` | zkrácená hodina | Traffic a konverze po hodinách |
| `mart_traffic_conversion_by_product` | měsíc × produkt | Produktový funnel (zobrazení → košík → objednávka) |
| `mart_employee_addon_performance` | měsíc × zaměstnanec | Attach rate a příjem z addonů podle délky praxe |

---

## Power BI Dashboard — stránky

1. **Přehled prodejů** — klíčové KPI, měsíční trend příjmů, MoM a YoY srovnání
2. **Výkon produktů** — příjmy a marže podle kategorie a produktu
3. **Traffic & Konverzní funnel** — zobrazení → košík → objednávka podle produktu a v čase
4. **Hodinové vzorce** — heatmapa trafficu a konverzí podle hodiny dne
5. **Výkon zaměstnanců (Addony)** — attach rate a příjem z addonů podle délky praxe

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

---

## Schéma architektury

![Data Pipeline Schema](./Documentation%20ENG/Data%20Pipeline%20Schema.png)
