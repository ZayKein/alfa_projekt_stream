# CLAUDE.md

> Shared context file for Claude sessions working on this project.
> Last updated: 2026-04-30

---

## Project: alfa_projekt_stream

**Alfa Stream v5.0** — end-to-end data platform combining data engineering, data analytics, and ML. Author: David Urban.

ELT pipelines land data in Snowflake across RAW → SILVER → GOLD layers, modeled with dbt. Reporting layer is **Power BI** (DirectQuery + composite model). ML layer produces revenue forecasts, anomaly flags, and traffic predictions.

### Current state (as of 2026-04-30)

- Data generators: **complete** — Products (300 items, 6 cats/3 subcats/4-6 brands/5-10 models), Traffic (full seasonal + day-of-week + random event spikes), Orders. All data starts from **2021-01-01**.
- Orchestration: **complete** — Master Orchestrator (DAG 05) runs 00→01a→01b→01c→02→03→04→06 sequentially.
- Postgres ODS: **complete** — local Docker, intermediate staging before Snowflake.
- Snowflake RAW layer: **complete** — all raw tables loaded via DAG 03.
- dbt SILVER: **complete** — 5 staging views in SILVER schema.
- dbt GOLD: **complete** — 3 dims, 1 fact, 5 mart aggregations + 3 ML output tables.
- ML layer (DAG 06): **complete** — Prophet (revenue forecast), Z-score (anomaly detection), Ridge Regression (traffic patterns). All 3 run in parallel.
- Power BI semantic model: **complete** — all TMDL files written, 10 relationships, 17 DAX measures, Date Hierarchy on DIM_DATE, BRAND column on DIM_PRODUCTS_GOLD.
- Power BI report pages: **in progress** — Page 1 (Sales Overview) has cards and line chart placed. Pages 2–5 not yet built.

### Active phase

Building the 5 Power BI report pages. Semantic model is stable. Work is visual — place visuals, bind fields, format. Publish to MS Fabric 60-day trial when done.

---

## Tech stack

- **Warehouse**: Snowflake — account `lraixsh-yh49291`, database `ALFA_PROJEKT`, warehouse `ALFA_WH`, role `ACCOUNTADMIN`, user `ZAYKEIN`
- **Transformation**: dbt Core (dbt-snowflake adapter), incremental materialization throughout gold layer
- **Orchestration**: Apache Airflow 2.7.1, LocalExecutor, Docker port 8082, credentials admin/admin
- **Ingestion**: Custom Python (Pandas) generators → PostgreSQL 15 (Docker, port 5434, db=airflow, user=airflow) → Snowflake RAW via DAG 03
- **Reporting**: Power BI Desktop PBIP format — `.pbip` at `C:\Users\durba\Desktop\Alfa_stream_Dashboard.pbip`
- **Source control**: Git (this repo)

---

## Repo layout

```
alfa_projekt_stream/
├── dags/
│   ├── 00_hr_generator.py              # HR/employee master data generator
│   ├── 01_A_Alfa_Products.py           # Product catalog generator (300 products, 6 cats)
│   ├── 01_B_Alfa_Traffic.py            # Incremental traffic generator (full seasonality)
│   ├── 01_C_Alfa_Orders.py             # Incremental order generator
│   ├── 02_Load_To_Postgres_Full.py     # Load all CSV data to Postgres ODS
│   ├── 03_Postgres_to_Snowflake.py     # Transfer Postgres → Snowflake RAW
│   ├── 04_Snowflake_Transformation.py  # Triggers dbt run + dbt test
│   ├── 05_Master_Orchestrator.py       # Master DAG: triggers all above sequentially
│   └── 06_ML_Predictions.py           # Parallel ML: Prophet + Z-score + Ridge
├── data/                               # CSV files (inside Docker at /opt/airflow/data/)
├── dbt_alfa/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                    # → SILVER schema, view materialization
│   │   │   ├── src_alfa.yml
│   │   │   ├── stg_employees.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_payroll.sql
│   │   │   ├── stg_products.sql        # includes BRAND column
│   │   │   └── stg_traffic.sql
│   │   └── marts/                      # → GOLD schema, incremental materialization
│   │       ├── dim_date.sql            # 2021-01-01 → 2031, 7 columns
│   │       ├── dim_employees_gold.sql
│   │       ├── dim_payroll_gold.sql
│   │       ├── dim_products_gold.sql   # SELECT * from stg (includes BRAND)
│   │       ├── fact_orders_gold.sql
│   │       ├── mart_employee_addon_performance.sql
│   │       ├── mart_hourly_traffic_conversion.sql
│   │       ├── mart_monthly_product_sales.sql
│   │       └── mart_traffic_conversion_by_product.sql
│   └── target/
├── docker-compose.yaml
├── profiles.yml                        # dbt Snowflake connection (reads SF_PASSWORD env var)
└── README.md
```

---

## Data model — current schema

### RAW schema (Snowflake)
| Table | Key columns |
|---|---|
| `DIM_PRODUCTS` | `PRODUCT_ID`, `NAME`, `CATEGORY`, `SUBCATEGORY`, `BRAND`, `BASE_PRICE`, `UNIT_COST` |
| `DIM_EMPLOYEES` | `EMPLOYEE_ID`, `NAME`, `GENDER`, `CURRENT_AGE`, `HIRE_DATE`, `EXIT_DATE` |
| `FACT_PAYROLL` | `EMPLOYEE_ID`, `MONTHLY_SALARY`, `MONTH_YEAR` |
| `FACT_ORDERS` | `ORDER_ID`, `PRODUCT_ID`, `QUANTITY`, `SERVICE_TYPE`, `SERVICE_PRICE`, `EMPLOYEE_ID`, `ORDER_DATE` |
| `FACT_TRAFFIC` | `TRAFFIC_ID`, `EVENT_TIME`, `ITEM_IN_CART`, `PRODUCT_ID`, `ORDER_ID` |

### GOLD schema — dim_date
Columns: `DATE_DAY`, `YEAR`, `QUARTER`, `MONTH`, `DAY`, `YEAR_QUARTER`, `YEAR_MONTH`
- Date range: **2021-01-01 → 2031-12-14** (4000 rows)
- `YEAR_QUARTER` format: `"2023-Q4"` — used as hierarchy Quarter level
- `YEAR_MONTH` format: `"2023-11"` — used as hierarchy Month level
- Import mode in Power BI. Must use `dbt run --full-refresh --select dim_date` if schema changes.

### GOLD schema — dim_products_gold
Columns: `PRODUCT_ID`, `PRODUCT_NAME`, `CATEGORY`, `SUBCATEGORY`, `BRAND`, `BASE_PRICE`, `UNIT_COST`
- Import mode in Power BI.

### GOLD schema — marts/facts
| Model | Grain | Key columns |
|---|---|---|
| `fact_orders_gold` | one row per order | `order_id`, `product_id`, `employee_id`, `order_timestamp`, `product_revenue`, `addon_revenue`, `total_order_value` |
| `mart_monthly_product_sales` | month × product | `month_prod_id`, `sales_month`, `product_id`, `category`, `subcategory`, `product_name`, `total_qty`, `product_revenue`, `addon_revenue`, `total_revenue`, `product_margin` |
| `mart_hourly_traffic_conversion` | truncated hour | `event_hour`, `total_visits`, `total_carts`, `total_orders` |
| `mart_traffic_conversion_by_product` | month × product | `traffic_month`, `product_id`, `total_views`, `total_carts`, `total_orders`, `cart_rate_pct`, `conversion_rate_pct` |
| `mart_employee_addon_performance` | month × employee | `performance_month`, `employee_id`, `total_orders`, `addon_orders`, `addon_attach_rate_pct`, `total_addon_revenue`, `avg_addon_value`, `tenure_months`, `tenure_bracket` |

### GOLD schema — ML tables
| Model | Grain | Key columns |
|---|---|---|
| `ML_REVENUE_FORECAST` | future date | `forecast_date`, `predicted_revenue`, `lower_bound`, `upper_bound` |
| `ML_ANOMALY_FLAGS` | month | `period`, `total_revenue`, `z_score`, `is_anomaly`, `anomaly_type` |
| `ML_TRAFFIC_PREDICTION` | hour × day-of-week | `hour_of_day`, `day_of_week`, `predicted_visits`, `model_version` |

---

## Product catalog — structure (as of 2026-04-30)

Full redesign from Czech flat list to English 4-level hierarchy. **300 products, random.seed(42)**.

| Category | Subcategories | Brands (examples) | Pricing tier |
|---|---|---|---|
| Mobile | Smartphones - iPhone, Smartphones - Android, Phone Accessories | Apple / Samsung, Google, Xiaomi, OnePlus, Sony / Apple, Anker, Spigen | iPhone 25–75k; Android 6–28k; Accessories 500–5k |
| Laptops | Premium Laptops, Gaming Laptops, Office Laptops | Apple, Dell, Microsoft, LG, ASUS / ASUS, Lenovo, MSI, Acer, Razer, HP / Lenovo, HP, Dell, ASUS, Acer | Premium 28–85k; Gaming 22–65k; Office 7–26k |
| Gaming | Consoles, Games, Gaming Accessories | Sony, Microsoft, Nintendo, Valve, ASUS, Lenovo / EA, Sony, Microsoft, CD Projekt, Bandai Namco, Ubisoft / Sony, Microsoft, Razer, Logitech, SteelSeries, HyperX | Consoles 7–20k; Games 800–2k; Accessories 500–7k |
| Beauty | Fragrances, Hair Care, Grooming | Dior, Chanel, Armani, Versace, Hugo Boss, YSL / Dyson, GHD, BaByliss, Philips, Remington / Braun, Philips, Oral-B, Panasonic, Wahl | Fragrances 1–6.5k; Hair Care 2–18k; Grooming 800–9k |
| Home Appliances | Refrigerators, Washing Machines, Kitchen Appliances | Samsung, LG, Bosch, Miele, Liebherr / LG, Samsung, Bosch, Miele, Siemens / Bosch, Siemens, Miele, Neff, AEG | Fridges 8–32k; Washing 5–22k; Kitchen 8–45k |
| Consumer Electronics | TVs, Sound Systems, Smart Home & Coffee | LG, Samsung, Sony, Philips, Panasonic / Sonos, Samsung, Sony, Bose, Denon / Apple, Philips, De'Longhi, Jura, Nespresso | TVs 12–75k; Sound 4–32k; Smart/Coffee 1.5–28k |

Margin ranges by tier: iPhones/Premium Laptops/Gaming Laptops/TVs/Kitchen → 10–22%; Android/Office/Consoles/Fridges/Washing/Sound → 18–30%; Accessories/Games/Beauty/Smart Home → 25–65%.

---

## Traffic generator — seasonality (as of 2026-04-30)

`01_B_Alfa_Traffic.py` generates incremental daily click events from 2021-01-01.

```python
# Annual growth bands
growth = {2021: (100,200), 2022: (200,400), 2023: (400,700),
          2024: (700,1200), 2025: (1200,2000), 2026: (1500,2500)}

# Monthly seasonal multipliers
month_mult = {1:0.70, 2:0.78, 3:0.88, 4:0.92, 5:1.05, 6:0.88,
              7:0.80, 8:0.90, 9:1.00, 10:1.10, 11:2.50, 12:3.50}

# Day-of-week multipliers (ISO: 1=Mon…7=Sun)
dow_mult = {1:1.0, 2:1.05, 3:1.10, 4:1.08, 5:1.15, 6:1.25, 7:0.85}

# 3% chance of random event spike (flash sale, etc.)
event_mult = random.uniform(1.8, 3.0) if random.random() < 0.03 else 1.0
```

Cart rate: 65–90% of visits; conversion rate: 30–50% of carts.

---

## Power BI — semantic model

### File locations
- `.pbip`: `C:\Users\durba\Desktop\Alfa_stream_Dashboard.pbip`
- Semantic model TMDL: `C:\Users\durba\Desktop\Alfa_stream_Dashboard.SemanticModel\definition\`
- Report pages: `C:\Users\durba\Desktop\Alfa_stream_Dashboard.Report\definition\pages\`

### CRITICAL: Power BI TMDL behaviour
**Always close Power BI Desktop before editing TMDL files.** When PBI is open and saves, it overwrites TMDL files and reverts manual changes — especially:
- Reverts `int64` columns to `double` (adds `PBI_FormatHint`)
- Reverts `summarizeBy: none` to `summarizeBy: sum` on numeric columns (if `SummarizationSetBy = Automatic`)
- Strips hierarchy level column references back to raw integer columns
- Removes manually added columns (like BRAND) from table TMDL files

Use `SummarizationSetBy = User` (not `Automatic`) on columns where `summarizeBy: none` must be preserved.

### Import tables (refreshed on demand)
- `DIM_DATE` — date spine
- `DIM_EMPLOYEES_GOLD`
- `DIM_PAYROLL_GOLD`
- `DIM_PRODUCTS_GOLD`

### DirectQuery tables
- `FACT_ORDERS_GOLD`, `MART_MONTHLY_PRODUCT_SALES`, `MART_HOURLY_TRAFFIC_CONVERSION`, `MART_TRAFFIC_CONVERSION_BY_PRODUCT`, `MART_EMPLOYEE_ADDON_PERFORMANCE`, `ML_REVENUE_FORECAST`, `ML_ANOMALY_FLAGS`, `ML_TRAFFIC_PREDICTION`

### DIM_DATE — Date Hierarchy (built in TMDL)
```
Date Hierarchy
  └─ Year      → column: YEAR          (integer, e.g. 2023)
  └─ Quarter   → column: YEAR_QUARTER  (string,  e.g. "2023-Q4")
  └─ Month     → column: YEAR_MONTH    (string,  e.g. "2023-11")
  └─ Day       → column: DATE_DAY      (date)
```
Use this hierarchy on line chart X-axis for proper drill-down with year context preserved at all levels.

### DAX measures — BUILT

| Measure | Table | Formula |
|---|---|---|
| `Total Revenue` | MART_MONTHLY_PRODUCT_SALES | `SUM([TOTAL_REVENUE])` |
| `Total Product Revenue` | MART_MONTHLY_PRODUCT_SALES | `SUM([PRODUCT_REVENUE])` |
| `Total Addon Revenue` | MART_MONTHLY_PRODUCT_SALES | `SUM([ADDON_REVENUE])` |
| `Total Margin` | MART_MONTHLY_PRODUCT_SALES | `SUM([PRODUCT_MARGIN])` |
| `Margin %` | MART_MONTHLY_PRODUCT_SALES | `DIVIDE([Total Margin], [Total Product Revenue], 0)` |
| `Total Qty Sold` | MART_MONTHLY_PRODUCT_SALES | `SUM([TOTAL_QTY])` |
| `Revenue MoM %` | MART_MONTHLY_PRODUCT_SALES | `DIVIDE(current - DATEADD(-1M), DATEADD(-1M), 0)` |
| `Revenue YoY %` | MART_MONTHLY_PRODUCT_SALES | `DIVIDE(current - SAMEPERIODLASTYEAR, SAMEPERIODLASTYEAR, 0)` |
| `Total Orders` | FACT_ORDERS_GOLD | `COUNTROWS(FACT_ORDERS_GOLD)` |
| `Total Views` | MART_TRAFFIC_CONVERSION_BY_PRODUCT | `SUM([TOTAL_VIEWS])` |
| `Total Carts` | MART_TRAFFIC_CONVERSION_BY_PRODUCT | `SUM([TOTAL_CARTS])` |
| `Conversion Rate %` | MART_TRAFFIC_CONVERSION_BY_PRODUCT | `DIVIDE(SUM[TOTAL_ORDERS], SUM[TOTAL_VIEWS], 0)` |
| `Cart Rate %` | MART_TRAFFIC_CONVERSION_BY_PRODUCT | `DIVIDE(SUM[TOTAL_CARTS], SUM[TOTAL_VIEWS], 0)` |
| `Total Visits` | MART_HOURLY_TRAFFIC_CONVERSION | `SUM([TOTAL_VISITS])` |
| `Addon Attach Rate %` | MART_EMPLOYEE_ADDON_PERFORMANCE | `DIVIDE(SUM[ADDON_ORDERS], SUM[TOTAL_ORDERS], 0)` |
| `Total Addon Revenue (emp)` | MART_EMPLOYEE_ADDON_PERFORMANCE | `SUM([TOTAL_ADDON_REVENUE])` |
| `Avg Addon Value` | MART_EMPLOYEE_ADDON_PERFORMANCE | `DIVIDE(SUM[TOTAL_ADDON_REVENUE], SUM[ADDON_ORDERS], 0)` |
| `Predicted Visits` | ML_TRAFFIC_PREDICTION | `SUM([PREDICTED_VISITS])` |
| `Total Anomalies` | ML_ANOMALY_FLAGS | `COUNTROWS(FILTER(ML_ANOMALY_FLAGS, [IS_ANOMALY]=TRUE()))` |

### Relationships — BUILT

| From (many) | To (one) | Notes |
|---|---|---|
| FACT_ORDERS_GOLD.ORDER_TIMESTAMP | DIM_DATE.DATE_DAY | |
| MART_MONTHLY_PRODUCT_SALES.SALES_MONTH | DIM_DATE.DATE_DAY | |
| MART_EMPLOYEE_ADDON_PERFORMANCE.PERFORMANCE_MONTH | DIM_DATE.DATE_DAY | |
| MART_TRAFFIC_CONVERSION_BY_PRODUCT.TRAFFIC_MONTH | DIM_DATE.DATE_DAY | |
| MART_HOURLY_TRAFFIC_CONVERSION.EVENT_HOUR | DIM_DATE.DATE_DAY | |
| ML_REVENUE_FORECAST.FORECAST_DATE | DIM_DATE.DATE_DAY | |
| ML_ANOMALY_FLAGS.PERIOD | DIM_DATE.DATE_DAY | |
| FACT_ORDERS_GOLD.PRODUCT_ID | DIM_PRODUCTS_GOLD.PRODUCT_ID | |
| MART_MONTHLY_PRODUCT_SALES.PRODUCT_ID | DIM_PRODUCTS_GOLD.PRODUCT_ID | |
| MART_TRAFFIC_CONVERSION_BY_PRODUCT.PRODUCT_ID | DIM_PRODUCTS_GOLD.PRODUCT_ID | |
| FACT_ORDERS_GOLD.EMPLOYEE_ID | DIM_EMPLOYEES_GOLD.EMPLOYEE_ID | |
| MART_EMPLOYEE_ADDON_PERFORMANCE.EMPLOYEE_ID | DIM_EMPLOYEES_GOLD.EMPLOYEE_ID | |

---

## Power BI — report pages

Page folder IDs under `Alfa_stream_Dashboard.Report/definition/pages/`:

| Page | Folder ID | Status |
|---|---|---|
| Sales Overview | `e73d27899e14a86c405a` | Line chart + 4 KPI cards placed. Line chart: X=DIM_DATE Date Hierarchy, Y=Total Revenue + Total Margin. Cards: Total Margin, (3 others TBD). |
| Product Performance | `a1b2c3d4e5f6a7b8c9d0` | Empty scaffold |
| Traffic & Conversion | `b2c3d4e5f6a7b8c9d0e1` | Empty scaffold |
| Hourly Patterns | `c3d4e5f6a7b8c9d0e1f2` | Empty scaffold |
| Employee Addon Performance | `d4e5f6a7b8c9d0e1f2a3` | Empty scaffold |

### Planned visual layout per page

**Page 1 — Sales Overview**
- KPI cards: Total Revenue, Total Margin, Margin %, Total Orders, Revenue MoM %, Revenue YoY %
- Line chart: DIM_DATE[Date Hierarchy] × Total Revenue + Total Margin (drill Year→Quarter→Month→Day)
- Donut chart: Revenue by Category (user added: "Orders by Category")
- Slicer: DIM_DATE[YEAR] or DIM_PRODUCTS_GOLD[CATEGORY]

**Page 2 — Product Performance**
- Bar chart: Total Revenue by CATEGORY
- Bar chart: Top 10 products by Total Revenue
- Matrix: BRAND × SUBCATEGORY, values = Total Revenue / Margin % (conditional formatting on Margin %)
- Slicer: CATEGORY, SUBCATEGORY, BRAND

**Page 3 — Traffic & Conversion**
- Funnel or clustered bar: Total Views → Total Carts → Total Orders (from MART_TRAFFIC_CONVERSION_BY_PRODUCT)
- Line chart: Conversion Rate % by month
- Scatter: product Total Views vs Conversion Rate % (bubble = revenue)
- Slicer: CATEGORY

**Page 4 — Hourly Patterns**
- Matrix heatmap: HOUR_OF_DAY (rows) × day-of-week (columns) × Total Visits (values, colour scale)
- Line chart: Total Visits by HOUR_OF_DAY
- Bar chart: ML_TRAFFIC_PREDICTION[PREDICTED_VISITS] by HOUR_OF_DAY
- Slicer: day of week

**Page 5 — Employee Addon Performance**
- Bar chart: Addon Attach Rate % by TENURE_BRACKET
- Scatter: TENURE_MONTHS vs Addon Attach Rate % per employee (bubble = Total Addon Revenue)
- Table: top 15 employees — Name, Tenure Bracket, Addon Orders, Attach Rate %, Total Addon Revenue
- KPI card: overall Addon Attach Rate %, Avg Addon Value

### Publishing target
**MS Fabric 60-day trial** — publish from Power BI Desktop → Fabric workspace → publish-to-web public link. No on-prem gateway needed for Snowflake DirectQuery in Fabric.

---

## Conventions

- **Model naming**: `stg_*` (staging views), `dim_*_gold` (dimension tables), `fact_*_gold` (fact table), `mart_*` (pre-aggregated mart tables)
- **Materialization**: staging → `view`; dims → `table`; all marts/facts → `incremental`
- **Schemas**: SILVER (staging views), GOLD (dims, facts, marts, ML outputs)
- **Incremental key**: `unique_key` on natural grain key of each model
- **Source columns**: RAW uses UPPERCASE; staging renames to lowercase snake_case
- **Don't touch** `dags/00–03` without explicit ask. `stg_products.sql` was modified in session 3 to add BRAND — treat with care.

---

## Full data reset procedure

When changing generator logic (products, traffic) and needing a clean rebuild:

```bash
# 1. Delete CSVs inside Docker
docker exec alfa_stream_airflow rm /opt/airflow/data/products_raw.test.csv
docker exec alfa_stream_airflow rm /opt/airflow/data/traffic_raw.test.csv

# 2. Truncate Postgres staging tables
docker exec alfa_stream_postgres psql -U airflow -d airflow \
  -c "TRUNCATE TABLE dim_products; TRUNCATE TABLE fact_traffic;"

# 3. Drop Snowflake GOLD tables (DROP not TRUNCATE — empty tables break dbt incremental)
# Use python in airflow container with insecure_mode=True
# Drop: RAW.DIM_PRODUCTS, GOLD.DIM_PRODUCTS_GOLD, GOLD.FACT_ORDERS_GOLD,
#       GOLD.MART_MONTHLY_PRODUCT_SALES, GOLD.MART_TRAFFIC_CONVERSION_BY_PRODUCT,
#       GOLD.MART_HOURLY_TRAFFIC_CONVERSION, GOLD.MART_EMPLOYEE_ADDON_PERFORMANCE,
#       GOLD.ML_REVENUE_FORECAST, GOLD.ML_ANOMALY_FLAGS, GOLD.ML_TRAFFIC_PREDICTION
# For DIM_DATE schema changes: also DROP GOLD.DIM_DATE

# 4. Trigger pipeline
docker exec alfa_stream_airflow airflow dags trigger 05_Master_Orchestrator
```

**Important**: Use DROP (not TRUNCATE) for dbt incremental tables. When a table exists but is empty, `WHERE x > MAX(x)` evaluates to `WHERE x > NULL` → nothing is inserted.

---

## How to work in this repo

```bash
# Start infrastructure
docker-compose up -d

# dbt — run inside container or with env var exported
export SF_PASSWORD=z4drXQhdtEfy7JE
cd dbt_alfa
dbt run --select staging
dbt run --select marts
dbt run --full-refresh --select dim_date   # use when dim_date schema changes

# Trigger full pipeline
docker exec alfa_stream_airflow airflow dags trigger 05_Master_Orchestrator

# Check DAG task states
docker exec alfa_stream_airflow airflow tasks states-for-dag-run \
  05_Master_Orchestrator <run_id>
```

Snowflake connection from Airflow container uses `insecure_mode=True` (bypasses OCSP validation — needed due to pycryptodome 3.18 incompatibility installed as Prophet dependency).

---

## Known bugs / fixed issues

- ~~`mart_monthly_product_sales` incremental filter bug~~ — **FIXED 2026-04-28**
- ~~Flat 25% margin on all products~~ — **FIXED 2026-04-30** (category-based margin ranges 10–65%)
- ~~Traffic line chart flat (no seasonality)~~ — **FIXED 2026-04-30** (full 12-month curve + DOW + event spikes)
- ~~DIM_DATE starting 2023, missing 2 years of history~~ — **FIXED 2026-04-30** (starts 2021-01-01)
- ~~Date hierarchy showing wrong counts on drill-down~~ — **FIXED 2026-04-30** (YEAR_QUARTER/YEAR_MONTH string columns used at Quarter/Month levels)
- **Power BI overwrites TMDL on save** — ongoing behaviour. Always close PBI before editing TMDL files. Use `SummarizationSetBy = User` to protect `summarizeBy: none` from being reverted.

---

## Glossary

- **traffic**: website visit events (FACT_TRAFFIC); one row per page visit, linked to order if converted.
- **item_in_cart**: `'yes'`/`'no'` flag on traffic event — used for cart count.
- **service_type / service_price**: add-on services on orders (warranty, installation); `'None'` → NULL in staging.
- **exit_date**: employee exit; `'9999-12-31'` for active employees.
- **month_prod_id**: surrogate key in `mart_monthly_product_sales` — MD5 of `(sales_month, product_id)`.
- **insecure_mode=True**: Snowflake SQLAlchemy parameter that bypasses OCSP certificate validation. Required in this environment due to pycryptodome 3.18 conflict with Prophet.

---

## Changelog

- *2026-04-30 (session 3)* — Full product catalog redesign (6 cats / 3 subcats / 4-6 brands / 5-10 models, all English, BRAND column added). Traffic seasonality overhauled (12-month curve + DOW + event spikes). DIM_DATE rebuilt from 2021-01-01 with Date Hierarchy (YEAR_QUARTER/YEAR_MONTH string levels). Page 1 line chart wired up. Multiple full data resets executed. Documented TMDL overwrite behaviour.
- *2026-04-29 (session 2)* — Re-added all 10 relationships, 17 DAX measures, PRODUCT_ID to mart TMDL. ML tables (ML_ANOMALY_FLAGS, ML_TRAFFIC_PREDICTION) added to semantic model. Publishing target confirmed: MS Fabric 60-day trial.
- *2026-04-29 (session 1)* — Power BI semantic model fully configured: 10 relationships, 17 DAX measures, Import/DirectQuery split, summarizeBy fixes. DAG 06 ML pipeline complete (Prophet + Z-score + Ridge). Fixed OCSP error with insecure_mode=True.
- *2026-04-28* — Codebase populated; incremental bug in mart_monthly_product_sales fixed; schema naming fixed; dim_date + 2 mart models added.
