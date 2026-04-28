# CLAUDE.md

> Shared context file for Claude sessions working on this project.
> Populated from codebase read on 2026-04-28.

---

## Project: alfa_projekt_stream

**Alfa Stream v4.5** — end-to-end data platform simulating real e-commerce operations. Author: David Urban.

ELT pipelines land data in Snowflake across RAW → SILVER → GOLD layers, modeled with dbt. Reporting layer is **Power BI** (DirectQuery against Snowflake, composite model planned).

### Current state (as of 2026-04-28)

- Data generators: **complete** — Python scripts simulate Products, Traffic, Orders with seasonality/peak-hour logic.
- Orchestration: **complete** — Airflow Master Orchestrator DAG runs the full pipeline sequentially.
- Postgres ODS: **complete** — local Docker container, used as intermediate staging before Snowflake.
- Snowflake RAW layer: **complete** — raw tables loaded via DAG 03.
- dbt SILVER (staging views): **complete** — all 5 staging models built.
- dbt GOLD (marts/dims): **complete** — 3 dims, 1 fact, 2 mart aggregations built.
- Power BI reports: **not yet built** — data layer is ready, `.pbix` not created yet.

### Active phase

Building Power BI composite model on top of the gold layer. The two mart models (`mart_monthly_product_sales`, `mart_hourly_traffic_conversion`) serve as **pre-aggregated tables** to back DirectQuery visuals. Small dims will be imported; large facts and aggs stay in DirectQuery.

---

## Tech stack

- **Warehouse**: Snowflake — account `lraixsh-yh49291`, database `ALFA_PROJEKT`, warehouse `ALFA_WH`, role `ACCOUNTADMIN`, user `ZAYKEIN`
- **Transformation**: dbt Core (dbt-snowflake adapter), incremental materialization throughout gold layer
- **Orchestration**: Apache Airflow 2.7.1, LocalExecutor, runs inside Docker on port 8082
- **Ingestion**: Custom Python (Pandas) generators → PostgreSQL 15 (Docker, port 5434) → Snowflake RAW via DAG 03
- **Reporting**: Power BI (DirectQuery + composite model planned)
- **Source control**: Git (this repo)

---

## Repo layout

```
alfa_projekt_stream/
├── dags/
│   ├── 00_hr_generator.py          # HR/employee master data generator
│   ├── 01_A_Alfa_Products.py       # Product master data
│   ├── 01_B_Alfa_Traffic.py        # Incremental traffic event generator (seasonality, peak hours)
│   ├── 01_C_Alfa_Orders.py         # Incremental order generator
│   ├── 02_Load_To_Postgres_Full.py # Load all data to local Postgres ODS
│   ├── 03_Postgres_to_Snowflake.py # Vectorized transfer Postgres → Snowflake RAW
│   ├── 04_Snowflake_Transformation.py  # Triggers dbt run + dbt test
│   └── 05_Master_Orchestrator.py   # Master DAG: sequential trigger of 00→01a→01b→01c→02→03→04
├── data/
│   ├── employees_master.csv
│   ├── employees_payroll.csv
│   ├── orders_raw.test.csv
│   ├── products_raw.test.csv
│   └── traffic_raw.test.csv
├── dbt_alfa/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                # → SILVER schema, materialized as view
│   │   │   ├── src_alfa.yml        # source definitions (RAW schema tables)
│   │   │   ├── stg_employees.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_payroll.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_traffic.sql
│   │   └── marts/                  # → GOLD schema, materialized as incremental table
│   │       ├── dim_date.sql
│   │       ├── dim_employees_gold.sql
│   │       ├── dim_payroll_gold.sql
│   │       ├── dim_products_gold.sql
│   │       ├── fact_orders_gold.sql
│   │       ├── mart_employee_addon_performance.sql
│   │       ├── mart_hourly_traffic_conversion.sql
│   │       ├── mart_monthly_product_sales.sql
│   │       └── mart_traffic_conversion_by_product.sql
│   └── target/                     # compiled + run artifacts (do not edit)
├── documentation/                  # CZ/ENG docs, architecture diagrams
├── docker-compose.yaml             # Postgres + Airflow services
├── profiles.yml                    # dbt connection profile (reads SF_PASSWORD from env)
└── README.md
```

---

## Conventions

- **Model naming**: `stg_*` (staging views), `dim_*_gold` (dimension tables), `fact_*_gold` (fact table), `mart_*` (pre-aggregated mart tables for Power BI)
- **Materialization**: staging → `view`; all gold/marts → `incremental` (except dims which are `table`)
- **Schemas**: staging lands in `SILVER`; all marts/dims/facts land in `GOLD`
- **Incremental key**: `unique_key` set on the natural grain key of each model
- **Source columns**: RAW tables use UPPERCASE column names; staging renames/casts to lowercase snake_case where needed
- **No clustering keys** defined yet on any model
- **No dbt tests** exist yet — adding `unique`, `not_null`, `relationships` tests is a pending task
- **Don't touch** `dags/00–03` or `staging/` models without an explicit ask — pipelines and silver layer are stable

---

## Data model — key columns

### Sources (RAW schema)
| Table | Key columns |
|---|---|
| `DIM_PRODUCTS` | `PRODUCT_ID`, `NAME`, `CATEGORY`, `SUBCATEGORY`, `BASE_PRICE`, `UNIT_COST` |
| `DIM_EMPLOYEES` | `EMPLOYEE_ID`, `NAME`, `GENDER`, `CURRENT_AGE`, `HIRE_DATE`, `EXIT_DATE` |
| `FACT_PAYROLL` | `EMPLOYEE_ID`, `MONTHLY_SALARY`, `MONTH_YEAR` |
| `FACT_ORDERS` | `ORDER_ID`, `PRODUCT_ID`, `QUANTITY`, `SERVICE_TYPE`, `SERVICE_PRICE`, `EMPLOYEE_ID`, `ORDER_DATE` |
| `FACT_TRAFFIC` | `TRAFFIC_ID`, `EVENT_TIME`, `ITEM_IN_CART`, `PRODUCT_ID`, `ORDER_ID` |

### Gold models
| Model | Grain | Key measures |
|---|---|---|
| `dim_date` | one row per day | year, quarter, month, day_of_week, is_weekend, week/month/quarter/year start |
| `fact_orders_gold` | one row per `order_id` | `product_revenue`, `addon_revenue`, `total_order_value` |
| `mart_monthly_product_sales` | month × product | `total_qty`, `product_revenue`, `addon_revenue`, `total_revenue`, `product_margin` |
| `mart_hourly_traffic_conversion` | truncated hour | `total_visits`, `total_carts`, `total_orders` |
| `mart_traffic_conversion_by_product` | month × product | `total_views`, `total_carts`, `total_orders`, `cart_rate_pct`, `conversion_rate_pct` |
| `mart_employee_addon_performance` | month × employee | `total_orders`, `addon_orders`, `addon_attach_rate_pct`, `total_addon_revenue`, `avg_addon_value`, `tenure_months`, `tenure_bracket` |

---

## Power BI prep — design notes

### Gold layer — full table inventory

| Model | Grain | Purpose |
|---|---|---|
| `dim_date` | one row per calendar day (2023-01-01 → ~2031) | Date spine for time intelligence |
| `dim_employees_gold` | one row per employee | Employee dimension |
| `dim_payroll_gold` | one row per employee × month | Payroll dimension |
| `dim_products_gold` | one row per product | Product dimension |
| `fact_orders_gold` | one row per order | Order-level detail fact |
| `mart_monthly_product_sales` | month × product | Revenue & margin agg |
| `mart_hourly_traffic_conversion` | truncated hour | Traffic & conversion agg |
| `mart_traffic_conversion_by_product` | month × product | Product-level funnel (views→carts→orders) |
| `mart_employee_addon_performance` | month × employee | Addon sales vs tenure/seniority |

All mart/fact models use **incremental materialization** (`unique_key` merge). Latest month is always re-processed to handle partial-month data correctly (`>= MAX(period_column)`).

### Power BI composite model design

**Imported tables** (small, refresh on schedule):
- `dim_date` — date spine, enables all time intelligence
- `dim_employees_gold`
- `dim_payroll_gold`
- `dim_products_gold`

**DirectQuery tables** (too large to import):
- `fact_orders_gold` — order detail
- `mart_monthly_product_sales` — agg over fact_orders, monthly × product
- `mart_hourly_traffic_conversion` — agg over traffic, hourly
- `mart_traffic_conversion_by_product` — agg over traffic, monthly × product
- `mart_employee_addon_performance` — agg over fact_orders, monthly × employee

**User-defined aggregations (agg awareness):**
- `mart_monthly_product_sales` → aggregation table for `fact_orders_gold` (month + product grain)
- `mart_traffic_conversion_by_product` → aggregation table for traffic detail (month + product grain)

### Planned report pages

1. **Sales Overview** — headline KPIs (total revenue, margin, orders), trend by month using `mart_monthly_product_sales` + `dim_date`
2. **Product Performance** — revenue/margin by category & product, top/bottom performers
3. **Traffic & Conversion Funnel** — views → carts → orders by product/category using `mart_traffic_conversion_by_product`
4. **Hourly Patterns** — peak hours heatmap using `mart_hourly_traffic_conversion`
5. **Employee Addon Performance** — addon attach rate & revenue by tenure bracket using `mart_employee_addon_performance`; does experience drive better upselling?

### Key DAX measures to build

- `Total Revenue` = SUM(mart_monthly_product_sales[total_revenue])
- `Total Margin` = SUM(mart_monthly_product_sales[product_margin])
- `Margin %` = DIVIDE([Total Margin], [Total Revenue])
- `Conversion Rate %` = DIVIDE(SUM(mart_traffic_conversion_by_product[total_orders]), SUM(mart_traffic_conversion_by_product[total_views]))
- `Cart Rate %` = DIVIDE(SUM(mart_traffic_conversion_by_product[total_carts]), SUM(mart_traffic_conversion_by_product[total_views]))
- `Addon Attach Rate %` = DIVIDE(SUM(mart_employee_addon_performance[addon_orders]), SUM(mart_employee_addon_performance[total_orders]))
- MoM / YoY variants using `dim_date` relationships and DATEADD/SAMEPERIODLASTYEAR

### Notes
- Mark `dim_date` as a **Date Table** in Power BI (table tools → mark as date table, date column = `date_day`) — required for time intelligence functions to work correctly.
- All fact/mart tables relate to `dim_date` via their date/month column cast to date type.
- `mart_employee_addon_performance` relates to `dim_employees_gold` on `employee_id` (use this instead of duplicating employee attributes in the mart).
- Match data types exactly between agg and detail fact — type mismatches break agg matching silently.

**Open questions**
- Refresh cadence — pipeline currently triggered manually via Master Orchestrator; consider scheduling for the portfolio demo.

---

## Known bugs / issues

- **`mart_monthly_product_sales` incremental filter is wrong**: compares `o.order_timestamp` (TIMESTAMP) against `MAX(sales_month)` (DATE_TRUNC to month). On a partial month, re-runs will miss new rows from the current month because the max sales_month equals the start of that month and orders later in that month won't satisfy `order_timestamp > first_of_month`. Fix: use `DATE_TRUNC('month', order_timestamp) >= (SELECT MAX(sales_month) FROM {{ this }})` and delete+reinsert the latest month, or switch to a delete+insert incremental strategy.

---

## How to work in this repo

```bash
# Start infrastructure
docker-compose up -d

# dbt (run from dbt_alfa/ or pass --project-dir)
export SF_PASSWORD=<from docker-compose.yaml>
cd dbt_alfa
dbt deps
dbt run --select staging        # build silver views
dbt run --select marts          # build gold tables
dbt run --select mart_monthly_product_sales mart_hourly_traffic_conversion
dbt test

# Trigger full pipeline via Airflow UI
# http://localhost:8082  (admin / admin)
# DAG: 05_Master_Orchestrator → trigger manually
```

Profiles file: `profiles.yml` at repo root (mounted into Airflow container at `/opt/airflow/profiles.yml`).  
Snowflake connection target: `dev` (in `dbt_alfa` profile).  
`SF_PASSWORD` is set as env var in `docker-compose.yaml` and must be exported locally for direct `dbt` runs.

---

## What Claude should do in this repo

- **Always read this file first.** Then `README.md` and `dbt_project.yml`.
- **Don't edit `dags/00–03` or `staging/` models** without explicit ask — pipelines and silver layer are stable.
- **Active work is in the gold layer** — `marts/` models and any new `agg_*` models for Power BI.
- When proposing aggs, **show the SQL plus a row-count estimate** before materializing — agg should be ≥10x smaller than the detail fact.
- Run `dbt test` after any model change. If tests don't exist for a new model, add them (`unique`, `not_null` on grain keys; `relationships` to dims).
- For Snowflake queries, prefer `dbt show` or compile + run via the configured connection — don't hardcode credentials.

---

## Glossary / domain notes

- **traffic**: website visit events (`FACT_TRAFFIC`); one row per page visit, linked to an order if the visitor converted.
- **item_in_cart**: `'yes'` / `'no'` flag on a traffic event — used to count cart additions.
- **service_type / service_price**: add-on services bundled with product orders (e.g. warranty, installation); `service_type = 'None'` is normalized to NULL in staging.
- **exit_date**: employee exit date; defaulted to `'9999-12-31'` for active employees in staging.
- **month_prod_id**: surrogate key in `mart_monthly_product_sales` — MD5 of `(sales_month, product_id)`.
- **Delta Load**: the project's term for incremental processing — all layers process only new rows since the last run.

---

## Changelog

- *2026-04-28* — all [TO FILL] sections populated from codebase read; known bugs section added; data model table added. Fixed incremental bug in mart_monthly_product_sales. Fixed schema naming (SILVER_GOLD → GOLD) via generate_schema_name macro. Added dim_date, mart_traffic_conversion_by_product, mart_employee_addon_performance. Full Power BI design plan documented.
- *YYYY-MM-DD* — file scaffolded by Cowork session before VS Code handoff.
