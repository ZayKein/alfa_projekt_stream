# Alfa Stream v5.0 — End-to-End Data Platform

> 🇨🇿 [Česká verze](./README_CZ.md)

**Alfa Stream** is a personal portfolio project built to showcase my skills across the full data spectrum — combining **data engineering**, **data analytics**, and **machine learning** in one end-to-end platform. I designed and built the entire stack from scratch — data simulation, pipeline orchestration, cloud warehousing, transformation modeling, a business intelligence layer, and predictive ML models on top.

The platform simulates a real e-commerce operation: products are listed, customers browse and place orders, employees handle add-on services. All of this data flows automatically through a multi-layer pipeline into a reporting-ready Snowflake data warehouse, where Power BI dashboards surface the business insights.

**Author:** David Urban

---

## Live Power BI Report

**[View the Power BI report](https://app.powerbi.com/view?r=eyJrIjoiZDQ2NTExYjEtOTZkYi00OGZjLTg2MDAtMjI0ZjJmODQxMjM0IiwidCI6ImI2ZWUwY2QxLWVhYWQtNDZkOC05YzYzLTJmZTgxNGFjOWZjNSJ9&pageName=353600777c1baa3bc046)**

The Power BI layer is the primary output of this project — a composite model (Import + DirectQuery) built on top of the Snowflake GOLD layer with four dashboard pages covering sales, product performance, traffic & conversion, and product ratings.

> **Work in progress** — approximately 40 hours of development invested. Pages 1–2 (Sales Overview, Product Performance) are fully complete. Page 3 (Traffic & Conversion) needs minor polishing. Page 4 (Product Rating) is approximately 50% complete and will be finished as soon as personal circumstances allow.

---

## What the platform does

1. **Generates realistic data** — Python scripts simulate product catalog, employee roster, web traffic events, and orders with seasonality and peak-hour patterns.
2. **Stores it locally** — all generated data lands in a PostgreSQL container (the ODS layer) before being pushed to the cloud.
3. **Loads to Snowflake** — a vectorized DAG transfers data from Postgres into Snowflake's RAW schema.
4. **Transforms with dbt** — dbt Core models clean, join, and aggregate the raw data into a SILVER (staging) and GOLD (business) layer using fully incremental (Delta Load) logic.
5. **Reports in Power BI** — a composite model (Import + DirectQuery) connects to the GOLD layer, with pre-built DAX measures and five dashboard pages covering sales, products, traffic, hourly patterns, and employee performance.
6. **Applies ML models** — three independent models run as DAG 06 after every pipeline cycle: Facebook Prophet forecasts the next 6 months of revenue per product category (`ML_REVENUE_FORECAST`); Z-score analysis flags statistical anomalies in product conversion rates and employee attach rates (`ML_ANOMALY_FLAGS`); Ridge Regression predicts expected traffic for all 168 hour × day combinations (`ML_TRAFFIC_PREDICTION`). All outputs land in Snowflake GOLD and are visualised directly in Power BI.

Everything runs automatically via a Master Orchestrator DAG in Apache Airflow.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.7.1 (Docker, LocalExecutor) |
| Data generation | Python (Pandas) |
| Local staging | PostgreSQL 15 (Docker) |
| Cloud warehouse | Snowflake |
| Transformation | dbt Core (incremental materialization) |
| Reporting | Power BI (composite model, DirectQuery + Import) |
| ML / Predictions | Python — Prophet (revenue forecast), Z-score (anomaly detection), Ridge Regression (traffic patterns) |

---

## Key Design Choices

- **Delta Load everywhere** — from generators through to dbt models, only new rows are processed on each run. This keeps Snowflake compute costs minimal.
- **Three-layer Snowflake architecture** — RAW (as-landed), SILVER (cleaned views), GOLD (business-ready tables and mart aggregations).
- **Pre-aggregated marts** — five mart tables serve as the primary targets for Power BI visuals, reducing query load on DirectQuery and enabling aggregation awareness.
- **Composite Power BI model** — small dimension tables are imported for speed; large fact and mart tables stay in DirectQuery for freshness.
- **Integrated ML layer** — three models (Prophet forecasting, Z-score anomaly detection, Ridge Regression traffic prediction) run inside the Airflow pipeline after every dbt cycle as parallel tasks, so all predictions are always based on the latest data without any manual intervention.

---

## Repo Structure

```
alfa_projekt_stream/
├── dags/
│   ├── 00_hr_generator.py              # HR master data generator
│   ├── 01_A_Alfa_Products.py           # Product master data
│   ├── 01_B_Alfa_Traffic.py            # Incremental traffic event generator
│   ├── 01_C_Alfa_Orders.py             # Incremental order generator
│   ├── 02_Load_To_Postgres_Full.py     # Load all data to local Postgres
│   ├── 03_Postgres_to_Snowflake.py     # Vectorized transfer Postgres → Snowflake RAW
│   ├── 04_Snowflake_Transformation.py  # Triggers dbt run + dbt test
│   ├── 05_Master_Orchestrator.py       # Sequential trigger of all DAGs above
│   └── 06_ML_Predictions.py            # Prophet forecast, Z-score anomaly detection, Ridge traffic prediction
├── dbt_alfa/
│   └── models/
│       ├── staging/                    # SILVER schema — cleaned views
│       └── marts/                      # GOLD schema — incremental tables
├── Documentation ENG/                  # Architecture diagrams and project docs
├── docker-compose.yaml
├── profiles.yml
└── README.md
```

---

## Gold Layer — Data Models

| Model | Grain | Purpose |
|---|---|---|
| `dim_date` | one row per calendar day | Date spine for time intelligence |
| `dim_products_gold` | one row per product | Product dimension |
| `dim_employees_gold` | one row per employee | Employee dimension |
| `dim_payroll_gold` | one row per employee × month | Payroll dimension |
| `fact_orders_gold` | one row per order | Order-level detail fact |
| `mart_monthly_product_sales` | month × product | Revenue & margin aggregation |
| `mart_hourly_traffic_conversion` | truncated hour | Traffic & conversion by hour |
| `mart_traffic_conversion_by_product` | month × product | Product-level funnel (views → carts → orders) |
| `mart_city_sales` | city × day | Revenue, margin and orders aggregated by city and region |
| `mart_product_reviews` | month × product | Average review score, review count, 5-star count, % positive |
| `mart_employee_addon_performance` | month × employee | Addon attach rate & revenue by tenure |
| `ML_REVENUE_FORECAST` | month × category | Prophet 6-month revenue forecast with confidence interval |
| `ML_ANOMALY_FLAGS` | period × entity | Z-score anomaly flags for conversion rates and attach rates |
| `ML_TRAFFIC_PREDICTION` | hour × day of week | Ridge Regression predicted visit count for all 168 combinations |

---

## Power BI Dashboard Pages

1. **Sales Overview** ✅ — city-level KPI cards, monthly revenue trend with year-comparison tooltip, MoM and YoY metrics
2. **Product Performance** ✅ — revenue and margin by category and product, decomposition tree with review scores
3. **Traffic & Conversion Funnel** 🔧 — views → carts → orders by product and over time
4. **Product Rating** 🔄 — product review scores, star ratings, and score distribution

---

## Running the Project Locally

```bash
# Start infrastructure
docker-compose up -d

# Run dbt transformations (from dbt_alfa/)
export SF_PASSWORD=<from docker-compose.yaml>
cd dbt_alfa
dbt run --select staging        # build SILVER views
dbt run --select marts          # build GOLD tables
dbt test

# Trigger the full pipeline
# Airflow UI → http://localhost:8082  (admin / admin)
# DAG: 05_Master_Orchestrator → trigger manually
```

