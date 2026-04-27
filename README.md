# 🚀 Alfa Stream v4.5: Hybrid Cloud Data Platform

**CZ:** Alfa Stream v4.5 je komplexní end-to-end datová platforma simulující reálný e-commerce provoz. Projekt demonstruje orchestraci hybridního cloudu, pokročilé dbt modelování a plně inkrementální datové toky (Delta Load).

**EN:** Alfa Stream v4.5 is a comprehensive end-to-end data platform simulating real-world e-commerce operations. The project demonstrates hybrid-cloud orchestration, advanced dbt modeling, and full incremental data flows (Delta Load).

---

## 🏗️ Architecture / Architektura

```mermaid
graph TD
    %% Airflow jako centrální mozek
    subgraph Control_Plane ["Orchestration Layer - Airflow"]
        MD["05_Master_Orchestrator"]        
               
        subgraph Pipelines ["Individual Pipelines"]
            P0["00_HR_Gen"]
            P1["01_Sales_Gen"]
            P2["02_Postgres_Load"]
            P3["03_Cloud_Sync"]
            P4["04_dbt_Transform"]
        end
    end

    %% Datové vrstvy
    subgraph Storage_Layer ["Data Storage & Processing"]
        direction TB
        subgraph Local ["Local Environment - Docker"]
            PE["Python Data Engines"]
            PG[("Postgres ODS")]
        end

        subgraph Cloud ["Cloud Warehouse - Snowflake"]
            SFR[[Snowflake RAW]]
            DBT{"dbt Transformations"}
            SFG[[Snowflake GOLD]]
        end
    end

    %% Řídící toky
    MD ==> P0
    MD ==> P1
    MD ==> P2
    MD ==> P3
    MD ==> P4

    P0 & P1 -.-> PE
    P2 -.-> PG
    P3 -.-> SFR
    P4 -.-> DBT

    %% Datové toky
    PE ===> PG
    PG ===> SFR
    SFR ===> DBT
    DBT ===> SFG

    %% Výstup
    SFG ---> PBI(("Power BI Dashboards"))

    %% Stylování
    style Control_Plane fill:#f9f9f9,stroke:#333,stroke-dasharray: 5 5
    style MD fill:#f9f,color:#000,stroke-width:3px
    style Pipelines fill:#fff,stroke:#00a1ff
    style DBT fill:#ff694b,color:#fff,stroke-width:2px
    style SFG fill:#ffd700,color:#000,stroke-width:2px
    style PBI fill:#fb0,stroke:#333
```

---

## ⚙️ Pipelines Overview / Přehled procesů

**CZ:** Systém je řízen Master Orchestrátorem (DAG 05), který sekvenčně spouští:
- **00 & 01A:** Správa Master dat (Zaměstnanci, Produkty).
- **01B & 01C:** Inkrementální generování Trafficu a Objednávek (Sezónnost, Peak Hours).
- **02 & 03:** Load do lokálního Postgresu a následný vektorizovaný přesun do Snowflake RAW.
- **04:** Inkrementální dbt transformace do vrstev SILVER (Staging) a GOLD (Business Marts).

**EN:** The system is governed by a Master Orchestrator (DAG 05), executing sequentially:
- **00 & 01A:** Master Data Management (Employees, Products).
- **01B & 01C:** Incremental generation of Traffic and Orders (Seasonality, Peak Hours).
- **02 & 03:** Load to local PostgreSQL and vectorized transfer to Snowflake RAW.
- **04:** Incremental dbt transformations into SILVER (Staging) and GOLD (Business Marts).

---

## 🌟 Key Features / Klíčové funkce

**CZ:**
- **Incremental Logic (Delta Load):** Všechny vrstvy od simulace po dbt zpracovávají pouze nové přírůstky, což minimalizuje náklady na Snowflake compute.
- **Advanced dbt Modeling:** Transformace surových dat do granulárních byznys pohledů (Hourly Traffic, Monthly Sales Performance).
- **Enterprise Orchestration:** Robustní Master DAG s logikou `wait_for_completion` a automatickým testováním kvality dat (`dbt test`).
- **Snowflake Stability:** Vyřešení kritických metadatových konfliktů při cloudovém nahrávání (Manual Drop & Append strategy).

**EN:**
- **Incremental Logic (Delta Load):** All layers, from simulation to dbt, process only new increments, minimizing Snowflake compute costs.
- **Advanced dbt Modeling:** Transformation of raw data into granular business views (Hourly Traffic, Monthly Sales Performance).
- **Enterprise Orchestration:** Robust Master DAG with `wait_for_completion` logic and automated data quality checks (`dbt test`).
- **Snowflake Stability:** Resolved critical metadata conflicts during cloud ingestion (Manual Drop & Append strategy).

---

## 🛠️ Tech Stack / Technologie

- **Orchestration:** Apache Airflow (LocalExecutor ready)
- **Data Engineering:** Python (Pandas), SQL
- **Database:** PostgreSQL (Docker), Snowflake (Cloud)
- **Transformation:** dbt Core (Incremental Materialization)
- **Visualization:** Power BI

---

## 📖 Documentation / Dokumentace

**CZ:** Kompletní technický popis, schéma databáze a řešení problémů naleznete ve složce:

**EN:** Full technical description, database schema, and troubleshooting can be found in the folder:

👉 [**Project Documentation Folder (CZ/ENG)**](./documentation/)

---

**Author:** David Urban  
**Status:** Production v4.5 (Orchestrated & Incremental)
