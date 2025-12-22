# 🧩 ETL Pipeline for Financial Statements

> 🔗 [Portuguese version](README.md)

This project implements an end-to-end **ETL pipeline for real financial statements** from the **Top 10 Brazilian retail companies by revenue (2024)**.
It covers SQL extraction, data standardization, dimensional modeling, KPI calculation, and loading into both a local Data Warehouse and a cloud Data Lake.

---

## ⚡ Quick Overview

**What the pipeline does:**
- Extracts structured financial data using an advanced SQL query (**CTEs + Window Functions**).
- Standardizes and transforms raw data into a dimensional model (**fact + dimensions**).
- Builds a consolidated analytical dataset (**wide table**) for exploration.
- Computes key financial metrics (liquidity, profitability, leverage, cash-related KPIs) and time-based evolution metrics (e.g., YoY).
- Loads curated datasets into:
  - **SQLite** (local Data Warehouse)
  - **Amazon S3** (Data Lake, stored as CSV objects)

**Skills demonstrated:**
- Advanced SQL (CTEs, window functions, conditional filters, aggregations)
- Python + Pandas for data cleaning, transformation, and modeling
- Dimensional modeling (company & account dimensions + financial fact table)
- Modular ETL design (**extract → transform → load**)
- Financial logic (ROE, ROA, margins, liquidity, leverage, cash metrics)
- AWS integration (S3 as a Data Lake destination)
- Professional project structure and reproducibility

---
**Main links:**
🔗 [SQL Query](sql/top10_empresas_comercio_receita_2024.sql) |
🔗 [Pipeline Orchestrator](src/pipeline.py)

For the full project documentation (PT-BR), see the main [README](README.md)
