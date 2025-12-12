# 🧩 Financial Statements ETL Pipeline — Summary (EN)
> 🔗 [Versão em Português](README.md)

This project implements an end-to-end **ETL pipeline for real financial statements** from the **Top 10 Brazilian retail companies by revenue (2024)**.
It covers SQL extraction, data standardization, dimensional modeling, KPI calculation, and loading into a local Data Warehouse.

---

## ⚡ Quick Overview

**What the pipeline does:**
- Extracts structured financial data using an advanced SQL query (CTEs + Window Functions).
- Standardizes and transforms raw data into a dimensional model (fact + dimensions).
- Calculates key financial indicators (liquidity, profitability, leverage, cash metrics).
- Builds a consolidated analytical table for visualization and exploration.
- Loads all curated data into a SQLite Data Warehouse.

**Skills demonstrated:**
- Advanced SQL (CTEs, window functions, conditional filters, aggregations)
- Python + Pandas for data cleaning, transformation, and modeling
- Dimensional modeling (company, account, date dimensions + financial fact table)
- Modular ETL design (extract → transform → analytics → load)
- Financial logic (ROE, ROA, margins, liquidity, leverage)
- Professional project structure and reproducibility

---

For the full project documentation (PT-BR), see the main README.
