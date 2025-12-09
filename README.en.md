# 🧩 ETL Pipeline for Financial Statements

> 🔗 [Versão em Português](README.md)

Project based on **real financial statement data** from the **Top 10 Brazilian commerce companies by revenue in 2024**.
> The pipeline standardizes accounting data, models dimensions and fact tables, and calculates **key financial indicators**, such as:

- Liquidity (current, quick, and cash ratio)
- Profitability (ROE, ROA, and margins)
- Capital structure and leverage
- Cash generation and conversion
- Year-over-year (YoY) financial evolution

---

## ▣ Overview

This project implements a complete **ETL (Extract, Transform, Load)** pipeline applied to corporate financial data.

The dataset is intentionally reduced (Top 10 companies) to simplify reading, technical evaluation, and architectural understanding.

The main goals are to demonstrate:

- Data extraction from SQL
- Standardization and dimensional modeling
- Construction of analytical tables
- Calculation of financial indicators
- Loading into a SQLite Data Warehouse
