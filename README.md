# 🧩 Pipeline ETL de Demonstrações Financeiras

> 🔗 [English version](README.en.md)

Projeto baseado em **dados reais** de demonstrações financeiras das **Top 10 empresas do setor de comércio brasileiro por receita em 2024**.
> O pipeline padroniza dados contábeis, modela dimensões e fato e calcula **indicadores financeiros fundamentais**, como:
>
> - Liquidez (corrente, geral e imediata)
> - Rentabilidade (ROE, ROA e margens)
> - Estrutura de capital e endividamento
> - Geração e conversão de caixa
> - Evolução financeira ano a ano (YoY)

---

## ▣ Visão Geral

Este projeto apresenta um pipeline **ETL (Extract, Transform, Load)** aplicado a dados de demonstrações financeiras.

O dataset é propositalmente reduzido (Top 10 empresas) para facilitar leitura, avaliação técnica e entendimento da arquitetura.

O objetivo é demonstrar:

- Extração de dados a partir de SQL
- Padronização e modelagem dimensional
- Construção de tabelas analíticas
- Cálculo de indicadores financeiros
- Carga dos dados em um Data Warehouse (SQLite)
---

## 🧱 Arquitetura do Pipeline

```text
Query SQL
   |
   v
CSV Bruto
   |
   v
Extract
   |
   v
Transform
   |-- Dados Padronizados
   |-- Dimensões e Fato
   |
   v
Analytics
   |-- Tabela Wide
   |-- Indicadores Financeiros
   |-- Evolução Temporal
   |
   v
Load (SQLite Data Warehouse)


```
Cada etapa do pipeline possui responsabilidade clara e módulos separados.

---

## 📁 Estrutura do Projeto

```text
data/
├── raw/
│   └── top10_empresas_comercio_receita_2024.csv
├── processed/
│   ├── extract/
│   ├── standardized/
│   ├── dimensions_fact/
│   └── analytics/
├── warehouse/
│   └── balance_dw.db

sql/
└── top10_empresas_comercio_receita_2024.sql

src/
├── extract/
├── transform/
├── analytics/
├── load/
├── utils/
└── pipeline.py
```

---

## ▶ Etapas do Pipeline

### 1. Extract
- Leitura de CSV gerado a partir de uma query SQL
- Validações básicas de estrutura

### 2. Transform
- Padronização de colunas e tipos
- Separação em dimensões e tabela fato


### 3. Analytics
- Criação da tabela wide para análises
- Cálculo de indicadores financeiros
- Análises de evolução temporal (YoY)

### 4. Load
- Carga das tabelas finais em banco SQLite



---

## 🧮 Tecnologias Utilizadas

- Python
- Pandas
- SQLite
- SQL
- Git

---

## ℹ Observações

- O pipeline assume estrutura específica de dados conforme a query SQL fornecida.
- O foco está na arquitetura ETL e organização do projeto
- O banco SQLite é gerado automaticamente durante a execução da etapa Load e não é versionado no repositório.


---

## 📎 Contexto Acadêmico

Este projeto também foi utilizado como entrega prática em um bootcamp de
formação em Python para dados promovido pelo Santander em parceria com a DIO,
servindo como exercício aplicado de ETL e organização de pipelines.

---

## ✔ Status do Projeto

Projeto finalizado e desenvolvido para fins de estudo e portfólio técnico.
