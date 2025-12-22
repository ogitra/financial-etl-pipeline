# 🧩 Pipeline ETL de Demonstrações Financeiras
🔗 [- English (Short Version)](README.en.md)



## ⚡ Resumo Rápido do Projeto e Habilidades Demonstradas

**Este projeto demonstra, de ponta a ponta, um pipeline ETL aplicado a dados financeiros reais — abrangendo desde consultas SQL até a modelagem dimensional e o cálculo de indicadores, culminando na carga em um Data Warehouse (SQLite) e em um Data Lake (Amazon S3).**


**O que o pipeline faz:**
- Extrai dados contábeis estruturados via SQL robusto (CTEs + Window Functions).
- Padroniza e transforma dados brutos em modelos dimensionais (dimensões + fato).
- Gera tabela analítica consolidada (wide table) para análises.
- Calcula indicadores financeiros essenciais (liquidez, rentabilidade, endividamento, caixa) e evolução temporal (YoY).
- Carrega os datasets finais em:
  - **SQLite** (Data Warehouse local)
  - **Amazon S3** (Data Lake)


**Competências demonstradas:**
- SQL avançado (CTEs, janelas, agregações, filtros condicionais).
- Python + Pandas aplicado a dados financeiros.
- Modelagem dimensional (dim_company, dim_account, fact_balance).
- Transformações analíticas (wide_table, indicadores e evolução).
- Integração com AWS (S3) como destino de dados.
- Arquitetura modular (extract → transform → load).
- Organização profissional de projeto (estrutura, versionamento, reprodutibilidade).
---

# 📊 Projeto — Pipeline ETL de Demonstrações Financeiras

Projeto baseado em **dados reais** de demonstrações financeiras das **Top 10 empresas do setor de comércio brasileiro por receita em 2024**.

O pipeline padroniza dados contábeis, modela dimensões e fato e calcula **indicadores financeiros fundamentais**, como:

- Liquidez (corrente, geral e imediata)
- Rentabilidade (ROE, ROA e margens)
- Estrutura de capital e endividamento
- Geração e conversão de caixa
- Evolução financeira ano a ano (YoY)

---

## ✨  Destaque: Query SQL
🔗 [Query SQL](sql/top10_empresas_comercio_receita_2024.sql)

A extração dos dados foi feita a partir de uma **query SQL completa, construída para filtrar, agregar e preparar as demonstrações financeiras antes mesmo do ETL em Python**.
A query utiliza:

- **CTEs** para modularizar etapas (filtro por setor, ranking de receita, base contábil).
- **Window Functions** como `ROW_NUMBER()` e `SUM() OVER(PARTITION BY...)`.
- **Filtros por ano** para criar dataset entre 2022 e 2024.
- **Junções entre tabelas contábeis** para consolidar informações.
- **Padronização de colunas** para facilitar o Transform.

### Trecho ilustrativo (exemplo reduzido):

```sql
WITH balancos_normalizados AS (
    SELECT
        b.IdPessoaJuridica,
        b.DataFechamento,
        bc.IdConta,

        /* Padronização monetária */
        CASE
            WHEN um.IdUnidadeMonetaria = -3 THEN bc.Valor * 1000000
            WHEN um.IdUnidadeMonetaria = -2 THEN bc.Valor * 1000
            ELSE bc.Valor
        END AS ValorPadronizado,

        /* Regra de prioridade entre balanços */
        ROW_NUMBER() OVER (
            PARTITION BY
                b.IdPessoaJuridica,
                b.DataFechamento,
                bc.IdConta
            ORDER BY
                CASE
                    WHEN b.IdNaturezaBalanco = -2 THEN 1   -- Consolidado
                    WHEN b.IdNaturezaBalanco = -1 THEN 2   -- Individual
                    ELSE 3
                END
        ) AS rn

    FROM Balanco b
    JOIN BalancoConta bc
        ON bc.IdBalanco = b.IdBalanco
    JOIN UnidadeMonetaria um
        ON b.IdUnidadeMonetaria = um.IdUnidadeMonetaria

    WHERE
        b.IdPeriodoBalanco = -3                         -- balanço anual
        AND b.DataFechamento BETWEEN '2022-12-31' AND '2024-12-31'
        AND bc.IdConta IN (
            55, 56, 57, 73, 103,
            178, 230, 286,
            332, 333, 335, 347, 375,
            421, 462, 473
        )
        AND bc.Valor IS NOT NULL
        AND bc.Valor <> 0
),

```
---
## ✨  Destaque — Python
🔗 [Orquestrador Pipeline](src/pipeline.py)

O pipeline utiliza Python de forma modular e organizada, cobrindo práticas valorizadas no mercado de dados:

### ✓ Arquitetura e organização
- Estrutura em **módulos independentes** (`extract`, `transform`, `load`)
- Script **orquestrador** (`pipeline.py`)
- Separação clara de responsabilidades (clean code aplicado)

### ✓ Processamento e manipulação de dados
- Uso de **Pandas** para padronização, limpeza e transformação
- Conversão de tipos, parsing de datas e validação de schema
- Criação de **tabelas fato** e **dimensões** a partir de dataframes
- Camada analítica dentro do Transform (transform/analytics)

### ✓ Integração com destino de dados
- SQLite (Data Warehouse local) para persistir as tabelas finais
- Amazon S3 (Data Lake) para armazenar os datasets finais como objetos CSV
- Automação de todo o fluxo com um único comando (`python pipeline.py`)


---


## 🧱 Arquitetura do Pipeline

```text
Query SQL (CTEs + Window Functions + Filtros Anuais)
   |
   v
CSV Bruto (sample versionado em data/raw)
   |
   v
Extract (DataFrame)
   |
   v
Transform
   |-- Padronização (standardize)
   |-- Modelagem (dim_company, dim_account, fact_balance)
   |-- Wide Table (wide_table)
   |-- Analytics (KPIs e evolução)
   |
   v
Load (SQLite Data Warehouse)
   |-- SQLite (DW local)
   |-- S3 (Data Lake)

```

---

## 📦 Outputs gerados
 O pipeline gera os seguintes datasets finais:

- dim_company
- dim_account
- fact_balance
- wide_table
- financial_indicators
- financial_evolution

## 🗄️ SQLite (Data Warehouse local)

O arquivo é gerado automaticamente em:
data/warehouse/balance_dw.db (não versionado no Git)

## ☁️ Amazon S3 (Data Lake)

Os datasets são enviados como CSV para o bucket configurado no loader do S3, organizados por camadas:
processed/dimensions/dim_company/dim_company.csv
processed/dimensions/dim_account/dim_account.csv
processed/facts/fact_balance/fact_balance.csv
processed/wide/wide_table.csv
analytics/financial_indicators.csv
analytics/financial_evolution.csv

---

## 📁 Estrutura do Projeto

```text
data/
├── raw/
│   └── top10_empresas_comercio_receita_2024.csv   # sample (input)
├── warehouse/
│   └── balance_dw.db                             # gerado em runtime (não versionado)

sql/
└── top10_empresas_comercio_receita_2024.sql

src/
├── extract/
├── transform/
│   └── analytics/
├── load/
│   ├── sqlite_loader.py
│   └── s3_loader.py
├── utils/
└── pipeline.py
```

---

## ▶ Etapas do Pipeline

### 1. Extract
- CSV gerado previamente via query SQL avançada
- Leitura do CSV em DataFrame

### 2. Transform
- Padronização de colunas e tipos
- Criação de dimensões (empresa, conta, data)
- Construção da tabela fato
- Geração de tabela wide
- Cálculo de indicadores financeiros e evolução temporal

### 3. Load
- Carga em SQLite
- Upload dos datasets finais para o S3 da AWS

---




## ▶ Como Reproduzir o Projeto

Este projeto pode ser reproduzido localmente utilizando o arquivo CSV disponibilizado na pasta `data/raw`.

### Pré-requisitos
- Python 3.10 ou superior
- Git
- (Opcional para S3) AWS CLI configurado e bucket S3 criado
### Passo a passo

#### 1. Clone o repositório
```bash
git clone https://github.com/ogitra/financial-etl-pipeline.git
cd financial-etl-pipeline
```

#### 2. Crie e ative um ambiente virtual
```bash
python -m venv venv
# Linux / Mac
source venv/bin/activate
# Windows
venv\Scripts\activate
```

#### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

#### 4. (Opcional) Configurar AWS para upload no S3
- Crie um bucket S3
- Crie um usuário IAM com permissão mínima (s3:PutObject e s3:ListBucket)
- Configure as credenciais localmente:

*Se você quiser executar apenas o SQLite (sem AWS), basta comentar a chamada do loader S3 no pipeline.py.*



#### 5. Execute o pipeline ETL

O pipeline deve ser executado **a partir do diretório `src`**:

```bash
cd src
python pipeline.py
```


### Resultados esperados

Isso executará todas as etapas do pipeline:

- Extract
- Transform
- Load (SQLite + S3)



---

## ℹ Observações

- Cada etapa do pipeline possui responsabilidade clara e módulos separados.
- O projeto assume a estrutura de dados fornecida no arquivo CSV localizado em `data/raw`.
- O banco SQLite é gerado automaticamente durante a execução e não é versionado no repositório.

---

## ✔ Status do Projeto
Projeto finalizado e desenvolvido para fins de estudo e portfólio técnico.
