import os
import sqlite3
import pandas as pd
from utils.logger import logger

# Caminhos dos samples
SAMPLES_DIR = "../data/sample"
DIM_COMPANY_SAMPLE = os.path.join(SAMPLES_DIR, "dim_company_sample.csv")
DIM_ACCOUNT_SAMPLE = os.path.join(SAMPLES_DIR, "dim_account_sample.csv")
FACT_BALANCE_SAMPLE = os.path.join(SAMPLES_DIR, "fact_balance_sample.csv")
WIDE_TABLE_SAMPLE = os.path.join(SAMPLES_DIR, "wide_table_sample.csv")
FINANCIAL_INDICATORS_SAMPLE = os.path.join(
    SAMPLES_DIR, "financial_indicators_sample.csv"
)
FINANCIAL_EVOLUTION_SAMPLE = os.path.join(SAMPLES_DIR, "financial_evolution_sample.csv")

# Caminho do banco
DB_PATH = "../data/warehouse/balance_dw.db"


def load_csv(path: str) -> pd.DataFrame:
    """Lê um arquivo CSV e retorna DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    logger.info(f"Lendo CSV: {path}")
    return pd.read_csv(path)


def connect_db(db_path: str):
    """Cria (ou conecta) ao banco SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    logger.info(f"Conectado ao banco: {db_path}")

    return conn


def create_tables(conn):
    """Cria tabelas de samples no SQLite."""
    cursor = conn.cursor()

    # Samples das dimensões e fato
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_company_sample (
        id_empresa INTEGER,
        nome_empresa TEXT
    );
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_account_sample (
        id_conta INTEGER,
        codigo_conta TEXT,
        descricao_conta TEXT
    );
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fact_balance_sample (
        id_empresa INTEGER,
        id_conta INTEGER,
        data_fechamento TEXT,
        valor INTEGER
    );
    """
    )

    # Sample wide
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS wide_table_sample (
        nome_empresa TEXT,
        data_fechamento TEXT,
        conta TEXT,
        valor REAL
    );
    """
    )

    # Sample indicators
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS financial_indicators_sample (
        nome_empresa TEXT,
        data_fechamento TEXT,
        liquidez_corrente_pontos REAL,
        liquidez_geral_pontos REAL,
        liquidez_imediata_pontos REAL,
        endividamento_pontos REAL,
        alavancagem_pontos REAL,
        composicao_endividamento_percent REAL,
        roe_percent REAL,
        roa_percent REAL,
        ebit REAL,
        margem_bruta_percent REAL,
        margem_operacional_percent REAL,
        margem_liquida_percent REAL,
        caixa_operacional_sobre_receita_percent REAL,
        conversao_caixa_percent REAL,
        fluxo_caixa_livre REAL,
        participacao_caixa_percent REAL
    );
    """
    )

    # Sample evolution
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS financial_evolution_sample (
        nome_empresa TEXT,
        data_fechamento TEXT,
        crescimento_receita_yoy_percent REAL,
        crescimento_lucro_yoy_percent REAL,
        crescimento_ebit_yoy_percent REAL,
        crescimento_ativo_yoy_percent REAL,
        crescimento_pl_yoy_percent REAL,
        crescimento_caixa_operacional_yoy_percent REAL,
        variacao_endividamento_yoy_pp REAL,
        margem_liquida_media_movel_percent REAL
    );
    """
    )

    conn.commit()
    logger.info("Tabelas de samples criadas com sucesso.")


def load_to_sqlite(df: pd.DataFrame, table_name: str, conn, if_exists="replace"):
    """Insere dados de um DataFrame no SQLite."""
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"Tabela carregada: {table_name} ({len(df)} registros)")


def run_load():
    """Executa a etapa Load: carrega todos os samples no banco."""
    # Ler os samples
    df_company = load_csv(DIM_COMPANY_SAMPLE)
    df_account = load_csv(DIM_ACCOUNT_SAMPLE)
    df_fact = load_csv(FACT_BALANCE_SAMPLE)
    df_wide = load_csv(WIDE_TABLE_SAMPLE)
    df_indicators = load_csv(FINANCIAL_INDICATORS_SAMPLE)
    df_evolution = load_csv(FINANCIAL_EVOLUTION_SAMPLE)

    # Conectar/criar banco
    conn = connect_db(DB_PATH)

    # Criar tabelas
    create_tables(conn)

    # Carregar samples
    load_to_sqlite(df_company, "dim_company_sample", conn)
    load_to_sqlite(df_account, "dim_account_sample", conn)
    load_to_sqlite(df_fact, "fact_balance_sample", conn)
    load_to_sqlite(df_wide, "wide_table_sample", conn)
    load_to_sqlite(df_indicators, "financial_indicators_sample", conn)
    load_to_sqlite(df_evolution, "financial_evolution_sample", conn)

    conn.close()
