import os
import sqlite3
import pandas as pd
from utils.logger import logger

# Caminho do banco
DB_PATH = "../data/warehouse/balance_dw.db"


def connect_db(db_path: str):
    """Cria (ou conecta) ao banco SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    logger.info(f"Conectado ao banco: {db_path}")
    return conn


def create_tables(conn):
    """Cria tabelas no SQLite para cada etapa do pipeline."""
    cursor = conn.cursor()

    # Dimensões e fato
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_company (
            id_empresa INTEGER,
            NomeFantasia TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_account (
            id_conta INTEGER,
            codigo_conta TEXT,
            descricao_conta TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_balance (
            id_empresa INTEGER,
            id_conta INTEGER,
            data_fechamento TEXT,
            valor REAL
        );
        """
    )

    # Wide table (analytics)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS wide_table (
            NomeFantasia TEXT,
            data_fechamento TEXT,
            conta TEXT,
            valor REAL
        );
        """
    )

    # Analytics: indicadores e evolução
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_indicators (
            NomeFantasia TEXT,
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_evolution (
            NomeFantasia TEXT,
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
    logger.info("Tabelas criadas com sucesso.")


def write_table_sqlite(df: pd.DataFrame, table_name: str, conn, if_exists="replace"):
    """Insere dados de um DataFrame no SQLite."""
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"Tabela carregada: {table_name} ({len(df)} registros)")


def run_load_sqlite(datasets: dict):
    """Executa a etapa Load: carrega dados processados no banco."""

    conn = connect_db(DB_PATH)
    create_tables(conn)

    for table_name, df in datasets.items():
        write_table_sqlite(df, table_name, conn)

    conn.close()
