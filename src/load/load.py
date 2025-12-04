import os
import sqlite3
import pandas as pd

# Caminhos dos samples
SAMPLES_DIR = "../data/sample"
DIM_EMP_SAMPLE = os.path.join(SAMPLES_DIR, "dim_empresa_sample.csv")
DIM_CONTA_SAMPLE = os.path.join(SAMPLES_DIR, "dim_conta_sample.csv")
FATO_SAMPLE = os.path.join(SAMPLES_DIR, "fato_sample.csv")
WIDE_SAMPLE = os.path.join(SAMPLES_DIR, "wide_sample.csv")
INDICATORS_SAMPLE = os.path.join(SAMPLES_DIR, "indicators_sample.csv")
EVOLUTION_SAMPLE = os.path.join(SAMPLES_DIR, "evolution_sample.csv")

# Caminho do banco
DB_PATH = "../data/warehouse/balanco_dw.db"


def load_csv(path: str) -> pd.DataFrame:
    """Lê um arquivo CSV e retorna DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    logger.info(f"[INFO] Lendo CSV: {path}")
    return pd.read_csv(path)


def connect_db(db_path: str):
    """Cria (ou conecta) ao banco SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    logger.info(f"[OK] Conectado ao banco: {db_path}")
    return conn


def create_tables(conn):
    """Cria tabelas de samples no SQLite."""
    cursor = conn.cursor()

    # Samples das dimensões e fato
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_empresa_sample (
        id_empresa INTEGER,
        nome_empresa TEXT
    );
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_conta_sample (
        id_conta INTEGER,
        codigo_conta TEXT,
        descricao_conta TEXT
    );
    """
    )
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fato_sample (
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
    CREATE TABLE IF NOT EXISTS sample_wide (
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
    CREATE TABLE IF NOT EXISTS sample_indicators (
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
    CREATE TABLE IF NOT EXISTS sample_evolution (
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
    logger.info("[OK] Tabelas de samples criadas com sucesso.")


def load_to_sqlite(df: pd.DataFrame, table_name: str, conn, if_exists="replace"):
    """Insere dados de um DataFrame no SQLite."""
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"[OK] Tabela carregada: {table_name} ({len(df)} registros)")


def run_load():
    """Executa a etapa Load: carrega todos os samples no banco."""
    # Ler os samples
    df_emp_sample = load_csv(DIM_EMP_SAMPLE)
    df_conta_sample = load_csv(DIM_CONTA_SAMPLE)
    df_fato_sample = load_csv(FATO_SAMPLE)
    df_wide = load_csv(WIDE_SAMPLE)
    df_indicators = load_csv(INDICATORS_SAMPLE)
    df_evolution = load_csv(EVOLUTION_SAMPLE)

    # Conectar/criar banco
    conn = connect_db(DB_PATH)

    # Criar tabelas
    create_tables(conn)

    # Carregar samples
    load_to_sqlite(df_emp_sample, "dim_empresa_sample", conn)
    load_to_sqlite(df_conta_sample, "dim_conta_sample", conn)
    load_to_sqlite(df_fato_sample, "fato_sample", conn)
    load_to_sqlite(df_wide, "sample_wide", conn)
    load_to_sqlite(df_indicators, "sample_indicators", conn)
    load_to_sqlite(df_evolution, "sample_evolution", conn)

    conn.close()
    logger.info("[OK] Load finalizado com sucesso!")
