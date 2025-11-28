import os
import sqlite3
import pandas as pd

DIM_EMP_PATH = "../data/processed/dim_empresa.csv"
DIM_CONTA_PATH = "../data/processed/dim_conta.csv"
FATO_PATH = "../data/processed/fato_balanco.csv"
DB_PATH = "../data/warehouse/balanco_dw.db"


def load_csv(path: str) -> pd.DataFrame:
    """
    - Lê um arquivo CSV do caminho especificado.
    - Verifica se o arquivo existe.
    - Retorna os dados em um DataFrame do pandas.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    print(f"[INFO] Lendo CSV: {path}")
    return pd.read_csv(path)


def connect_db(db_path: str):
    """
    - Cria (ou conecta) ao banco SQLite no caminho especificado.
    - Garante que o diretório do banco exista.
    - Retorna objeto de conexão SQLite.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    print(f"[OK] Conectado ao banco: {db_path}")
    return conn


def create_tables(conn):
    """
    - Cria tabelas dimensão e fato no SQLite.
    - Tabelas: dim_empresa, dim_conta, fato_balanco.
    - Define chaves primárias e relacionamentos via FOREIGN KEY.
    """
    cursor = conn.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_empresa (
        id_empresa INTEGER PRIMARY KEY,
        nome_empresa TEXT
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_conta (
        id_conta INTEGER PRIMARY KEY,
        codigo_conta TEXT,
        descricao_conta TEXT
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS fato_balanco (
        id_empresa INTEGER,
        id_conta INTEGER,
        data_fechamento TEXT,
        valor INTEGER,
        FOREIGN KEY (id_empresa) REFERENCES dim_empresa(id_empresa),
        FOREIGN KEY (id_conta) REFERENCES dim_conta(id_conta)
    );
    """
    )

    conn.commit()
    print("[OK] Tabelas criadas com sucesso.")


def load_to_sqlite(df: pd.DataFrame, table_name: str, conn, if_exists="replace"):
    """
    - Insere dados de um DataFrame no SQLite.
    - Parâmetros:
        df: DataFrame a ser carregado.
        table_name: nome da tabela destino.
        conn: conexão SQLite.
        if_exists: comportamento caso a tabela já exista (default = replace).
    - Remove índice para manter tabela limpa.
    """

    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    print(f"[OK] Tabela carregada: {table_name} ({len(df)} registros)")


def run_load():
    """
    Executa a etapa Load: carrega dimensões e fato no banco.
    """
    # Ler os arquivos CSV

    df_emp = load_csv(DIM_EMP_PATH)
    df_conta = load_csv(DIM_CONTA_PATH)
    df_fato = load_csv(FATO_PATH)

    # Conectar/criar banco

    conn = connect_db(DB_PATH)

    # Criar tabelas

    create_tables(conn)

    # Carregar dados para o banco

    load_to_sqlite(df_emp, "dim_empresa", conn)
    load_to_sqlite(df_conta, "dim_conta", conn)
    load_to_sqlite(df_fato, "fato_balanco", conn)

    conn.close()
    print("\n[OK] Load finalizado com sucesso!")
