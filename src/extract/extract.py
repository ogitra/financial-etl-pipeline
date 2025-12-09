import os
import pandas as pd
from utils.logger import logger

RAW_DIR = "../data/raw"


def load_raw_data(csv_path: str) -> pd.DataFrame:
    """
    Carrega o CSV bruto e retorna um DataFrame.
    - Verifica se o arquivo existe.
    - Converte em DataFrame padronizado
    - Tenta ler em UTF-8, se falhar usa latin1.

    """
    if not os.path.exists(csv_path):
        logger.error(f"Arquivo não encontrado: {csv_path}")
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    logger.info(f"Lendo arquivo bruto: {csv_path}")

    try:
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        logger.warning("Falha ao ler em UTF-8. Tentando latin1...")
        df = pd.read_csv(csv_path, sep=";", encoding="latin1", low_memory=False)

    logger.info(f"Linhas carregadas: {len(df)}")

    return df


def run_extract():
    """
    Executa a etapa Extract.
    - Busca arquivos CSV no diretório raw.
    - Se não houver nenhum, interrompe o processo (não há dados para extrair).
    - Se houver mais de um, também interrompe
    - Caso exista exatamente um arquivo, define o caminho completo para ser usado na leitura.
    """
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]

    if not files:
        logger.error("Nenhum arquivo CSV encontrado em data/raw/")
        raise FileNotFoundError("Nenhum arquivo CSV encontrado em data/raw/")

    if len(files) > 1:
        logger.warning(
            f"Mais de um CSV encontrado em data/raw/. "
            f"Mantenha apenas um arquivo. Encontrados: {files}"
        )
        raise ValueError(
            f"Mais de um CSV encontrado em data/raw/. "
            f"Mantenha apenas um arquivo. Encontrados: {files}"
        )

    raw_path = os.path.join(RAW_DIR, files[0])
    df_raw = load_raw_data(raw_path)

    return df_raw
