import os
import pandas as pd

RAW_DIR = "../data/raw"
PROCESSED_PATH = "../data/processed/balancos_processed.csv"


def load_raw_data(csv_path: str) -> pd.DataFrame:
    """
    Carrega o CSV bruto e retorna um DataFrame.
    - Verifica se o arquivo existe.
    - Converte em DataFrame padronizado
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    print(f"[INFO] Lendo arquivo bruto: {csv_path}")
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    print(f"[INFO] Linhas carregadas: {len(df)}")

    return df


def save_processed(df: pd.DataFrame, output_path: str):
    """
    Salva o dataframe no diretório processed.
    - Cria diretório de saída se não existir.
    - Salva em CSV
    """
    out_dir = os.path.dirname(output_path)
    os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Salvando arquivo processado: {output_path}")
    df.to_csv(output_path, index=False, encoding="utf-8")


def run_extract():
    """
    Executa a etapa Extract.
    - Busca arquivos CSV no diretório raw.
    - Se não houver nenhum, interrompe o processo (não há dados para extrair).
    - Se houver mais de um, também interrompe
    - Caso exista exatamente um arquivo, define o caminho completo para ser usado na leitura.
    """
    arquivos = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado em data/raw/")

    if len(arquivos) > 1:
        raise ValueError(
            f"Mais de um CSV encontrado em data/raw/. "
            f"Mantenha apenas um arquivo. Encontrados: {arquivos}"
        )

    raw_path = os.path.join(RAW_DIR, arquivos[0])
    df_raw = load_raw_data(raw_path)
    save_processed(df_raw, PROCESSED_PATH)
    print("[OK] Extract finalizado com sucesso.")

    return df_raw
