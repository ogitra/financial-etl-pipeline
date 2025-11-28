import os
import pandas as pd


def load_raw_data(csv_path: str) -> pd.DataFrame:
    """
    Carrega o CSV bruto e retorna um DataFrame.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    print(f"[INFO] Lendo arquivo bruto: {csv_path}")
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", low_memory=False)
    print(f"[INFO] Linhas carregadas: {len(df)}")

    return df


def save_processed(df: pd.DataFrame, output_path: str) -> None:
    """
    Salva o dataframe no diretório processed.
    """
    out_dir = os.path.dirname(output_path)
    os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Salvando arquivo processado: {output_path}")
    df.to_csv(output_path, index=False, encoding="utf-8")


if __name__ == "__main__":

    raw_dir = "data/raw"
    arquivos = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado em data/raw/")

if len(arquivos) > 1:
    raise ValueError(
        f"Mais de um CSV encontrado em data/raw/. "
        f"Mantenha apenas um arquivo. Encontrados: {arquivos}"
    )

arquivos.sort()
RAW_PATH = os.path.join(raw_dir, arquivos[0])

PROCESSED_PATH = "data/processed/balancos_processed.csv"

df_raw = load_raw_data(RAW_PATH)
save_processed(df_raw, PROCESSED_PATH)

print("[OK] Extract finalizado com sucesso.")
