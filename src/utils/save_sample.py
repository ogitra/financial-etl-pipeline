import os
import pandas as pd


def save_sample(df: pd.DataFrame, output_dir: str, sample_name: str):
    """
    Salva um sample do DataFrame
    """

    # Garante que o diretório existe
    os.makedirs(output_dir, exist_ok=True)

    # Definir nome do arquivo
    fname = f"{sample_name}_sample.csv" if sample_name else "sample.csv"
    output_path = os.path.join(output_dir, fname)

    sample_df = df.sample(frac=1, random_state=1).head(100)
    sample_df.to_csv(output_path, index=False)

    print(f"[OK] Sample salvo em: {output_path}")
