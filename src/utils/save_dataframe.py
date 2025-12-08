import os
import pandas as pd
from utils.logger import logger


def save_dataframe(df: pd.DataFrame, output_dir: str, file_name: str):
    """
    Salva o DataFrame completo em CSV

    """

    # Garante que o diretório existe
    os.makedirs(output_dir, exist_ok=True)

    # Definir nome do arquivo
    fname = f"{file_name}.csv" if file_name else "data.csv"
    output_path = os.path.join(output_dir, fname)

    df.to_csv(output_path, index=False)

    logger.info(f"Arquivo  salvo em: {output_path}")
