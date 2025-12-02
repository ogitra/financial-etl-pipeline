import os
import pandas as pd


def save_sample(df: pd.DataFrame, output_dir: str, sample_type: str):
    """
    Salva um sample do DataFrame conforme o tipo:
    - dim_conta → salva tudo
    - outros → salva 100 linhas embaralhadas (shuffle antes)
    """
    # Nome do arquivo sempre definido
    fname = f"{sample_type}_sample.csv" if sample_type else "sample.csv"
    output_path = os.path.join(output_dir, fname)

    if sample_type == "dim_conta":
        # Salva tudo
        df.to_csv(output_path, index=False)
    else:
        # Cria sample de 100 linhas, com variedade (shuffle antes)
        sample_df = df.sample(frac=1, random_state=1).head(100)
        sample_df.to_csv(output_path, index=False)

    print(f"[OK] Sample salvo em: {output_path}")
    return df
