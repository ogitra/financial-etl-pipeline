import pandas as pd
from utils.logger import logger


def standardize_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza tipos das principais colunas.
    """
    df["DataFechamento"] = pd.to_datetime(df["DataFechamento"], errors="coerce")
    df["IdEmpresa"] = df["IdEmpresa"].astype(int)
    df["IdConta"] = df["IdConta"].astype(int)
    df["ValorPadronizado"] = df["ValorPadronizado"].astype("int64")
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomeia colunas para padrão snake_case.
    """
    return df.rename(
        columns={
            "IdEmpresa": "id_empresa",
            "NomeFinal": "nome_empresa",
            "DataFechamento": "data_fechamento",
            "IdConta": "id_conta",
            "CodigoConta": "codigo_conta",
            "ContaDescricao": "descricao_conta",
            "ValorPadronizado": "valor",
        }
    )


def run_standardize(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper que aplica padronização de tipos e renomeação de colunas."""
    logger.info("Iniciando a padronização do arquivo bruto...")
    df = standardize_types(df)
    df = rename_columns(df)
    logger.info("[OK] Normalização concluída!")
    return df
