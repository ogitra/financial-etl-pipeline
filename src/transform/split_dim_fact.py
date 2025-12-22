import pandas as pd
from utils.logger import logger


def create_dim_company(df):
    """
    Cria a dimensão de empresas sem duplicações, seleciona apenas id e nome.
    """
    return df[["id_empresa", "NomeFantasia"]].drop_duplicates()


def create_dim_account(df):
    """
    Cria a dimensão de contas contábeis sem duplicações ,seleciona id, código e descrição.
    """
    return df[["id_conta", "codigo_conta", "descricao_conta"]].drop_duplicates()


def create_fact_balance(df):
    """
    Cria a tabela fato de balanço. (valores por empresa/conta/data).
    """
    return df[["id_empresa", "id_conta", "data_fechamento", "valor"]]


def run_split(df):
    """
    Wrapper que cria as tabelas de dimensão e fato a partir do DataFrame padronizado.
    Retorna um dicionário com os três DataFrames.
    """
    logger.info("Iniciando a separação para as tabelas fato e dimensão...")
    dim_company = create_dim_company(df)
    dim_account = create_dim_account(df)
    fact_balance = create_fact_balance(df)

    logger.info("[OK] Separação concluída! ")
    return {
        "dim_company": dim_company,
        "dim_account": dim_account,
        "fact_balance": fact_balance,
    }
