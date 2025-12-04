import pandas as pd
from utils.logger import logger


def create_dim_empresa(df):
    """
    Cria a dimensão de empresas sem duplicações, seleciona apenas id e nome.
    """
    return df[["id_empresa", "nome_empresa"]].drop_duplicates()


def create_dim_conta(df):
    """
    Cria a dimensão de contas contábeis sem duplicações ,seleciona id, código e descrição.
    """
    return df[["id_conta", "codigo_conta", "descricao_conta"]].drop_duplicates()


def create_fato(df):
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
    dim_empresa = create_dim_empresa(df)
    dim_conta = create_dim_conta(df)
    fato = create_fato(df)

    logger.info("[OK] Separação concluída! ")
    return {"dim_company": dim_empresa, "dim_account": dim_conta, "fact_balance": fato}
