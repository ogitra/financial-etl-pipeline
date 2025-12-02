import pandas as pd


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
