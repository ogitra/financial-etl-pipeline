import os
import pandas as pd

PROCESSED_PATH = "data/processed/balancos_processed.csv"
DIM_EMP_PATH = "data/processed/dim_empresa.csv"
DIM_CONTA_PATH = "data/processed/dim_conta.csv"
FATO_PATH = "data/processed/fato_balanco.csv"


def load_processed(csv_path: str) -> pd.DataFrame:
    """
    -Lê o CSV já extraído e tratado pelo SQL.
         -Verifica se o arquivo existe.
         -Carrega os dados em um DataFrame.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    print(f"[INFO] Lendo arquivo processado: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"[INFO] Registros carregados: {len(df)}")
    return df


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


def create_dim_empresa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a dimensão de empresas sem duplicações, seleciona apenas id e nome.
    """
    return df[["id_empresa", "nome_empresa"]].drop_duplicates()


def create_dim_conta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a dimensão de contas contábeis sem duplicações ,seleciona id, código e descrição.
    """
    return df[["id_conta", "codigo_conta", "descricao_conta"]].drop_duplicates()


def create_fato_balanco(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria a tabela fato de balanço. (valores por empresa/conta/data).
    """
    return df[["id_empresa", "id_conta", "data_fechamento", "valor"]]


def save_output(df: pd.DataFrame, path: str):
    """
    Salva DataFrame em CSV no caminho especificado.
    - Cria diretório se não existir.
    - Remove índice para manter arquivo limpo.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[OK] Arquivo salvo em: {path}")


def run_transform():
    """
    Executa a etapa Transform: aplica padronização, cria dimensões e fato.
    """
    # Pipeline de transformação

    df = load_processed(PROCESSED_PATH)
    df = standardize_types(df)
    df = rename_columns(df)

    # Criação das tabelas dimensão e fato

    dim_empresa = create_dim_empresa(df)
    dim_conta = create_dim_conta(df)
    fato = create_fato_balanco(df)

    save_output(dim_empresa, DIM_EMP_PATH)
    save_output(dim_conta, DIM_CONTA_PATH)
    save_output(fato, FATO_PATH)

    # Amostra aleatória para versionamento no GitHub

    save_output(df.sample(n=100, random_state=42), "data/sample/balancos_sample.csv")

    print("\n[OK] Transform finalizado com sucesso.")
