import pandas as pd


def create_pivot_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte o DataFrame (long format) para formato wide, onde cada conta contábil
    vira uma coluna e cada linha representa empresa + data.
    """
    print("[INFO] Criando tabela pivotada (wide format)...")

    pivot_df = df.pivot_table(
        index=["nome_empresa", "data_fechamento"],
        columns="descricao_conta",
        values="valor",
    )

    pivot_df = pivot_df.reset_index()

    # Seleciona apenas as colunas de contas (exclui nome_empresa e data_fechamento)
    conta_cols = [
        col
        for col in pivot_df.columns
        if col not in ["nome_empresa", "data_fechamento"]
    ]

    # Converte para int (se houver NaN, preenche com 0 antes)
    pivot_df[conta_cols] = pivot_df[conta_cols].fillna(0).astype("int64")

    print("[OK] Pivot criado com sucesso!")
    return pivot_df
