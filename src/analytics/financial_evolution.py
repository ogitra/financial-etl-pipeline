import pandas as pd
from utils.logger import logger


def calculate_evolution(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o arquivo em formato wide e cria um novo DataFrame contendo
    apenas identificadores (empresa, data) e os indicadores de evolução calculados.
    """
    logger.info("Iniciando cálculo de evolução financeira...")
    evolution = pd.DataFrame()
    evolution["NomeFantasia"] = pivot_df["NomeFantasia"]
    evolution["data_fechamento"] = pivot_df["data_fechamento"]

    # Ordenar para cálculos temporais
    pivot_df = pivot_df.sort_values(["NomeFantasia", "data_fechamento"])

    # ============================
    # CRESCIMENTO YoY
    # ============================
    logger.info("Calculando crescimento YoY...")
    evolution["crescimento_receita_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Receita líquida de vendas e/ou serviços"
    ].pct_change()

    evolution["crescimento_lucro_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Lucro/Prejuízo do período"
    ].pct_change()

    evolution["crescimento_ebit_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Resultado antes do resultado financeiro e dos tributos"
    ].pct_change()

    evolution["crescimento_ativo_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Ativo total"
    ].pct_change()

    evolution["crescimento_pl_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Patrimônio líquido"
    ].pct_change()

    evolution["crescimento_caixa_operacional_yoy (%)"] = pivot_df.groupby(
        "NomeFantasia"
    )["Caixa líquido das atividades operacionais"].pct_change()

    # ============================
    # VARIAÇÃO ENDIVIDAMENTO
    # ============================
    logger.info("Calculando variação YoY do endividamento...")
    evolution["variacao_endividamento_yoy (p.p.)"] = pivot_df.groupby("NomeFantasia")[
        "Passivo circulante"
    ].pct_change()

    evolution = evolution.round(2)

    return evolution
