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

    # ============================================================
    # OBS 1 — Crescimento YoY (%) usando pct_change padrão
    # Indicadores que não assumem valores negativos com frequência
    # ============================================================

    logger.info("Calculando crescimento YoY (pct_change padrão)...")

    evolution["crescimento_receita_yoy (%)"] = pivot_df.groupby("NomeFantasia")[
        "Receita líquida de vendas e/ou serviços"
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

    # ============================================================
    # OBS 2 — Crescimento YoY (%) com base absoluta (|ano anterior|)
    # Evita distorções em métricas que podem ser negativas
    # ============================================================

    logger.info("Calculando crescimento YoY (% ajustado – base absoluta)...")

    evolution["crescimento_lucro_yoy (%)"] = (
        pivot_df.groupby("NomeFantasia")["Lucro/Prejuízo do período"].diff()
        / pivot_df.groupby("NomeFantasia")["Lucro/Prejuízo do período"].shift(1).abs()
    )

    evolution["crescimento_ebit_yoy (%)"] = (
        pivot_df.groupby("NomeFantasia")[
            "Resultado antes do resultado financeiro e dos tributos"
        ].diff()
        / pivot_df.groupby("NomeFantasia")[
            "Resultado antes do resultado financeiro e dos tributos"
        ]
        .shift(1)
        .abs()
    )

    evolution["variacao_endividamento_yoy (%)"] = (
        pivot_df.groupby("NomeFantasia")["Passivo circulante"].diff()
        / pivot_df.groupby("NomeFantasia")["Passivo circulante"].shift(1).abs()
    )

    evolution = evolution.round(2)

    return evolution
