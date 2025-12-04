import pandas as pd
from utils.logger import logger


def calculate_indicators(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o arquivo em formato wide e cria um novo DataFrame contendo
    apenas identificadores (empresa, data) e os indicadores calculados
    """
    logger.info("Iniciando cálculo de indicadores financeiros...")
    indicators = pd.DataFrame()
    indicators["nome_empresa"] = pivot_df["nome_empresa"]
    indicators["data_fechamento"] = pivot_df["data_fechamento"]

    # ============================
    # LIQUIDEZ
    # ============================
    logger.info("Calculando indicadores de LIQUIDEZ...")
    indicators["liquidez_corrente (pontos)"] = (
        pivot_df["Ativo circulante"] / pivot_df["Passivo circulante"]
    )

    indicators["liquidez_geral (pontos)"] = (
        pivot_df["Ativo circulante"] + pivot_df["Ativo não circulante"]
    ) / (pivot_df["Passivo circulante"] + pivot_df["Passivo não circulante"])

    indicators["liquidez_imediata (pontos)"] = (
        pivot_df["Caixa e equivalentes de caixa"] / pivot_df["Passivo circulante"]
    )
    logger.info("[OK] Indicadores de LIQUIDEZ calculados.")

    # ============================
    # ESTRUTURA DE CAPITAL
    # ============================
    logger.info("Calculando indicadores de ESTRUTURA DE CAPITAL...")
    indicators["endividamento (pontos)"] = (
        pivot_df["Passivo circulante"] + pivot_df["Passivo não circulante"]
    ) / pivot_df["Ativo total"]

    indicators["alavancagem (pontos)"] = (
        pivot_df["Ativo total"] / pivot_df["Patrimônio líquido"]
    )

    indicators["composicao_endividamento (%)"] = pivot_df["Passivo circulante"] / (
        pivot_df["Passivo circulante"] + pivot_df["Passivo não circulante"]
    )
    logger.info("[OK] Indicadores de ESTRUTURA DE CAPITAL calculados.")

    # ============================
    # RENTABILIDADE
    # ============================
    logger.info("Calculando indicadores de RENTABILIDADE...")
    indicators["roe (%)"] = (
        pivot_df["Lucro/Prejuízo do período"] / pivot_df["Patrimônio líquido"]
    )

    indicators["roa (%)"] = (
        pivot_df["Lucro/Prejuízo do período"] / pivot_df["Ativo total"]
    )

    indicators["ebit (R$)"] = pivot_df[
        "Resultado antes do resultado financeiro e dos tributos"
    ]

    indicators["margem_bruta (%)"] = (
        pivot_df["Receita líquida de vendas e/ou serviços"]
        - pivot_df["Custo de bens e/ou serviços vendidos"]
    ) / pivot_df["Receita líquida de vendas e/ou serviços"]

    indicators["margem_operacional (%)"] = (
        pivot_df["Resultado antes do resultado financeiro e dos tributos"]
        / pivot_df["Receita líquida de vendas e/ou serviços"]
    )

    indicators["margem_liquida (%)"] = (
        pivot_df["Lucro/Prejuízo do período"]
        / pivot_df["Receita líquida de vendas e/ou serviços"]
    )
    logger.info("[OK] Indicadores de RENTABILIDADE calculados.")

    # ============================
    # FLUXOS DE CAIXA
    # ============================
    logger.info("Calculando indicadores de FLUXOS DE CAIXA...")
    indicators["caixa_operacional_sobre_receita (%)"] = (
        pivot_df["Caixa líquido das atividades operacionais"]
        / pivot_df["Receita líquida de vendas e/ou serviços"]
    )

    indicators["conversao_caixa (%)"] = pivot_df[
        "Caixa líquido das atividades operacionais"
    ] / pivot_df["Lucro/Prejuízo do período"].replace(0, pd.NA)

    indicators["fluxo_caixa_livre (R$)"] = (
        pivot_df["Caixa líquido das atividades operacionais"]
        - pivot_df["Caixa líquido das atividades de investimento"]
    )

    indicators["participacao_caixa (%)"] = (
        pivot_df["Caixa e equivalentes de caixa"] / pivot_df["Ativo total"]
    )
    logger.info("[OK] Indicadores de FLUXOS DE CAIXA calculados.")

    indicators = indicators.round(2)

    return indicators
