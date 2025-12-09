from extract import run_extract
from transform.standardize import run_standardize
from transform.split_dim_fato import run_split
from transform.create_pivot import create_pivot_table
from analytics.financial_indicators import calculate_indicators
from analytics.financial_evolution import calculate_evolution
from load.load import run_load
from utils.save_dataframe import save_dataframe
from utils.logger import logger

EXTRACT_PATH = "../data/processed/extract"
STD_PATH = "../data/processed/standardized"
DIM_FACT_PATH = "../data/processed/dimensions_fact"
ANALYTICS_PATH = "../data/processed/analytics"


def run_pipeline():
    logger.info("============================================")
    logger.info("🚀 Pipeline Financeiro - Início da execução")
    logger.info("============================================")

    # 1. Extract
    df_raw = run_extract()
    save_dataframe(df_raw, EXTRACT_PATH, "extract")
    logger.info("Extract finalizado com sucesso ✅")

    # 2. Transform
    df_std = run_standardize(df_raw)
    save_dataframe(df_std, STD_PATH, "standardized")

    df_split = run_split(df_std)
    # Itera sobre cada tabela retornada e salva o sample
    for name, df_table in df_split.items():
        save_dataframe(df_table, DIM_FACT_PATH, name)

    logger.info("Transform finalizado com sucesso ✅")

    # 3. Analytics

    wide_df = create_pivot_table(df_std)
    save_dataframe(wide_df, ANALYTICS_PATH, "wide_table")

    indicators_df = calculate_indicators(wide_df)
    save_dataframe(indicators_df, ANALYTICS_PATH, "financial_indicators")

    evolution_df = calculate_evolution(wide_df)
    save_dataframe(evolution_df, ANALYTICS_PATH, "financial_evolution")

    logger.info("Analytics finalizado com sucesso ✅")

    # 4. Load (carrega os samples no banco)
    run_load()

    logger.info("Load finalizado com sucesso ✅")

    logger.info("============================================")
    logger.info("✅ Pipeline Financeiro finalizado com sucesso")
    logger.info("============================================")


if __name__ == "__main__":
    run_pipeline()
