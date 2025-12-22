from extract import run_extract
from transform.standardize import run_standardize
from transform.split_dim_fact import run_split
from transform.wide_table import run_wide_table
from transform.analytics.financial_indicators import calculate_indicators
from transform.analytics.financial_evolution import calculate_evolution
from load.sqlite_loader import run_load_sqlite
from load.s3_loader import run_load_s3
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
    logger.info("Extract finalizado com sucesso ✅")

    # 2. Transform
    df_std = run_standardize(df_raw)

    dfs_dim_fact = run_split(df_std)
    df_wide = run_wide_table(df_std)

    logger.info("Transform finalizado com sucesso ✅")

    # 3. Analytics
    indicators_df = calculate_indicators(df_wide)
    evolution_df = calculate_evolution(df_wide)

    logger.info("Analytics finalizado com sucesso ✅")

    # 4. Load
    datasets = {
        # dimensions / fact / wide
        "dim_company": dfs_dim_fact["dim_company"],
        "dim_account": dfs_dim_fact["dim_account"],
        "fact_balance": dfs_dim_fact["fact_balance"],
        "wide_table": df_wide,
        # analytics
        "financial_indicators": indicators_df,
        "financial_evolution": evolution_df,
    }
    run_load_sqlite(datasets)
    run_load_s3(datasets)

    logger.info("Load finalizado com sucesso ✅")

    logger.info("============================================")
    logger.info("✅ Pipeline Financeiro finalizado com sucesso")
    logger.info("============================================")


if __name__ == "__main__":
    run_pipeline()
