from extract import run_extract
from transform.standardize import run_standardize
from transform.split_dim_fato import run_split
from transform.create_pivot import create_pivot_table
from analytics.financial_indicators import calculate_indicators
from analytics.financial_evolution import calculate_evolution
from load.load import run_load
from utils.save_sample import save_sample

SAMPLES_DIR = "../data/sample"


def run_pipeline():
    print("============================================")
    print("🚀 Pipeline Financeiro - Início da execução")
    print("============================================")

    # 1. Extract
    df_raw = run_extract()
    save_sample(df_raw, SAMPLES_DIR, "extract")

    # 2. Transform
    df_std = run_standardize(df_raw)
    save_sample(df_std, SAMPLES_DIR, "standardize")

    df_split = run_split(df_std)
    # Itera sobre cada tabela retornada e salva o sample
    for name, df_table in df_split.items():
        save_sample(df_table, SAMPLES_DIR, name)

    wide_df = create_pivot_table(df_std)
    save_sample(wide_df, SAMPLES_DIR, "wide")

    # 3. Analytics
    indicators_df = calculate_indicators(wide_df)
    save_sample(indicators_df, SAMPLES_DIR, "indicators")

    evolution_df = calculate_evolution(wide_df)
    save_sample(evolution_df, SAMPLES_DIR, "evolution")

    # 4. Load (carrega os samples no banco)
    run_load()

    print("============================================")
    print("✅ Pipeline Financeiro finalizado com sucesso")
    print("============================================")


if __name__ == "__main__":
    run_pipeline()
