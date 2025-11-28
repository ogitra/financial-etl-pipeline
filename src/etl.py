"""
Orquestrador da pipeline ETL.
- Coordena a execução das etapas: Extract → Transform → Load.
- Cada etapa é implementada em módulos separados já documentados.
- Este script é o ponto de entrada para rodar o processo completo.
"""

from extract import run_extract
from transform import run_transform
from load import run_load

if __name__ == "__main__":
    print("\n🚀 Iniciando pipeline ETL...\n")

    run_extract()
    run_transform()
    run_load()

    print("\n🏁 Pipeline finalizado com sucesso!\n")
