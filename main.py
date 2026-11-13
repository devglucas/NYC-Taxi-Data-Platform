"""Roda a plataforma sem Airflow, para desenvolvimento e para o CI.

    python main.py 2026-08 2026-09        # meses informados
    python main.py --sem-download 2026-09  # usa o arquivo já em dados/raw
"""
from __future__ import annotations

import argparse
import time

from src import pipeline, qualidade
from src.config import Caminhos, validar_mes
from src.exportar import exportar_extracoes
from src.painel import gerar_painel


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("meses", nargs="+", type=validar_mes)
    parser.add_argument("--sem-download", action="store_true")
    args = parser.parse_args()
    caminhos = Caminhos.do_ambiente()
    if not args.sem_download:
        pipeline.baixar_referencias(caminhos)
    for mes in args.meses:
        inicio = time.time()
        novas = [] if args.sem_download else pipeline.baixar_mes_completo(caminhos, mes)["colunas_novas"]
        if not args.sem_download:
            pipeline.baixar_clima(caminhos, mes)
        metricas = pipeline.processar_lake(caminhos, mes)
        criticas = qualidade.criticas_reprovadas(pipeline.verificar(caminhos, metricas, novas))
        if criticas:
            raise SystemExit(f"{mes} reprovado: {criticas}")
        print(f"{mes}: {metricas['viagens']} viagens na silver em {time.time() - inicio:.0f}s")
    print(pipeline.dbt_build(caminhos))
    print(exportar_extracoes(caminhos))
    print(gerar_painel(caminhos))


if __name__ == "__main__":
    main()
