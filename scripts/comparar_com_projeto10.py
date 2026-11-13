"""Roda bronze e silver em Spark sobre os arquivos já baixados e compara com o projeto 10.

Uso: python scripts/comparar_com_projeto10.py <pasta do projeto 10> <mês> [<mês> ...]
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.lake import bronze, sessao, silver  # noqa: E402

projeto10 = Path(sys.argv[1])
lake = Path(tempfile.mkdtemp()) / "lake"
spark = sessao.criar()
for mes in sys.argv[2:]:
    inicio = time.time()
    bronze.carregar_viagens(spark, projeto10 / "dados" / "raw" / f"yellow_tripdata_{mes}.parquet", mes, "-", lake)
    meu = silver.processar_mes(spark, lake, mes)
    dele = json.loads((projeto10 / "dados" / "controle" / f"metricas_{mes}.json").read_text())
    campos = ["linhas_bruto", "viagens", "linhas_quarentena", "motivos", "duracao_invalida", "soma_diverge"]
    diferencas = {c: (meu[c], dele[c]) for c in campos if meu[c] != dele[c]}
    print(mes, f"{time.time() - inicio:.0f}s", "iguais" if not diferencas else diferencas, meu["receita_total"], dele["receita_total"])
