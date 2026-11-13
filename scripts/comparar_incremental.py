"""Confere que o build incremental de um mês gera as mesmas tabelas que o full refresh.

Uso: python scripts/comparar_incremental.py <mês>  (com o dbt já construído em full refresh)
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import pipeline  # noqa: E402
from src.config import Caminhos  # noqa: E402

TABELAS = ["int_viagens_hora_zona", "fct_viagens_hora", "fct_demanda_clima", "fct_zonas_mes", "fct_rotas_mes", "fct_mensal",
           "mart_conciliacao_oficial", "mart_qualidade_mes"]

caminhos = Caminhos.do_ambiente()
completo = caminhos.dados / "completo.duckdb"
shutil.copy(caminhos.warehouse, completo)
pipeline.dbt_build(caminhos, sys.argv[1])
diferencas = 0
with duckdb.connect(str(caminhos.warehouse), read_only=True) as con:
    con.execute(f"attach '{completo}' as completo (read_only)")
    for tabela in TABELAS:
        colunas = con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_catalog = 'warehouse' and table_name = ? order by ordinal_position", [tabela]
        ).fetchall()
        # Somas de float em paralelo variam na última casa; o valor arredondado a 2 casas pode
        # mudar 0,01. Por isso: colunas exatas como chave e tolerância de 0,011 nos floats.
        exatas = [n for n, t in colunas if t != "DOUBLE"]
        floats = [n for n, t in colunas if t == "DOUBLE"]
        juncao = " and ".join(f"a.{n} is not distinct from b.{n}" for n in exatas)
        diverge = " or ".join(f"abs(coalesce(a.{n}, 0) - coalesce(b.{n}, 0)) > 0.011 + 1e-9 * abs(coalesce(b.{n}, 0))" for n in floats) or "false"
        n = con.execute(
            f"""select count(*) from warehouse.main.{tabela} a full join completo.main.{tabela} b on {juncao}
                where a.{exatas[0]} is null or b.{exatas[0]} is null or {diverge}"""
        ).fetchall()[0][0]
        print(f"{tabela}: {n} linhas diferentes")
        diferencas += n
completo.unlink()
sys.exit(1 if diferencas else 0)
