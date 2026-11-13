"""Extrações para o Tableau: só agregados, com nomes de zona e dia da semana já resolvidos."""
from __future__ import annotations

import duckdb

from src.config import Caminhos

EXTRACOES = {
    "zonas": "select * from dim_zona order by zona_id",
    "demanda_clima_hora": """
        select f.*, d.dia_semana, d.fim_de_semana
        from fct_demanda_clima f join dim_data d on d.data = f.hora::date
        order by f.hora""",
    "zonas_mes": """
        select f.*, z.zona, z.distrito, z.aeroporto
        from fct_zonas_mes f join dim_zona z on z.zona_id = f.zona_origem
        order by f.mes, f.zona_origem""",
    "rotas_mes": """
        select r.*, o.zona as zona_origem_nome, o.distrito as distrito_origem,
            d.zona as zona_destino_nome, d.distrito as distrito_destino
        from fct_rotas_mes r
        join dim_zona o on o.zona_id = r.zona_origem
        join dim_zona d on d.zona_id = r.zona_destino
        order by r.mes, r.viagens desc""",
    "resumo_mensal": "select * from fct_mensal order by mes, canal, pagamento",
    "conciliacao_oficial": "select * from mart_conciliacao_oficial order by mes",
    "qualidade_mes": "select * from mart_qualidade_mes order by mes",
}


def exportar_extracoes(caminhos: Caminhos) -> dict[str, int]:
    destino = caminhos.output / "tableau"
    destino.mkdir(parents=True, exist_ok=True)
    linhas = {}
    with duckdb.connect(str(caminhos.warehouse), read_only=True) as con:
        for nome, sql in EXTRACOES.items():
            arquivo = destino / f"{nome}.csv"
            con.execute(f"copy ({sql}) to '{arquivo}' (header, delimiter ',')")
            linhas[nome] = con.execute(f"select count(*) from ({sql})").fetchall()[0][0]
    return linhas
