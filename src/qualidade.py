"""Portão antes do warehouse: críticas barram o mês; alertas publicam e avisam."""
from __future__ import annotations

from src.config import TAXA_QUARENTENA_CRITICA


def _verificacao(nome: str, nivel: str, passou: bool, detalhe: str) -> dict:
    return {"verificacao": nome, "nivel": nivel, "passou": bool(passou), "detalhe": detalhe}


def avaliar(metricas: dict, horas_de_clima: int, colunas_novas: list[str] | None = None) -> list[dict]:
    bruto, viagens, quarentena = metricas["linhas_bruto"], metricas["viagens"], metricas["linhas_quarentena"]
    taxa = quarentena / bruto if bruto else 1.0
    return [
        _verificacao("conservacao_de_linhas", "critica", bruto == viagens + quarentena,
                     f"bruto {bruto} = silver {viagens} + quarentena {quarentena}"),
        _verificacao("silver_nao_vazia", "critica", viagens > 0, f"{viagens} viagens"),
        _verificacao("silver_dentro_do_mes", "critica", metricas["fora_de_mes_na_silver"] == 0,
                     f"{metricas['fora_de_mes_na_silver']} linhas fora do mês"),
        _verificacao("taxa_de_quarentena", "critica", taxa <= TAXA_QUARENTENA_CRITICA,
                     f"{taxa:.2%} (limite {TAXA_QUARENTENA_CRITICA:.0%})"),
        # Um mês tem de 672 a 744 horas; a virada do horário de verão tira ou junta uma.
        _verificacao("clima_cobre_o_mes", "critica", horas_de_clima >= 671, f"{horas_de_clima} horas de clima"),
        _verificacao("colunas_novas_na_fonte", "alerta", not colunas_novas, f"novas: {colunas_novas or []}"),
    ]


def criticas_reprovadas(verificacoes: list[dict]) -> list[dict]:
    return [v for v in verificacoes if v["nivel"] == "critica" and not v["passou"]]


def alertas_reprovados(verificacoes: list[dict]) -> list[dict]:
    return [v for v in verificacoes if v["nivel"] == "alerta" and not v["passou"]]
