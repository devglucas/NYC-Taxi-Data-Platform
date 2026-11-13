"""Etapas da plataforma, chamadas pelas DAGs do Airflow e pelo `main.py`.

Cada etapa recebe o mês e os caminhos e grava seu estado em `dados/controle`, então pode ser
repetida sem efeito colateral.
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime
from pathlib import Path

from src import clima, contrato, controle, fonte, qualidade
from src.config import URL_RELATORIO_OFICIAL, URL_ZONAS, Caminhos, url_viagens

RAIZ = Path(__file__).resolve().parent.parent


def estado_viagens(caminhos: Caminhos, mes: str) -> dict:
    return controle.ler_json(caminhos.controle / f"viagens_{mes}.json") or {}


def baixar_viagens(caminhos: Caminhos, mes: str, consulta: dict, staging: Path, rebaixar: bool = False) -> dict:
    """Pula o download se o ETag não mudou; senão baixa para o staging indicado."""
    estado = estado_viagens(caminhos, mes)
    if not rebaixar and caminhos.arquivo_viagens(mes).exists() and consulta.get("etag") and estado.get("etag") == consulta["etag"]:
        return {"mes": mes, "novo": False, "sha256": estado["sha256"], "consulta": consulta}
    recebido = fonte.baixar_parquet(consulta["url"], staging)
    return {"mes": mes, "novo": True, "staging": str(staging), "consulta": consulta, **recebido}


def promover_viagens(caminhos: Caminhos, baixado: dict, rejeitado_como: str) -> dict:
    """Só o arquivo que cumpre o contrato vira raw; o rejeitado fica guardado para análise."""
    mes = baixado["mes"]
    if not baixado["novo"]:
        return baixado | {"colunas_novas": []}
    staging = Path(baixado["staging"])
    try:
        resultado = contrato.exigir(staging)
    except contrato.ContratoViolado:
        rejeitados = caminhos.dados / "rejeitados"
        rejeitados.mkdir(parents=True, exist_ok=True)
        staging.replace(rejeitados / f"{rejeitado_como}.parquet")
        raise
    destino = caminhos.arquivo_viagens(mes)
    destino.parent.mkdir(parents=True, exist_ok=True)
    staging.replace(destino)
    controle.gravar_json(
        caminhos.controle / f"viagens_{mes}.json",
        {
            "mes": mes, "url": baixado["consulta"]["url"], "etag": baixado["consulta"].get("etag"),
            "ultima_modificacao": baixado["consulta"].get("ultima_modificacao"), "sha256": baixado["sha256"],
            "tamanho": baixado["tamanho"], "linhas": baixado["linhas"],
            "baixado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        },
    )
    return baixado | {"colunas_novas": resultado.novas}


def baixar_clima(caminhos: Caminhos, mes: str) -> int:
    import json

    destino = clima.baixar(mes, caminhos.arquivo_clima(mes))
    return len(clima.horas_locais(json.loads(destino.read_text(encoding="utf-8")), mes))


def baixar_referencias(caminhos: Caminhos) -> None:
    """Zonas e relatório oficial mudam pouco, mas o relatório ganha um mês por vez."""
    fonte.baixar_texto(URL_ZONAS, caminhos.zonas)
    fonte.baixar_texto(URL_RELATORIO_OFICIAL, caminhos.relatorio_oficial)


def processar_lake(caminhos: Caminhos, mes: str) -> dict:
    """Bronze e silver de viagens e clima do mês, em uma sessão Spark."""
    import json

    from src.lake import bronze, sessao, silver

    spark = sessao.criar()
    try:
        sha = estado_viagens(caminhos, mes).get("sha256", "desconhecido")
        bronze.carregar_viagens(spark, caminhos.arquivo_viagens(mes), mes, sha, caminhos.lake)
        resposta = json.loads(caminhos.arquivo_clima(mes).read_text(encoding="utf-8"))
        bronze.carregar_clima(spark, resposta, mes, caminhos.lake)
        metricas = silver.processar_mes(spark, caminhos.lake, mes)
        metricas["horas_de_clima"] = silver.processar_clima(spark, caminhos.lake, mes)
    finally:
        spark.stop()
    controle.gravar_json(caminhos.controle / f"metricas_{mes}.json", metricas)
    return metricas


def verificar(caminhos: Caminhos, metricas: dict, colunas_novas: list[str]) -> list[dict]:
    verificacoes = qualidade.avaliar(metricas, metricas["horas_de_clima"], colunas_novas)
    controle.gravar_json(caminhos.controle / f"qualidade_{metricas['mes']}.json", {"mes": metricas["mes"], "verificacoes": verificacoes})
    return verificacoes


def comando_dbt(caminhos: Caminhos, *argumentos: str) -> list[str]:
    executavel = os.getenv("DBT_BIN", "dbt")
    return [
        executavel, *argumentos, "--project-dir", str(RAIZ / "dbt"), "--profiles-dir", str(RAIZ / "dbt"),
        "--target-path", str(caminhos.dados / "dbt" / "target"), "--log-path", str(caminhos.dados / "dbt" / "logs"),
    ]


def ambiente_dbt(caminhos: Caminhos) -> dict:
    return os.environ | {
        "WAREHOUSE_PATH": str(caminhos.warehouse), "LAKE_PATH": str(caminhos.lake),
        "REFERENCIA_PATH": str(caminhos.zonas.parent), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def dbt_build(caminhos: Caminhos, mes: str | None = None) -> str:
    argumentos = ["build"] + (["--vars", f"{{mes: '{mes}'}}"] if mes else ["--full-refresh"])
    execucao = subprocess.run(
        comando_dbt(caminhos, *argumentos), env=ambiente_dbt(caminhos), capture_output=True, text=True, check=False
    )
    if execucao.returncode != 0:
        raise RuntimeError(f"dbt build falhou:\n{execucao.stdout[-4000:]}")
    return execucao.stdout.strip().splitlines()[-1]


def baixar_mes_completo(caminhos: Caminhos, mes: str) -> dict:
    """Atalho sem Airflow: consulta, baixa e promove o arquivo do mês."""
    consulta = fonte.consultar(url_viagens(mes))
    if not consulta.disponivel:
        raise fonte.FonteInstavel(f"{mes} ainda não publicado")
    baixado = baixar_viagens(caminhos, mes, consulta.como_dict(), caminhos.staging / f"yellow_tripdata_{mes}.local.parquet")
    return promover_viagens(caminhos, baixado, f"local_{mes}")
