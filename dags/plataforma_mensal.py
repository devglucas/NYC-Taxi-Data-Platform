"""Plataforma mensal: viagens e clima → lake Delta com Spark → warehouse com dbt → extrações.

A TLC publica cada mês com cerca de dois meses de atraso e sem data certa; a execução do mês
espera o arquivo com um sensor em modo reschedule. A lógica está em `src/`; aqui ficam ordem,
repetição, pools e o que fazer em cada falha.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, CronDataIntervalTimetable, Param, PokeReturnValue, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from src import alertas, contrato, fonte, pipeline, qualidade
from src.config import Caminhos, url_viagens, validar_mes

MARTS = Asset("plataforma_marts")


def resolver_mes_do_contexto(contexto: dict) -> str:
    """Mês explícito em `params` ou o início do intervalo de dados da execução."""
    mes = contexto["params"].get("mes")
    if mes:
        return validar_mes(mes)
    inicio = contexto.get("data_interval_start")
    if inicio is None:
        raise AirflowFailException("execução manual sem data: informe o parâmetro mes")
    return inicio.strftime("%Y-%m")


def _registrar(contexto: dict, nivel: str, prefixo: str = "") -> None:
    instancia = contexto["ti"]
    alertas.registrar(
        Caminhos.do_ambiente().alertas, nivel, f"{instancia.dag_id}.{instancia.task_id}",
        f"{prefixo}{str(contexto.get('exception'))[:500]}", id_execucao=instancia.run_id, tentativa=instancia.try_number,
    )


def avisar_falha(contexto: dict) -> None:
    _registrar(contexto, "erro")


def avisar_nova_tentativa(contexto: dict) -> None:
    _registrar(contexto, "aviso", "nova tentativa: ")


with DAG(
    dag_id="plataforma_mensal",
    description="Ingestão, lake, warehouse e extrações de um mês de viagens de táxi",
    # Com intervalo de dados, a execução de 1º de dezembro cuida de novembro.
    schedule=CronDataIntervalTimetable("0 6 1 * *", timezone="UTC"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=["tlc", "plataforma"],
    params={
        "mes": Param(None, type=["null", "string"], description="AAAA-MM; vazio usa o intervalo da execução"),
        "rebaixar": Param(False, type="boolean", description="Baixa de novo mesmo com o ETag igual"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=40),
        "on_failure_callback": avisar_falha,
        "on_retry_callback": avisar_nova_tentativa,
    },
) as dag:

    @task
    def resolver_mes() -> str:
        return resolver_mes_do_contexto(get_current_context())

    @task.sensor(poke_interval=timedelta(hours=6).total_seconds(), timeout=timedelta(days=75).total_seconds(), mode="reschedule")
    def aguardar_viagens(mes: str) -> PokeReturnValue:
        consulta = fonte.consultar(url_viagens(mes))
        return PokeReturnValue(is_done=consulta.disponivel, xcom_value=consulta.como_dict())

    @task(pool="downloads", execution_timeout=timedelta(minutes=20))
    def baixar_viagens(mes: str, consulta: dict) -> dict:
        caminhos = Caminhos.do_ambiente()
        # Nome único por execução: duas execuções do mesmo mês não disputam o mesmo arquivo.
        staging = caminhos.staging / f"yellow_tripdata_{mes}.{uuid.uuid4().hex[:8]}.parquet"
        return pipeline.baixar_viagens(caminhos, mes, consulta, staging, get_current_context()["params"]["rebaixar"])

    @task
    def verificar_contrato(baixado: dict) -> dict:
        try:
            return pipeline.promover_viagens(Caminhos.do_ambiente(), baixado, get_current_context()["run_id"])
        except contrato.ContratoViolado as erro:
            raise AirflowFailException(str(erro)) from erro

    @task(pool="downloads")
    def baixar_clima(mes: str) -> int:
        return pipeline.baixar_clima(Caminhos.do_ambiente(), mes)

    @task(pool="downloads")
    def atualizar_referencias() -> None:
        pipeline.baixar_referencias(Caminhos.do_ambiente())

    @task(pool="spark", execution_timeout=timedelta(minutes=30))
    def processar_lake(mes: str) -> dict:
        return pipeline.processar_lake(Caminhos.do_ambiente(), mes)

    # Reprovação usa AirflowFailException e não se repete; as tentativas ficam para falhas
    # de infraestrutura, como um token expirado depois que a máquina dormiu.
    @task
    def portao_de_qualidade(metricas: dict, verificado: dict) -> None:
        caminhos = Caminhos.do_ambiente()
        verificacoes = pipeline.verificar(caminhos, metricas, verificado["colunas_novas"])
        for falha in qualidade.alertas_reprovados(verificacoes):
            alertas.registrar(caminhos.alertas, "alerta", f"plataforma_mensal.{falha['verificacao']}", falha["detalhe"], mes=metricas["mes"])
        criticas = qualidade.criticas_reprovadas(verificacoes)
        if criticas:
            raise AirflowFailException(f"{metricas['mes']}: {criticas}")

    @task(pool="warehouse", execution_timeout=timedelta(minutes=30))
    def dbt_build(mes: str) -> str:
        return pipeline.dbt_build(Caminhos.do_ambiente(), mes)

    @task(pool="warehouse", outlets=[MARTS])
    def exportar(_resumo_dbt: str) -> dict:
        from src.exportar import exportar_extracoes

        return exportar_extracoes(Caminhos.do_ambiente())

    mes = resolver_mes()
    consulta = aguardar_viagens(mes)
    verificado = verificar_contrato(baixar_viagens(mes, consulta))
    metricas = processar_lake(mes)
    [verificado, baixar_clima(mes)] >> metricas
    portao = portao_de_qualidade(metricas, verificado)
    resumo = dbt_build(mes)
    [portao, atualizar_referencias()] >> resumo
    exportar(resumo)
