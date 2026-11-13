"""Refaz o painel HTML sempre que os marts mudam: agendamento por dado, não por hora."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

with DAG(
    dag_id="plataforma_painel",
    description="Gera o painel HTML quando plataforma_marts é atualizado",
    schedule=[Asset("plataforma_marts")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "painel"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task(pool="warehouse")
    def gerar_painel() -> str:
        from src.config import Caminhos
        from src.painel import gerar_painel as gerar

        return gerar(Caminhos.do_ambiente())

    gerar_painel()
