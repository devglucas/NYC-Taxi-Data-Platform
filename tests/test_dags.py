"""Integridade das DAGs. Roda onde o Airflow está instalado: no contêiner e no CI."""
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk.exceptions import AirflowFailException  # noqa: E402

from dags.plataforma_mensal import resolver_mes_do_contexto  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(Path(__file__).parent.parent / "dags"))


def test_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {"plataforma_mensal", "plataforma_painel"}


def test_dependencias(dagbag):
    dag = dagbag.dags["plataforma_mensal"]
    antes = {
        "processar_lake": {"verificar_contrato", "baixar_clima"},
        "dbt_build": {"portao_de_qualidade", "atualizar_referencias"},
        "exportar": {"dbt_build"},
    }
    for tarefa, esperadas in antes.items():
        assert esperadas <= dag.get_task(tarefa).upstream_task_ids


def test_pools_e_politicas(dagbag):
    dag = dagbag.dags["plataforma_mensal"]
    assert type(dag.timetable).__name__ == "CronDataIntervalTimetable"
    assert dag.catchup is False
    assert dag.get_task("aguardar_viagens").mode == "reschedule"
    assert dag.get_task("processar_lake").pool == "spark"
    assert {dag.get_task(t).pool for t in ("dbt_build", "exportar")} == {"warehouse"}
    assert dag.get_task("baixar_viagens").retries == 3


def test_painel_e_disparado_pelo_asset(dagbag):
    assert type(dagbag.dags["plataforma_painel"].timetable).__name__ == "AssetTriggeredTimetable"


def test_resolver_mes():
    assert resolver_mes_do_contexto({"params": {"mes": "2025-03"}, "data_interval_start": None}) == "2025-03"
    assert resolver_mes_do_contexto({"params": {"mes": None}, "data_interval_start": datetime(2026, 1, 1, tzinfo=UTC)}) == "2026-01"
    with pytest.raises(AirflowFailException):
        resolver_mes_do_contexto({"params": {"mes": None}, "data_interval_start": None})
