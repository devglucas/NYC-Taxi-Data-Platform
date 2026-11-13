from datetime import datetime, timedelta

import pytest

from src.lake import bronze, silver

pytestmark = pytest.mark.spark
BASE = datetime(2026, 9, 10, 8, 0)


def rodar(spark, escrever_parquet, tmp_path, linhas, mes="2026-09", **kw):
    arquivo = escrever_parquet(linhas, **kw)
    lake = tmp_path / "lake"
    bronze.carregar_viagens(spark, arquivo, mes, "sha", lake)
    return silver.processar_mes(spark, lake, mes), lake


def silver_linhas(spark, lake, colunas):
    return [tuple(r) for r in spark.read.format("delta").load(str(lake / "silver" / "viagens")).select(*colunas).collect()]


@pytest.mark.parametrize(
    ("linha", "motivo"),
    [
        ({"tpep_pickup_datetime": datetime(2009, 1, 1), "tpep_dropoff_datetime": datetime(2009, 1, 1, 0, 10)}, "fora_do_mes"),
        ({"tpep_dropoff_datetime": BASE - timedelta(minutes=1)}, "desembarque_antes_do_embarque"),
        ({"tpep_dropoff_datetime": BASE + timedelta(hours=25)}, "duracao_acima_do_limite"),
        ({"total_amount": -20.5}, "estorno"),
        ({"total_amount": 5000.0}, "valor_acima_do_limite"),
        ({"trip_distance": 232644.8}, "distancia_invalida"),
        ({"PULocationID": 999}, "zona_invalida"),
    ],
)
def test_motivos_de_quarentena(spark, escrever_parquet, tmp_path, linha, motivo):
    metricas, _ = rodar(spark, escrever_parquet, tmp_path, [{}, linha])
    assert metricas["motivos"] == {motivo: 1}
    assert metricas["viagens"] == 1


def test_nenhuma_linha_se_perde(spark, escrever_parquet, tmp_path):
    metricas, lake = rodar(spark, escrever_parquet, tmp_path, [{}, {"total_amount": -1.0}, {"PULocationID": 999}, {"trip_distance": 3.0}])
    assert metricas["linhas_bruto"] == 4 == metricas["viagens"] + metricas["linhas_quarentena"]
    assert spark.read.format("delta").load(str(lake / "silver" / "quarentena")).count() == 2


def test_corrida_dupla_mantem_o_registro_de_app(spark, escrever_parquet, tmp_path):
    linhas = [
        {"request_source": None, "payment_type": 1, "extra": 3.25},
        {"request_source": "EH0004", "payment_type": 0, "extra": 0.0},
    ]
    metricas, lake = rodar(spark, escrever_parquet, tmp_path, linhas, extras={"request_source": None})
    assert metricas["motivos"] == {"corrida_em_duplicidade": 1}
    assert silver_linhas(spark, lake, ["request_source", "canal"]) == [("EH0004", "Outros apps")]


def test_duracao_zero_fica_marcada(spark, escrever_parquet, tmp_path):
    _, lake = rodar(spark, escrever_parquet, tmp_path, [{"VendorID": 7, "tpep_dropoff_datetime": BASE}])
    assert silver_linhas(spark, lake, ["duracao_valida", "velocidade_mph"]) == [(False, None)]


def test_campos_derivados(spark, escrever_parquet, tmp_path):
    _, lake = rodar(spark, escrever_parquet, tmp_path, [{"cbd_congestion_fee": 0.75, "total_amount": 21.25}], extras={"cbd_congestion_fee": 0.0})
    linha = spark.read.format("delta").load(str(lake / "silver" / "viagens")).first()
    assert linha["hora"] == datetime(2026, 9, 10, 8, 0)
    assert linha["duracao_min"] == 20
    assert linha["velocidade_mph"] == pytest.approx(6.0)
    assert linha["soma_confere"] and linha["paga_taxa_cbd"]


def test_horario_nao_e_convertido_de_fuso(spark, escrever_parquet, tmp_path):
    _, lake = rodar(spark, escrever_parquet, tmp_path, [{"tpep_pickup_datetime": datetime(2026, 9, 1, 0, 5), "tpep_dropoff_datetime": datetime(2026, 9, 1, 0, 25)}])
    assert silver_linhas(spark, lake, ["pickup"]) == [(datetime(2026, 9, 1, 0, 5),)]


def test_reprocessar_um_mes_nao_mexe_nos_outros(spark, escrever_parquet, tmp_path):
    lake = tmp_path / "lake"
    agosto = escrever_parquet(
        [{"tpep_pickup_datetime": datetime(2026, 8, 5, 8), "tpep_dropoff_datetime": datetime(2026, 8, 5, 8, 30)}], nome="a.parquet"
    )
    setembro = escrever_parquet([{}, {"trip_distance": 3.0}], nome="s.parquet")
    for arquivo, mes in [(agosto, "2026-08"), (setembro, "2026-09"), (setembro, "2026-09")]:
        bronze.carregar_viagens(spark, arquivo, mes, "sha", lake)
        silver.processar_mes(spark, lake, mes)
    contagem = dict(spark.read.format("delta").load(str(lake / "silver" / "viagens")).groupBy("mes").count().collect())
    assert contagem == {"2026-08": 1, "2026-09": 2}


def test_mes_antigo_sem_colunas_novas(spark, escrever_parquet, tmp_path):
    metricas, lake = rodar(spark, escrever_parquet, tmp_path, [{}])
    assert metricas["viagens"] == 1
    assert silver_linhas(spark, lake, ["cbd_congestion_fee", "request_source", "paga_taxa_cbd"]) == [(None, None, False)]


def test_clima_vai_para_silver(spark, tmp_path):
    resposta = {"hourly": {"time": ["2026-09-01T04:00", "2026-09-01T05:00"], "temperature_2m": [20.0, 21.0],
                           "precipitation": [0.0, 1.2], "rain": [0.0, 1.2], "snowfall": [0.0, 0.0], "wind_speed_10m": [5.0, 6.0]}}
    lake = tmp_path / "lake"
    assert bronze.carregar_clima(spark, resposta, "2026-09", lake) == 2
    assert silver.processar_clima(spark, lake, "2026-09") == 2
