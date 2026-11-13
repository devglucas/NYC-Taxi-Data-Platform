"""Silver: regras de qualidade das viagens, em Spark.

São as mesmas regras do projeto 10, que rodava em DuckDB; a contagem de cada mês tem de bater
com a dele. Nenhuma linha some: o que não entra na silver vai para a quarentena com o motivo.

- Total negativo é estorno: somar contaria a corrida e o seu cancelamento.
- O fornecedor 1 envia parte das corridas duas vezes, como app e como taxímetro. Fica o
  registro de app, com desempate por todas as colunas, para o resultado não variar.
- O fornecedor 7 grava duração zero em todas as viagens: elas ficam, marcadas, e saem só das
  médias de duração.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import LIMITE_DISTANCIA_MILHAS, LIMITE_DURACAO_HORAS, LIMITE_VALOR_USD, validar_mes
from src.lake import esquema
from src.lake.bronze import gravar_particao

CANAL = """case
    when request_source is null then 'Rua ou ponto'
    when request_source like 'HV%' then 'App de alto volume'
    else 'Outros apps'
end"""


def classificar(sessao: SparkSession, bronze: DataFrame, mes: str) -> DataFrame:
    ano, numero = map(int, validar_mes(mes).split("-"))
    inicio = f"timestamp_ntz'{ano:04d}-{numero:02d}-01 00:00:00'"
    desempate = ", ".join(
        f"{c} asc nulls last" for c in esquema.COLUNAS if c not in esquema.CHAVE_CORRIDA and c != "request_source"
    )
    bronze.createOrReplaceTempView("bronze_mes")
    return sessao.sql(
        f"""
        with numerado as (
            select *, row_number() over (
                partition by {", ".join(esquema.CHAVE_CORRIDA)}
                order by request_source asc nulls last, {desempate}
            ) as ocorrencia
            from bronze_mes
        )
        select * except (ocorrencia),
            case
                when pickup is null or dropoff is null then 'horario_ausente'
                when pickup < {inicio} or pickup >= {inicio} + interval 1 month then 'fora_do_mes'
                when dropoff < pickup then 'desembarque_antes_do_embarque'
                when dropoff > pickup + interval {LIMITE_DURACAO_HORAS} hours then 'duracao_acima_do_limite'
                when total_amount is null then 'valor_ausente'
                when total_amount < 0 then 'estorno'
                when total_amount > {LIMITE_VALOR_USD} then 'valor_acima_do_limite'
                when trip_distance is null or trip_distance < 0
                     or trip_distance > {LIMITE_DISTANCIA_MILHAS} then 'distancia_invalida'
                when pulocationid is null or dolocationid is null
                     or pulocationid not between 1 and 265
                     or dolocationid not between 1 and 265 then 'zona_invalida'
                when ocorrencia > 1 then 'corrida_em_duplicidade'
            end as motivo_rejeicao
        from numerado
        """
    )


def enriquecer(validas: DataFrame) -> DataFrame:
    duracao = (F.unix_timestamp(F.col("dropoff").cast("timestamp")) - F.unix_timestamp(F.col("pickup").cast("timestamp"))) / 60
    soma = (
        F.col("fare_amount") + F.col("extra") + F.col("mta_tax") + F.col("tip_amount") + F.col("tolls_amount")
        + F.col("improvement_surcharge") + F.coalesce("congestion_surcharge", F.lit(0.0))
        + F.coalesce("airport_fee", F.lit(0.0)) + F.coalesce("cbd_congestion_fee", F.lit(0.0))
    )
    return (
        validas.drop("motivo_rejeicao", "_arquivo", "_sha256", "_ingerido_em")
        .withColumn("data", F.to_date("pickup"))
        .withColumn("hora", F.date_trunc("hour", F.col("pickup")).cast("timestamp_ntz"))
        .withColumn("canal", F.expr(CANAL))
        .withColumn("duracao_min", duracao)
        .withColumn("duracao_valida", F.col("dropoff") > F.col("pickup"))
        .withColumn(
            "velocidade_mph",
            F.when(F.col("duracao_min") >= 1, F.col("trip_distance") / (F.col("duracao_min") / 60)),
        )
        .withColumn("soma_confere", F.abs(F.col("total_amount") - soma) <= 0.05)
        .withColumn("paga_taxa_cbd", F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)) > 0)
    )


def processar_mes(sessao: SparkSession, lake: Path, mes: str) -> dict:
    bronze = sessao.read.format("delta").load(str(lake / "bronze" / "viagens")).where(F.col("mes") == mes)
    classificado = classificar(sessao, bronze, mes).persist()
    try:
        gravar_particao(enriquecer(classificado.where("motivo_rejeicao is null")), lake / "silver" / "viagens", mes)
        gravar_particao(
            classificado.where("motivo_rejeicao is not null").drop("_ingerido_em"),
            lake / "silver" / "quarentena", mes,
        )
        motivos = {
            linha["motivo_rejeicao"]: linha["count"]
            for linha in classificado.where("motivo_rejeicao is not null").groupBy("motivo_rejeicao").count().collect()
        }
        bruto = classificado.count()
    finally:
        classificado.unpersist()
    resumo = (
        sessao.read.format("delta").load(str(lake / "silver" / "viagens")).where(F.col("mes") == mes)
        .agg(
            F.count("*").alias("viagens"),
            F.sum(F.when(~F.col("duracao_valida"), 1).otherwise(0)).alias("duracao_invalida"),
            F.sum(F.when(~F.col("soma_confere"), 1).otherwise(0)).alias("soma_diverge"),
            F.sum(F.when(F.date_format("pickup", "yyyy-MM") != mes, 1).otherwise(0)).alias("fora_de_mes_na_silver"),
            F.round(F.sum("total_amount"), 2).alias("receita_total"),
        )
        .first()
        .asDict()
    )
    return {
        "mes": mes, "linhas_bruto": bruto, "linhas_quarentena": sum(motivos.values()),
        "motivos": dict(sorted(motivos.items(), key=lambda item: -item[1])), **resumo,
    }


def processar_clima(sessao: SparkSession, lake: Path, mes: str) -> int:
    bronze = sessao.read.format("delta").load(str(lake / "bronze" / "clima")).where(F.col("mes") == mes)
    silver = bronze.drop("_ingerido_em").withColumn("data", F.to_date("hora_local"))
    gravar_particao(silver, lake / "silver" / "clima", mes)
    return silver.count()
