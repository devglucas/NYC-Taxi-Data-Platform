"""Bronze: o arquivo do mês como veio, com nomes e tipos padronizados e a origem anotada."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import clima
from src.config import validar_mes
from src.lake import esquema


def ler_viagens(sessao: SparkSession, arquivo: Path, mes: str, sha256: str) -> DataFrame:
    bruto = sessao.read.parquet(str(arquivo))
    return bruto.selectExpr(*esquema.selecao(bruto.columns)).select(
        "*",
        F.lit(validar_mes(mes)).alias("mes"),
        F.lit(arquivo.name).alias("_arquivo"),
        F.lit(sha256).alias("_sha256"),
        F.current_timestamp().alias("_ingerido_em"),
    )


def gravar_particao(df: DataFrame, destino: Path, mes: str) -> None:
    """Substitui só o mês: reprocessar não duplica nem apaga os outros meses."""
    (
        df.write.format("delta").mode("overwrite").option("replaceWhere", f"mes = '{validar_mes(mes)}'")
        .partitionBy("mes").save(str(destino))
    )


def carregar_viagens(sessao: SparkSession, arquivo: Path, mes: str, sha256: str, lake: Path) -> int:
    gravar_particao(ler_viagens(sessao, arquivo, mes, sha256), lake / "bronze" / "viagens", mes)
    return contar(sessao, lake / "bronze" / "viagens", mes)


def carregar_clima(sessao: SparkSession, resposta: dict, mes: str, lake: Path) -> int:
    linhas = clima.horas_locais(resposta, mes)
    esquema_clima = (
        "mes string, hora_local timestamp_ntz, temperatura_c double, precipitacao_mm double, "
        "chuva_mm double, neve_cm double, vento_kmh double, medicoes int"
    )
    df = sessao.createDataFrame(linhas, esquema_clima).withColumn("_ingerido_em", F.current_timestamp())
    gravar_particao(df, lake / "bronze" / "clima", mes)
    return len(linhas)


def contar(sessao: SparkSession, tabela: Path, mes: str) -> int:
    return sessao.read.format("delta").load(str(tabela)).where(F.col("mes") == mes).count()
