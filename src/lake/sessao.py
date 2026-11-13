"""Sessão Spark local com Delta Lake."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def criar(nome: str = "plataforma-taxi", particoes: int = 8) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    # Os workers do Spark precisam do mesmo Python do driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    construtor = (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[4]"))
        .appName(nome)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", str(particoes))
        # Os horários da TLC são locais e sem fuso: UTC na sessão impede qualquer conversão.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    sessao = configure_spark_with_delta_pip(construtor).getOrCreate()
    sessao.sparkContext.setLogLevel("ERROR")
    return sessao
