"""Endereços, caminhos e limites da plataforma."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

URL_TLC = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_ZONAS = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
URL_RELATORIO_OFICIAL = "https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv"
URL_CLIMA = "https://archive-api.open-meteo.com/v1/archive"
USER_AGENT = "data-engineering-portfolio"

# Central Park, a estação de referência de Nova York.
LATITUDE, LONGITUDE = 40.7812, -73.9665
FUSO_NY = "America/New_York"

LIMITE_DURACAO_HORAS = 24
LIMITE_DISTANCIA_MILHAS = 200
LIMITE_VALOR_USD = 1000
TAXA_QUARENTENA_CRITICA = 0.10

_FORMATO_MES = re.compile(r"^20\d{2}-(0[1-9]|1[0-2])$")


def validar_mes(mes: str) -> str:
    if not _FORMATO_MES.match(mes or ""):
        raise ValueError(f"mês inválido: {mes!r}; use AAAA-MM")
    return mes


def url_viagens(mes: str, base: str | None = None) -> str:
    return f"{(base or URL_TLC).rstrip('/')}/yellow_tripdata_{validar_mes(mes)}.parquet"


@dataclass(frozen=True)
class Caminhos:
    dados: Path
    output: Path

    @classmethod
    def do_ambiente(cls) -> Caminhos:
        raiz = Path(__file__).resolve().parent.parent
        return cls(
            Path(os.getenv("DADOS_DIR", raiz / "dados")),
            Path(os.getenv("OUTPUT_DIR", raiz / "output")),
        )

    @property
    def raw(self) -> Path:
        return self.dados / "raw"

    @property
    def staging(self) -> Path:
        return self.dados / "staging"

    @property
    def lake(self) -> Path:
        return self.dados / "lake"

    @property
    def controle(self) -> Path:
        return self.dados / "controle"

    @property
    def warehouse(self) -> Path:
        return self.dados / "warehouse.duckdb"

    @property
    def alertas(self) -> Path:
        return self.dados / "alertas.jsonl"

    def arquivo_viagens(self, mes: str) -> Path:
        return self.raw / "viagens" / f"yellow_tripdata_{mes}.parquet"

    def arquivo_clima(self, mes: str) -> Path:
        return self.raw / "clima" / f"clima_{mes}.json"

    @property
    def zonas(self) -> Path:
        return self.raw / "referencia" / "taxi_zone_lookup.csv"

    @property
    def relatorio_oficial(self) -> Path:
        return self.raw / "referencia" / "data_reports_monthly.csv"
