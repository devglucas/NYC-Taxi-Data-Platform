"""Clima horário de Nova York pela API histórica da Open-Meteo.

A API é consultada em UTC e convertida para o horário local, que é o mesmo das viagens.
No fim do horário de verão, a 1h da manhã acontece duas vezes: as duas medições viram uma
só, com a chuva somada. Na entrada do horário de verão, as 2h não existem e ficam sem linha.
"""
from __future__ import annotations

import calendar
import json
import os
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src.config import FUSO_NY, LATITUDE, LONGITUDE, URL_CLIMA, validar_mes
from src.fonte import obter_json

VARIAVEIS = ["temperature_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"]


def parametros_do_mes(mes: str) -> dict:
    """Um dia de folga em cada ponta cobre a diferença entre UTC e o horário local."""
    ano, numero = map(int, validar_mes(mes).split("-"))
    inicio = date(ano, numero, 1) - timedelta(days=1)
    fim = date(ano, numero, calendar.monthrange(ano, numero)[1]) + timedelta(days=1)
    return {
        "latitude": LATITUDE, "longitude": LONGITUDE, "start_date": inicio.isoformat(),
        "end_date": fim.isoformat(), "hourly": ",".join(VARIAVEIS), "timezone": "GMT",
    }


def baixar(mes: str, destino: Path, url: str = URL_CLIMA) -> Path:
    resposta = obter_json(url, parametros_do_mes(mes))
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".parcial")
    temporario.write_text(json.dumps(resposta), encoding="utf-8")
    os.replace(temporario, destino)
    return destino


def _valores(medicoes: list[dict], campo: str) -> list[float]:
    # JSON não distingue 0 de 0.0: tudo vira float para o esquema do Spark aceitar.
    return [float(m[campo]) for m in medicoes if m[campo] is not None]


def _media(medicoes: list[dict], campo: str) -> float | None:
    valores = _valores(medicoes, campo)
    return round(sum(valores) / len(valores), 2) if valores else None


def _soma(medicoes: list[dict], campo: str) -> float | None:
    valores = _valores(medicoes, campo)
    return round(sum(valores), 2) if valores else None


def horas_locais(resposta: dict, mes: str) -> list[dict]:
    """Converte a resposta em linhas por hora local do mês, sem horas repetidas."""
    fuso = ZoneInfo(FUSO_NY)
    horaria = resposta["hourly"]
    agrupado: dict[datetime, list[dict]] = defaultdict(list)
    for indice, texto in enumerate(horaria["time"]):
        utc = datetime.fromisoformat(texto).replace(tzinfo=UTC)
        local = utc.astimezone(fuso).replace(tzinfo=None)
        if local.strftime("%Y-%m") != mes:
            continue
        agrupado[local].append({variavel: horaria[variavel][indice] for variavel in VARIAVEIS})
    linhas = []
    for hora, medicoes in sorted(agrupado.items()):
        linhas.append(
            {
                "mes": mes, "hora_local": hora, "temperatura_c": _media(medicoes, "temperature_2m"),
                "precipitacao_mm": _soma(medicoes, "precipitation"), "chuva_mm": _soma(medicoes, "rain"),
                "neve_cm": _soma(medicoes, "snowfall"), "vento_kmh": _media(medicoes, "wind_speed_10m"),
                "medicoes": len(medicoes),
            }
        )
    return linhas
