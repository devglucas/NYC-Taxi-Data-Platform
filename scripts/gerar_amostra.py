"""Gera uma amostra sintética e pequena para o CI: dois meses de viagens, clima e referências.

Não usa nenhum registro real da TLC. Inclui de propósito um estorno, uma corrida em
duplicidade, uma viagem fora do mês e uma do fornecedor 7 com duração zero.
"""
from __future__ import annotations

import calendar
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

MESES = ["2026-01", "2026-02"]
ZONAS = list(range(1, 266))


def viagens_do_mes(mes: str, gerador: random.Random) -> list[dict]:
    ano, numero = map(int, mes.split("-"))
    dias = calendar.monthrange(ano, numero)[1]
    linhas = []
    for _ in range(3000):
        embarque = datetime(ano, numero, 1) + timedelta(minutes=gerador.randrange(dias * 24 * 60))
        minutos = gerador.randint(4, 45)
        distancia = round(minutos * gerador.uniform(0.1, 0.4), 2)
        tarifa = round(3 + distancia * 2.8, 2)
        gorjeta = round(tarifa * gerador.choice([0, 0.15, 0.2]), 2)
        linhas.append(
            {
                "VendorID": gerador.choice([1, 2, 2, 7]), "tpep_pickup_datetime": embarque,
                "tpep_dropoff_datetime": embarque + timedelta(minutes=minutos), "passenger_count": 1,
                "trip_distance": distancia, "RatecodeID": 1, "store_and_fwd_flag": "N",
                "PULocationID": gerador.choice(ZONAS), "DOLocationID": gerador.choice(ZONAS),
                "payment_type": gerador.choice([1, 1, 2]), "fare_amount": tarifa, "extra": 1.0, "mta_tax": 0.5,
                "tip_amount": gorjeta, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
                "total_amount": round(tarifa + 2.5 + gorjeta, 2), "congestion_surcharge": 0.0, "Airport_fee": 0.0,
                "cbd_congestion_fee": gerador.choice([0.0, 0.75]), "request_source": None,
            }
        )
    for linha in linhas:
        if linha["VendorID"] == 7:
            linha["tpep_dropoff_datetime"] = linha["tpep_pickup_datetime"]
        linha["total_amount"] = round(linha["total_amount"] + linha["cbd_congestion_fee"], 2)
    linhas.append(linhas[0] | {"total_amount": -linhas[0]["total_amount"]})
    linhas.append(linhas[1] | {"request_source": "HV0003", "payment_type": 0})
    linhas.append(linhas[2] | {"tpep_pickup_datetime": datetime(2009, 1, 1), "tpep_dropoff_datetime": datetime(2009, 1, 1, 0, 10)})
    return linhas


def gravar_parquet(linhas: list[dict], destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tipos = {"tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp", "store_and_fwd_flag": "varchar",
             "request_source": "varchar", "VendorID": "integer", "PULocationID": "integer", "DOLocationID": "integer",
             "passenger_count": "bigint", "RatecodeID": "bigint", "payment_type": "bigint"}
    colunas = list(linhas[0])
    with duckdb.connect() as con:
        con.execute("create table t (" + ", ".join(f'"{c}" {tipos.get(c, "double")}' for c in colunas) + ")")
        con.executemany(f"insert into t values ({', '.join('?' for _ in colunas)})", [[r[c] for c in colunas] for r in linhas])
        con.execute(f"copy t to '{destino}' (format parquet)")


def clima_do_mes(mes: str, gerador: random.Random) -> dict:
    ano, numero = map(int, mes.split("-"))
    inicio = datetime(ano, numero, 1) - timedelta(days=1)
    horas = [inicio + timedelta(hours=h) for h in range((calendar.monthrange(ano, numero)[1] + 2) * 24)]
    chuva = [round(gerador.choice([0, 0, 0, 0, 0.5, 3.0]), 1) for _ in horas]
    return {"hourly": {"time": [h.strftime("%Y-%m-%dT%H:%M") for h in horas], "temperature_2m": [2.0] * len(horas),
                       "precipitation": chuva, "rain": chuva, "snowfall": [0.0] * len(horas), "wind_speed_10m": [10.0] * len(horas)}}


def main(dados: Path) -> None:
    gerador = random.Random(11)
    referencia = dados / "raw" / "referencia"
    referencia.mkdir(parents=True, exist_ok=True)
    (referencia / "taxi_zone_lookup.csv").write_text(
        '"LocationID","Borough","Zone","service_zone"\n' + "".join(f'{z},"Distrito {z % 5}","Zona {z}","Zona amarela"\n' for z in ZONAS),
        encoding="utf-8",
    )
    relatorio = "Month/Year,License Class,Trips Per Day\n"
    for mes in MESES:
        linhas = viagens_do_mes(mes, gerador)
        gravar_parquet(linhas, dados / "raw" / "viagens" / f"yellow_tripdata_{mes}.parquet")
        destino = dados / "raw" / "clima" / f"clima_{mes}.json"
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(json.dumps(clima_do_mes(mes, gerador)), encoding="utf-8")
        relatorio += f'{mes},Yellow,"{round(3000 / calendar.monthrange(*map(int, mes.split("-")))[1])}"\n'
    (referencia / "data_reports_monthly.csv").write_text(relatorio, encoding="utf-8")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
