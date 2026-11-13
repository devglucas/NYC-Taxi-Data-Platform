"""Esquema canônico das viagens: o mesmo para todos os meses, qualquer que seja o arquivo."""
from __future__ import annotations

from src.contrato import OBRIGATORIAS, OPCIONAIS

APELIDOS = {"tpep_pickup_datetime": "pickup", "tpep_dropoff_datetime": "dropoff"}
TIPOS = {
    "vendorid": "int", "passenger_count": "int", "ratecodeid": "int", "pulocationid": "int",
    "dolocationid": "int", "payment_type": "int", "store_and_fwd_flag": "string",
    "request_source": "string", "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
}
COLUNAS_FONTE = [*OBRIGATORIAS, *OPCIONAIS]
COLUNAS = [APELIDOS.get(nome, nome) for nome in COLUNAS_FONTE]
# Mesma corrida enviada duas vezes: igual nestes campos.
CHAVE_CORRIDA = ["vendorid", "pickup", "dropoff", "pulocationid", "dolocationid", "trip_distance", "fare_amount", "total_amount"]


def selecao(colunas_do_arquivo: list[str]) -> list[str]:
    """Expressões SQL que padronizam nomes e tipos e preenchem com nulo o que faltar."""
    por_nome = {nome.lower(): nome for nome in colunas_do_arquivo}
    expressoes = []
    for nome in COLUNAS_FONTE:
        tipo = TIPOS.get(nome, "double")
        apelido = APELIDOS.get(nome, nome)
        origem = f"try_cast(`{por_nome[nome]}` as {tipo})" if nome in por_nome else f"cast(null as {tipo})"
        expressoes.append(f"{origem} as {apelido}")
    return expressoes
