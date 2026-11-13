"""Acesso HTTP às fontes: consulta sem baixar, download íntegro e JSON com repetição."""
from __future__ import annotations

import hashlib
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb
import requests

from src.config import USER_AGENT

CABECALHOS = {"User-Agent": USER_AGENT}
# A TLC responde 403, e não 404, para um mês ainda não publicado.
STATUS_NAO_PUBLICADO = {403, 404}


class FonteInstavel(Exception):
    """Falha que pode passar sozinha: vale tentar de novo."""


class ArquivoCorrompido(FonteInstavel):
    """O arquivo chegou incompleto ou ilegível."""


@dataclass(frozen=True)
class Consulta:
    url: str
    disponivel: bool
    status: int
    etag: str | None = None
    tamanho: int | None = None
    ultima_modificacao: str | None = None

    def como_dict(self) -> dict:
        return asdict(self)


def consultar(url: str, timeout: int = 30) -> Consulta:
    try:
        resposta = requests.head(url, headers=CABECALHOS, timeout=timeout, allow_redirects=True)
    except requests.RequestException as erro:
        raise FonteInstavel(f"sem resposta de {url}: {erro}") from erro
    if resposta.status_code in STATUS_NAO_PUBLICADO:
        return Consulta(url, False, resposta.status_code)
    if resposta.status_code != 200:
        raise FonteInstavel(f"{url} respondeu {resposta.status_code}")
    tamanho = resposta.headers.get("Content-Length")
    return Consulta(
        url, True, 200, (resposta.headers.get("ETag") or "").strip('"') or None,
        int(tamanho) if tamanho else None, resposta.headers.get("Last-Modified"),
    )


def baixar_parquet(url: str, destino: Path, timeout: int = 120) -> dict:
    """Baixa para um temporário, confere tamanho e leitura e só então move para o destino."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".parcial")
    resumo = hashlib.sha256()
    recebidos = 0
    try:
        try:
            with requests.get(url, headers=CABECALHOS, timeout=timeout, stream=True) as resposta:
                if resposta.status_code != 200:
                    raise FonteInstavel(f"{url} respondeu {resposta.status_code}")
                esperado = resposta.headers.get("Content-Length")
                with temporario.open("wb") as arquivo:
                    for bloco in resposta.iter_content(chunk_size=1 << 20):
                        arquivo.write(bloco)
                        resumo.update(bloco)
                        recebidos += len(bloco)
        except requests.RequestException as erro:
            raise FonteInstavel(f"download interrompido de {url}: {erro}") from erro
        if esperado and int(esperado) != recebidos:
            raise ArquivoCorrompido(f"recebi {recebidos} de {esperado} bytes")
        linhas = contar_linhas(temporario)
        os.replace(temporario, destino)
    finally:
        temporario.unlink(missing_ok=True)
    return {"sha256": resumo.hexdigest(), "tamanho": recebidos, "linhas": linhas}


def contar_linhas(parquet: Path) -> int:
    try:
        with duckdb.connect() as conexao:
            return conexao.execute(
                "select coalesce(sum(num_rows), 0) from parquet_file_metadata(?)", [str(parquet)]
            ).fetchall()[0][0]
    except duckdb.Error as erro:
        raise ArquivoCorrompido(f"{parquet.name} não é um Parquet legível: {erro}") from erro


def baixar_texto(url: str, destino: Path, timeout: int = 60) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    try:
        resposta = requests.get(url, headers=CABECALHOS, timeout=timeout)
        resposta.raise_for_status()
    except requests.RequestException as erro:
        raise FonteInstavel(f"falha ao baixar {url}: {erro}") from erro
    temporario = destino.with_name(destino.name + ".parcial")
    temporario.write_bytes(resposta.content)
    os.replace(temporario, destino)
    return destino


def obter_json(url: str, parametros: dict, tentativas: int = 4, espera: float = 2.0, timeout: int = 60) -> dict:
    """GET com repetição para erros 429 e 5xx; outros erros sobem na hora."""
    for tentativa in range(1, tentativas + 1):
        try:
            resposta = requests.get(url, params=parametros, headers=CABECALHOS, timeout=timeout)
        except requests.RequestException as erro:
            if tentativa == tentativas:
                raise FonteInstavel(f"sem resposta de {url}: {erro}") from erro
        else:
            if resposta.status_code == 200:
                return resposta.json()
            if resposta.status_code != 429 and resposta.status_code < 500:
                raise ValueError(f"{url} respondeu {resposta.status_code}: {resposta.text[:200]}")
            if tentativa == tentativas:
                raise FonteInstavel(f"{url} respondeu {resposta.status_code} após {tentativas} tentativas")
        time.sleep(espera * 2 ** (tentativa - 1))
    raise AssertionError("inalcançável")
