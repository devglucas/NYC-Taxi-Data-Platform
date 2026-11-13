import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from src import contrato, fonte


@pytest.fixture
def servidor():
    estado = {"status": 200, "corpo": b"", "falhas": 0}

    class Manipulador(BaseHTTPRequestHandler):
        def _responder(self, corpo=True):
            if estado["falhas"] > 0:
                estado["falhas"] -= 1
                self.send_response(503)
                self.end_headers()
                return
            self.send_response(estado["status"])
            self.send_header("ETag", '"abc"')
            self.send_header("Content-Length", str(len(estado["corpo"])))
            self.end_headers()
            if corpo:
                self.wfile.write(estado["corpo"])

        def do_HEAD(self):
            self._responder(corpo=False)

        def do_GET(self):
            self._responder()

        def log_message(self, *args):
            pass

    http = HTTPServer(("127.0.0.1", 0), Manipulador)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    estado["url"] = f"http://127.0.0.1:{http.server_port}/a"
    yield estado
    http.shutdown()


def test_mes_nao_publicado(servidor):
    servidor["status"] = 403
    assert not fonte.consultar(servidor["url"]).disponivel


def test_download_valido(servidor, escrever_parquet, tmp_path):
    servidor["corpo"] = escrever_parquet([{}, {}]).read_bytes()
    assert fonte.baixar_parquet(servidor["url"], tmp_path / "x" / "a.parquet")["linhas"] == 2


def test_arquivo_ilegivel_preserva_o_anterior(servidor, tmp_path):
    servidor["corpo"] = b"nao e parquet"
    destino = tmp_path / "a.parquet"
    destino.write_bytes(b"anterior")
    with pytest.raises(fonte.ArquivoCorrompido):
        fonte.baixar_parquet(servidor["url"], destino)
    assert destino.read_bytes() == b"anterior" and not list(tmp_path.glob("*.parcial"))


def test_json_repete_erro_de_servidor(servidor):
    servidor["corpo"], servidor["falhas"] = b'{"ok": 1}', 2
    assert fonte.obter_json(servidor["url"], {}, tentativas=3, espera=0) == {"ok": 1}


def test_json_desiste_depois_das_tentativas(servidor):
    servidor["falhas"] = 5
    with pytest.raises(fonte.FonteInstavel):
        fonte.obter_json(servidor["url"], {}, tentativas=2, espera=0)


def test_json_erro_do_cliente_nao_repete(servidor):
    servidor["status"], servidor["corpo"] = 400, b"{}"
    with pytest.raises(ValueError):
        fonte.obter_json(servidor["url"], {}, tentativas=3, espera=0)


def test_contrato_aceita_caixa_diferente_e_aponta_coluna_faltando(escrever_parquet):
    assert contrato.verificar(escrever_parquet([{"airport_fee": 0.0}], remover={"Airport_fee"})).ok
    with pytest.raises(contrato.ContratoViolado, match="total_amount"):
        contrato.exigir(escrever_parquet([{}], nome="b.parquet", remover={"total_amount"}))
