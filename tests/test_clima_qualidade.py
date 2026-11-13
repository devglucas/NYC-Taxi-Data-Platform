from datetime import datetime

from src import clima, qualidade


def resposta(horas_utc, chuva):
    n = len(horas_utc)
    return {"hourly": {"time": horas_utc, "temperature_2m": [10.0] * n, "precipitation": chuva,
                       "rain": chuva, "snowfall": [0.0] * n, "wind_speed_10m": [5.0] * n}}


def test_parametros_incluem_um_dia_de_folga():
    p = clima.parametros_do_mes("2026-02")
    assert (p["start_date"], p["end_date"], p["timezone"]) == ("2026-01-31", "2026-03-01", "GMT")


def test_converte_utc_para_horario_local_e_filtra_o_mes():
    linhas = clima.horas_locais(resposta(["2026-09-01T03:00", "2026-09-01T04:00"], [0.0, 1.0]), "2026-09")
    # 03:00 UTC é 23:00 de 31 de agosto em Nova York; fica de fora.
    assert [linha["hora_local"] for linha in linhas] == [datetime(2026, 9, 1, 0, 0)]


def test_hora_repetida_no_fim_do_horario_de_verao_vira_uma_so():
    # Em 1º de novembro de 2026, a 1h local acontece às 05:00 e às 06:00 UTC.
    linhas = clima.horas_locais(resposta(["2026-11-01T05:00", "2026-11-01T06:00"], [0.5, 1.0]), "2026-11")
    assert len(linhas) == 1
    assert linhas[0]["hora_local"] == datetime(2026, 11, 1, 1, 0)
    assert linhas[0]["precipitacao_mm"] == 1.5 and linhas[0]["medicoes"] == 2


def test_valores_inteiros_viram_float():
    linhas = clima.horas_locais(resposta(["2026-09-01T04:00"], [0]), "2026-09")
    assert isinstance(linhas[0]["precipitacao_mm"], float)


def test_hora_inexistente_na_entrada_do_horario_de_verao():
    # Em 8 de março de 2026, o relógio pula das 2h para as 3h: 06:00 e 07:00 UTC viram 1h e 3h.
    linhas = clima.horas_locais(resposta(["2026-03-08T06:00", "2026-03-08T07:00"], [0.0, 0.0]), "2026-03")
    assert [linha["hora_local"].hour for linha in linhas] == [1, 3]


def metricas(**kw):
    return {"mes": "2026-09", "linhas_bruto": 100, "viagens": 98, "linhas_quarentena": 2, "fora_de_mes_na_silver": 0} | kw


def test_mes_sadio():
    resultado = qualidade.avaliar(metricas(), 720)
    assert not qualidade.criticas_reprovadas(resultado) and not qualidade.alertas_reprovados(resultado)


def test_linhas_perdidas_e_clima_incompleto_sao_criticos():
    resultado = qualidade.avaliar(metricas(viagens=90), 500)
    assert {v["verificacao"] for v in qualidade.criticas_reprovadas(resultado)} == {"conservacao_de_linhas", "clima_cobre_o_mes"}


def test_coluna_nova_so_alerta():
    resultado = qualidade.avaliar(metricas(), 720, ["taxa_nova"])
    assert not qualidade.criticas_reprovadas(resultado)
    assert [v["verificacao"] for v in qualidade.alertas_reprovados(resultado)] == ["colunas_novas_na_fonte"]
