"""Painel HTML estático, gerado a cada atualização dos marts.

Um arquivo só, com gráficos embutidos: abre em qualquer navegador e não depende do Tableau.
Cada título afirma o achado, calculado dos dados no momento da geração.
"""
from __future__ import annotations

import base64
import html
import io
from datetime import datetime

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, MaxNLocator  # noqa: E402

from src.config import Caminhos  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#8b5cc7"]
CINZA, TINTA, TINTA_2, GRADE, EIXO, SUPERFICIE = "#b5b3ab", "#1f1e1b", "#52514e", "#e1e0d9", "#c3c2b7", "#fcfcfb"
DIAS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
ORDEM_CONDICAO = ["Seco", "Chuva fraca", "Chuva moderada ou forte", "Neve"]

plt.rcParams.update({
    "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "savefig.facecolor": SUPERFICIE,
    "axes.edgecolor": EIXO, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
    "text.color": TINTA, "axes.spines.top": False, "axes.spines.right": False, "axes.titlelocation": "left",
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlepad": 18, "font.size": 9.5,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRADE, "axes.axisbelow": True,
})


def _png(fig) -> str:
    buffer = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buffer, format="png", dpi=130)
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode()


def _salvar(fig, caminhos: Caminhos, nome: str) -> str:
    """Grava o PNG em output/graficos (para o README) e devolve a versão embutível."""
    destino = caminhos.output / "graficos" / f"{nome}.png"
    destino.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(destino, dpi=150)
    return _png(fig)


def _mapa_de_calor(con, caminhos) -> tuple[str, str]:
    linhas = con.execute(
        "select dia_semana_num, hora_do_dia, avg(viagens) from fct_demanda_clima group by all"
    ).fetchall()
    matriz = [[0.0] * 24 for _ in range(7)]
    for dia, hora, media in linhas:
        matriz[dia - 1][hora] = media
    pico_dia, pico_hora = max(((d, h) for d in range(7) for h in range(24)), key=lambda p: matriz[p[0]][p[1]])
    fig, ax = plt.subplots(figsize=(10, 3.6))
    imagem = ax.imshow(matriz, aspect="auto", cmap="Blues")
    ax.set_yticks(range(7), DIAS)
    ax.set_xticks(range(0, 24, 2), [f"{h}h" for h in range(0, 24, 2)])
    ax.grid(False)
    fig.colorbar(imagem, ax=ax, label="viagens por hora, em média", shrink=0.9)
    pico = f"{matriz[pico_dia][pico_hora]:,.0f}".replace(",", ".")
    titulo = f"O pico é {DIAS[pico_dia].lower()} às {pico_hora}h, com {pico} viagens por hora"
    ax.set_title(titulo)
    return titulo, _salvar(fig, caminhos, "01_demanda_por_hora")


def _chuva(con, caminhos) -> tuple[str, str]:
    linhas = con.execute(
        """select condicao, count(*) as horas, median(indice_demanda) as indice
        from fct_demanda_clima where indice_demanda is not null and condicao <> 'Sem dado'
        group by all"""
    ).fetchall()
    dados = {c: (h, i) for c, h, i in linhas}
    ordem = [c for c in ORDEM_CONDICAO if c in dados]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    valores = [(dados[c][1] - 1) * 100 for c in ordem]
    cores = [CINZA if c == "Seco" else SERIES[i % len(SERIES)] for i, c in enumerate(ordem)]
    ax.bar(range(len(ordem)), valores, color=cores, width=0.6)
    ax.axhline(0, color=EIXO, linewidth=1)
    ax.set_xticks(range(len(ordem)), [f"{c}\n{dados[c][0]:,} horas".replace(",", ".") for c in ordem])
    ax.yaxis.set_major_locator(MaxNLocator(nbins=6, integer=True))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    ax.set_ylabel("viagens contra horas secas\ncomparáveis (mediana)")
    forte = dados.get("Chuva moderada ou forte")
    titulo = (
        f"Com chuva moderada ou forte, a hora tem {(forte[1] - 1) * 100:+.0f}% de viagens que uma hora seca comparável"
        if forte else "Demanda por condição do tempo"
    )
    ax.set_title(titulo)
    return titulo, _salvar(fig, caminhos, "02_chuva_e_demanda")


def _gorjeta(con, caminhos) -> tuple[str, str]:
    linhas = con.execute(
        """select hora_do_dia, sum(gorjeta_pct_cartao * viagens) / sum(viagens)
        from fct_demanda_clima where gorjeta_pct_cartao is not null group by all order by 1"""
    ).fetchall()
    horas, valores = zip(*linhas, strict=True)
    minimo, maximo = min(linhas, key=lambda x: x[1]), max(linhas, key=lambda x: x[1])
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.plot(horas, valores, color=SERIES[0], linewidth=2)
    ax.set_xticks(range(0, 24, 2), [f"{h}h" for h in range(0, 24, 2)])
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("gorjeta sobre a tarifa,\nem pagamentos com cartão")
    titulo = f"A gorjeta vai de {minimo[1]:.1f}% às {minimo[0]}h a {maximo[1]:.1f}% às {maximo[0]}h"
    ax.set_title(titulo)
    return titulo, _salvar(fig, caminhos, "03_gorjeta_por_hora")


def _taxa_cbd(con, caminhos) -> tuple[str, str] | None:
    linhas = con.execute(
        """select mes, 100.0 * sum(viagens_taxa_cbd) / sum(viagens), sum(taxa_cbd_usd)
        from fct_mensal group by mes having sum(viagens_taxa_cbd) > 0 order by mes"""
    ).fetchall()
    if not linhas:
        return None
    meses, pct, receita = zip(*linhas, strict=True)
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.bar(range(len(meses)), pct, color=SERIES[3], width=0.8)
    passo = max(1, len(meses) // 10)
    ax.set_xticks(range(0, len(meses), passo), [meses[i] for i in range(0, len(meses), passo)], rotation=45, ha="right")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("viagens que pagam a taxa")
    total = sum(receita) / 1e6
    titulo = f"Cerca de {sum(pct) / len(pct):.0f}% das viagens pagam a taxa de congestionamento, US$ {total:.0f} milhões desde {meses[0]}"
    ax.set_title(titulo.replace("$", r"\$"))
    return titulo, _salvar(fig, caminhos, "04_taxa_de_congestionamento")


def _conciliacao(con, caminhos) -> tuple[str, str] | None:
    linhas = con.execute(
        "select mes, diferenca_pct, estado from mart_conciliacao_oficial where diferenca_pct is not null order by mes"
    ).fetchall()
    if not linhas:
        return None
    meses, valores, estados = zip(*linhas, strict=True)
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.bar(range(len(meses)), valores, color=[SERIES[1] if e == "divergente" else SERIES[0] for e in estados], width=0.8)
    ax.axhspan(-3, 3, color=GRADE, alpha=0.5, zorder=0)
    ax.axhline(0, color=EIXO, linewidth=1)
    passo = max(1, len(meses) // 10)
    ax.set_xticks(range(0, len(meses), passo), [meses[i] for i in range(0, len(meses), passo)], rotation=45, ha="right")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    ax.set_ylabel("viagens por dia, contra o oficial")
    fora = estados.count("divergente")
    titulo = f"{len(meses) - fora} de {len(meses)} meses ficam a até 3% da contagem oficial da TLC"
    ax.set_title(titulo)
    return titulo, _salvar(fig, caminhos, "05_conciliacao_oficial")


def _indicadores(con) -> list[tuple[str, str]]:
    meses, viagens, receita = con.execute("select count(distinct mes), sum(viagens), sum(total_usd) from fct_mensal").fetchall()[0]
    bruto, quarentena = con.execute("select sum(linhas_bruto), sum(linhas_quarentena) from mart_qualidade_mes").fetchall()[0]
    ok, total = con.execute("select count(*) filter (estado = 'ok'), count(*) filter (estado <> 'pendente') from mart_conciliacao_oficial").fetchall()[0]
    numero = lambda v: f"{v:,.0f}".replace(",", ".")  # noqa: E731
    return [
        ("Meses", str(meses)),
        ("Viagens válidas", numero(viagens)),
        ("Receita registrada", f"US$ {receita / 1e9:.2f} bi".replace(".", ",")),
        ("Linhas em quarentena", f"{100 * quarentena / bruto:.2f}%".replace(".", ",")),
        ("Meses dentro de 3% do oficial", f"{ok} de {total}"),
    ]


def _tabela_zonas(con) -> str:
    linhas = con.execute(
        """select z.zona, z.distrito, sum(f.viagens), sum(f.total_usd)
        from fct_zonas_mes f join dim_zona z on z.zona_id = f.zona_origem
        group by all order by 4 desc limit 10"""
    ).fetchall()
    corpo = "".join(
        f"<tr><td>{html.escape(str(z))}</td><td>{html.escape(str(d))}</td>"
        f"<td class=n>{v:,.0f}</td><td class=n>{t / 1e6:,.1f}</td></tr>".replace(",", ".")
        for z, d, v, t in linhas
    )
    return f"<table><thead><tr><th>Zona de embarque</th><th>Distrito</th><th class=n>Viagens</th><th class=n>US$ milhões</th></tr></thead><tbody>{corpo}</tbody></table>"


def gerar_painel(caminhos: Caminhos) -> str:
    with duckdb.connect(str(caminhos.warehouse), read_only=True) as con:
        indicadores = _indicadores(con)
        graficos = [g for g in (_mapa_de_calor(con, caminhos), _chuva(con, caminhos), _gorjeta(con, caminhos),
                                _taxa_cbd(con, caminhos), _conciliacao(con, caminhos)) if g]
        zonas = _tabela_zonas(con)
        periodo = con.execute("select min(mes), max(mes) from fct_mensal").fetchall()[0]
    cartoes = "".join(f"<div class=kpi><span>{html.escape(r)}</span><strong>{html.escape(v)}</strong></div>" for r, v in indicadores)
    figuras = "".join(
        f"<figure><img alt='{html.escape(t)}' src='data:image/png;base64,{img}'></figure>" for t, img in graficos
    )
    pagina = f"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Táxis de Nova York</title>
<style>
body{{margin:0;background:#fcfcfb;color:#1f1e1b;font:15px/1.5 system-ui,-apple-system,sans-serif}}
main{{max-width:1040px;margin:0 auto;padding:32px 16px}}
h1{{font-size:26px;margin:0 0 4px}} p.sub{{color:#52514e;margin:0 0 24px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:28px}}
.kpi{{border:1px solid #e1e0d9;border-radius:8px;padding:12px 14px;background:#fff}}
.kpi span{{display:block;color:#52514e;font-size:13px}} .kpi strong{{font-size:22px}}
figure{{margin:0 0 24px;border:1px solid #e1e0d9;border-radius:8px;background:#fff;padding:8px}}
img{{width:100%;height:auto;display:block}}
table{{width:100%;border-collapse:collapse;background:#fff}} th,td{{padding:6px 8px;border-bottom:1px solid #e1e0d9;text-align:left}}
.n{{text-align:right;font-variant-numeric:tabular-nums}} footer{{color:#52514e;font-size:13px;margin-top:24px}}
</style></head><body><main>
<h1>Táxis amarelos de Nova York</h1>
<p class=sub>{periodo[0]} a {periodo[1]} · gerado em {datetime.now().astimezone():%Y-%m-%d %H:%M %Z} pela plataforma, a partir do warehouse</p>
<div class=kpis>{cartoes}</div>
{figuras}
<h2>Zonas de embarque com maior receita</h2>
{zonas}
<footer>Fontes: NYC Taxi and Limousine Commission (registros de viagem e relatório mensal) e Open-Meteo
(dados históricos de clima, CC BY 4.0). A relação entre chuva e demanda é uma associação, não um efeito causal medido.</footer>
</main></body></html>"""
    destino = caminhos.output / "painel" / "index.html"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(pagina, encoding="utf-8")
    return str(destino)
