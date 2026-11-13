# Dashboard no Tableau Public: especificação

Este guia é para montar o dashboard no **Tableau Public** (versão gratuita do Tableau Desktop)
a partir das extrações que a plataforma gera em [`output/tableau/`](../output/tableau/). A
plataforma atualiza os CSVs a cada mês processado. O Tableau Public não se conecta direto ao
warehouse, então os dados são atualizados abrindo a pasta de trabalho e publicando de novo.

## Público e perguntas

Para quem gerencia a frota ou analisa a mobilidade da cidade. O dashboard deve responder:

1. Quando a demanda acontece, por dia da semana e hora?
2. A chuva e a neve mudam a demanda e a gorjeta?
3. De onde saem as viagens que mais faturam, e para onde vão?
4. Quanto a taxa de congestionamento pesa nas viagens desde 2025?
5. Os números são confiáveis? Como ficam contra o relatório oficial da TLC?

## Fontes de dados

Conecte cada CSV como **Arquivo de texto**.

| Arquivo | Linhas | Grão | Uso |
|---|---|---|---|
| `demanda_clima_hora.csv` | ~24 mil | uma hora da cidade | Painéis 1 e 2 |
| `zonas_mes.csv` | ~8,6 mil | mês × zona de embarque | Painel 3 e mapa |
| `rotas_mes.csv` | ~9,9 mil | mês × origem × destino (300 por mês) | Painel 3 |
| `resumo_mensal.csv` | ~600 | mês × canal × forma de pagamento | Painel 4 |
| `conciliacao_oficial.csv` | 33 | mês | Painel 5 |
| `qualidade_mes.csv` | 33 | mês | Painel 5 |
| `zonas.csv` | 265 | zona | Nomes e distritos |

**Mapa:** baixe `taxi_zones.zip` na [página de dados da TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(Taxi Zone Shapefile), abra como **Arquivo espacial** e relacione `LocationID` com
`zonas_mes.zona_origem`. O shapefile não está no repositório.

### Relacionamentos

Use relacionamentos, não joins, para não duplicar linhas:

- `zonas_mes.zona_origem` → `zonas.zona_id`
- `rotas_mes.zona_origem` → `zonas.zona_id`
- Shapefile `LocationID` → `zonas_mes.zona_origem`

As outras fontes ficam soltas, cada uma alimentando suas próprias folhas.

## Campos calculados

```text
// demanda_clima_hora
Data                 = DATE([hora])
Variação vs seco     = [indice_demanda] - 1
Hora com chuva       = [condicao] = "Chuva fraca" OR [condicao] = "Chuva moderada ou forte"

// zonas_mes
Receita por viagem   = SUM([total_usd]) / SUM([viagens])

// resumo_mensal
Forma de pagamento   = CASE [pagamento]
                         WHEN 0 THEN "Tarifa flexível" WHEN 1 THEN "Cartão" WHEN 2 THEN "Dinheiro"
                         WHEN 3 THEN "Sem cobrança" WHEN 4 THEN "Contestada" ELSE "Outros" END
% com taxa CBD       = SUM([viagens_taxa_cbd]) / SUM([viagens])
```

Use sempre `SUM(viagens)` como peso. Não faça a média de colunas que já são médias, como
`ticket_medio_usd` e `minutos_medios`, entre zonas ou meses: recalcule pela soma.

## Folhas

| # | Folha | Tipo | Linhas e colunas | Cor e detalhe |
|---|---|---|---|---|
| 1 | Demanda por hora | Mapa de calor | `dia_semana` × `hora_do_dia` | `AVG(viagens)`; ordene os dias de segunda a domingo |
| 2 | Clima e demanda | Barras | `condicao` × `MEDIAN(Variação vs seco)` | Rótulo com `COUNT(hora)`; deixe "Seco" em cinza |
| 3 | Gorjeta por hora | Linha | `hora_do_dia` × média de `gorjeta_pct_cartao` ponderada por `viagens` | Uma linha por `Hora com chuva` |
| 4 | Mapa de zonas | Mapa preenchido | Geometria do shapefile | Cor por `SUM(viagens)`; tooltip com receita e `% app` |
| 5 | Rotas principais | Barras horizontais | Top 15 de `zona_origem_nome → zona_destino_nome` | `SUM(viagens)`; filtro pelo mês |
| 6 | Taxa de congestionamento | Barras | `mes` × `% com taxa CBD` | A partir de 2025-01 |
| 7 | Conciliação oficial | Barras | `mes` × `diferenca_pct` | Faixa de referência de −3% a +3%; cor por `estado` |

## Dashboards

1. **Quando e onde**, com as folhas 1, 4 e 5, e filtros de mês (intervalo) e distrito.
2. **Clima, gorjeta e taxa**, com as folhas 2, 3 e 6.
3. **Confiabilidade**, com a folha 7 e os números de `qualidade_mes`: linhas brutas, quarentena
   e % em quarentena.

Inclua um texto curto em cada dashboard: as fontes (TLC e Open-Meteo, CC BY 4.0) e que a
relação entre chuva e demanda é uma associação, não um efeito causal.

## Publicação

1. Crie a conta em [public.tableau.com](https://public.tableau.com) (só você pode criar).
2. Monte a pasta de trabalho no Tableau Public e salve: salvar já publica.
3. Coloque o link no README do projeto, na seção do dashboard.

Tudo o que é publicado no Tableau Public fica visível para qualquer pessoa. Os CSVs têm só
agregados, sem nenhuma viagem individual.
