select
    mes,
    canal,
    pagamento,
    sum(viagens) as viagens,
    round(sum(total_usd), 2) as total_usd,
    round(sum(tarifa_usd), 2) as tarifa_usd,
    round(sum(gorjeta_usd), 2) as gorjeta_usd,
    round(sum(taxa_cbd_usd), 2) as taxa_cbd_usd,
    sum(viagens_taxa_cbd) as viagens_taxa_cbd,
    round(sum(minutos_validos) / nullif(sum(viagens_duracao_valida), 0), 2) as minutos_medios
from {{ ref('int_viagens_hora_zona') }}
group by all
