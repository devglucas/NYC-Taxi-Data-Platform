{{ config(unique_key='mes', incremental_strategy='delete+insert') }}

select
    mes,
    hora,
    zona_origem,
    canal,
    pagamento,
    count(*) as viagens,
    sum(passageiros) as passageiros,
    sum(distancia_milhas) as distancia_milhas,
    sum(tarifa_usd) as tarifa_usd,
    sum(gorjeta_usd) as gorjeta_usd,
    sum(total_usd) as total_usd,
    sum(taxa_cbd_usd) as taxa_cbd_usd,
    count(*) filter (where paga_taxa_cbd) as viagens_taxa_cbd,
    count(*) filter (where duracao_valida) as viagens_duracao_valida,
    sum(duracao_min) filter (where duracao_valida) as minutos_validos,
    sum(distancia_milhas) filter (where duracao_valida) as distancia_duracao_valida
from {{ ref('stg_viagens') }}
{{ filtro_do_mes() }}
group by all
