{{ config(materialized='incremental', unique_key='mes', incremental_strategy='delete+insert', on_schema_change='fail') }}

select
    mes,
    zona_origem,
    sum(viagens) as viagens,
    round(sum(total_usd), 2) as total_usd,
    round(sum(total_usd) / sum(viagens), 2) as ticket_medio_usd,
    round(sum(minutos_validos) / nullif(sum(viagens_duracao_valida), 0), 2) as minutos_medios,
    round(100.0 * sum(viagens) filter (where pagamento = 1) / sum(viagens), 2) as pct_cartao,
    round(100.0 * sum(viagens) filter (where canal <> 'Rua ou ponto') / sum(viagens), 2) as pct_app,
    round(100.0 * sum(viagens_taxa_cbd) / sum(viagens), 2) as pct_taxa_cbd
from {{ ref('int_viagens_hora_zona') }}
{{ filtro_do_mes() }}
group by all
