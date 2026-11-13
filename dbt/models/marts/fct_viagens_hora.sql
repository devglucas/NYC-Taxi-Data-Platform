{{ config(materialized='incremental', unique_key='mes', incremental_strategy='delete+insert') }}

select
    mes,
    hora,
    zona_origem,
    sum(viagens) as viagens,
    sum(viagens) filter (where canal <> 'Rua ou ponto') as viagens_app,
    round(sum(total_usd), 2) as total_usd,
    round(sum(gorjeta_usd), 2) as gorjeta_usd
from {{ ref('int_viagens_hora_zona') }}
{{ filtro_do_mes() }}
group by all
