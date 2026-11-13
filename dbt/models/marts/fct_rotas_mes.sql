{{ config(materialized='incremental', unique_key='mes', incremental_strategy='delete+insert') }}

-- As 300 rotas mais frequentes de cada mês.
with rotas as (
    select
        mes,
        zona_origem,
        zona_destino,
        count(*) as viagens,
        round(sum(total_usd), 2) as total_usd,
        round(median(duracao_min) filter (where duracao_valida), 2) as minutos_mediana,
        round(median(distancia_milhas), 2) as distancia_mediana_milhas
    from {{ ref('stg_viagens') }}
    {{ filtro_do_mes() }}
    group by all
)

select *
from rotas
qualify row_number() over (partition by mes order by viagens desc, zona_origem, zona_destino) <= 300
