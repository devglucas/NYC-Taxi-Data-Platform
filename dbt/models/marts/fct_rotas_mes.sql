{{ config(materialized='incremental', unique_key='mes', incremental_strategy='delete+insert') }}

-- As 300 rotas mais frequentes de cada mês. As medianas são calculadas só para elas: calcular
-- a mediana de todos os pares de zonas estourava a memória no full refresh dos 33 meses.
with contagem as (
    select mes, zona_origem, zona_destino, count(*) as viagens, round(sum(total_usd), 2) as total_usd
    from {{ ref('stg_viagens') }}
    {{ filtro_do_mes() }}
    group by all
),

principais as (
    select *
    from contagem
    qualify row_number() over (partition by mes order by viagens desc, zona_origem, zona_destino) <= 300
),

medianas as (
    select
        v.mes,
        v.zona_origem,
        v.zona_destino,
        round(median(v.duracao_min) filter (where v.duracao_valida), 2) as minutos_mediana,
        round(median(v.distancia_milhas), 2) as distancia_mediana_milhas
    from {{ ref('stg_viagens') }} as v
    semi join principais as p using (mes, zona_origem, zona_destino)
    {{ filtro_do_mes('v.mes') }}
    group by all
)

select p.mes, p.zona_origem, p.zona_destino, p.viagens, p.total_usd, m.minutos_mediana, m.distancia_mediana_milhas
from principais as p
join medianas as m using (mes, zona_origem, zona_destino)
