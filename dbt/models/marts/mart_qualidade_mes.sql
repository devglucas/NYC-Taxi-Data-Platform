with silver as (
    select mes, sum(viagens) as viagens from {{ ref('int_viagens_hora_zona') }} group by all
),

quarentena as (
    select mes, sum(linhas) as linhas_quarentena from {{ ref('stg_quarentena') }} group by all
)

select
    s.mes,
    s.viagens + coalesce(q.linhas_quarentena, 0) as linhas_bruto,
    s.viagens,
    coalesce(q.linhas_quarentena, 0) as linhas_quarentena,
    round(100.0 * coalesce(q.linhas_quarentena, 0) / (s.viagens + coalesce(q.linhas_quarentena, 0)), 3) as pct_quarentena
from silver as s
left join quarentena as q using (mes)
