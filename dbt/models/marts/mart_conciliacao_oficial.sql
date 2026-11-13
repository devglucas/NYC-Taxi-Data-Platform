with pipeline as (
    select mes, sum(viagens) as viagens from {{ ref('int_viagens_hora_zona') }} group by all
)

select
    p.mes,
    round(p.viagens / day(last_day(strptime(p.mes || '-01', '%Y-%m-%d'))), 1) as viagens_dia_pipeline,
    o.viagens_dia_oficial,
    round(100 * (p.viagens / day(last_day(strptime(p.mes || '-01', '%Y-%m-%d'))) / o.viagens_dia_oficial - 1), 2) as diferenca_pct,
    case
        when o.viagens_dia_oficial is null then 'pendente'
        when abs(p.viagens / day(last_day(strptime(p.mes || '-01', '%Y-%m-%d'))) / o.viagens_dia_oficial - 1) * 100
            <= {{ var('tolerancia_oficial_pct') }} then 'ok'
        else 'divergente'
    end as estado
from pipeline as p
left join {{ ref('stg_relatorio_oficial') }} as o using (mes)
