with limites as (
    select min(hora)::date as inicio, max(hora)::date as fim from {{ ref('int_viagens_hora_zona') }}
)
select
    dia::date as data,
    year(dia) as ano,
    strftime(dia, '%Y-%m') as mes,
    isodow(dia) as dia_semana_num,
    ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][isodow(dia)] as dia_semana,
    isodow(dia) >= 6 as fim_de_semana
from limites, generate_series(limites.inicio, limites.fim, interval 1 day) as serie(dia)
