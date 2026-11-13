with silver as (select mes, sum(total_usd) as total from {{ ref('stg_viagens') }} group by all),
mart as (select mes, sum(total_usd) as total from {{ ref('fct_zonas_mes') }} group by all)
select mes, s.total as silver, m.total as mart
from silver as s
join mart as m using (mes)
where abs(s.total - m.total) > 1
