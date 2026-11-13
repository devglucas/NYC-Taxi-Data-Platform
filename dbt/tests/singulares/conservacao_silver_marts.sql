-- Cada viagem da silver aparece uma vez nos marts: a contagem por mês tem de bater.
with silver as (select mes, count(*) as viagens from {{ ref('stg_viagens') }} group by all),
mart as (select mes, sum(viagens) as viagens from {{ ref('fct_viagens_hora') }} group by all)
select coalesce(s.mes, m.mes) as mes, s.viagens as silver, m.viagens as mart
from silver as s
full join mart as m using (mes)
where s.viagens is distinct from m.viagens
