-- A entrada do horário de verão apaga as 2h de um domingo de março: até uma hora por ano
-- pode ficar sem clima. Mais que isso indica falha na ingestão do clima.
{{ config(warn_if='>0', error_if='>4') }}
select hora, viagens
from {{ ref('fct_demanda_clima') }}
where condicao = 'Sem dado'
