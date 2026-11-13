select mes, motivo_rejeicao as motivo, count(*) as linhas
from {{ source('lake', 'silver_quarentena') }}
group by all
