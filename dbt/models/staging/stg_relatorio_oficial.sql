select
    "Month/Year" as mes,
    replace("Trips Per Day", ',', '')::integer as viagens_dia_oficial
from {{ source('referencia', 'relatorio_oficial') }}
where "License Class" = 'Yellow'
