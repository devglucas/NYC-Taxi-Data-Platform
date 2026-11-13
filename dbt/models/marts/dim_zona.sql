select
    zona_id,
    distrito,
    zona,
    area_servico,
    zona_id in (1, 132, 138) as aeroporto
from {{ ref('stg_zonas') }}
