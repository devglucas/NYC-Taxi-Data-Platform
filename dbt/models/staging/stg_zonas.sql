select
    "LocationID"::integer as zona_id,
    "Borough" as distrito,
    "Zone" as zona,
    service_zone as area_servico
from {{ source('referencia', 'zonas') }}
