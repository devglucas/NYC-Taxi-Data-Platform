select
    mes,
    hora_local as hora,
    data,
    temperatura_c,
    precipitacao_mm,
    neve_cm,
    vento_kmh,
    -- Limiares do Serviço Meteorológico dos EUA para chuva leve e moderada por hora.
    case
        when coalesce(neve_cm, 0) > 0 then 'Neve'
        when coalesce(precipitacao_mm, 0) < 0.1 then 'Seco'
        when precipitacao_mm < 2.5 then 'Chuva fraca'
        else 'Chuva moderada ou forte'
    end as condicao
from {{ source('lake', 'silver_clima') }}
