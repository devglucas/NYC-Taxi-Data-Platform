{{ config(materialized='incremental', unique_key='mes', incremental_strategy='delete+insert', on_schema_change='fail') }}

with por_hora as (
    select
        mes,
        hora,
        sum(viagens) as viagens,
        sum(total_usd) as total_usd,
        sum(gorjeta_usd) filter (where pagamento = 1) as gorjeta_cartao_usd,
        sum(tarifa_usd) filter (where pagamento = 1) as tarifa_cartao_usd,
        sum(minutos_validos) / nullif(sum(viagens_duracao_valida), 0) as minutos_medios,
        sum(distancia_duracao_valida) / nullif(sum(minutos_validos) / 60, 0) as velocidade_media_mph
    from {{ ref('int_viagens_hora_zona') }}
    {{ filtro_do_mes() }}
    group by all
),

com_clima as (
    select
        p.*,
        isodow(p.hora) as dia_semana_num,
        hour(p.hora) as hora_do_dia,
        c.temperatura_c,
        c.precipitacao_mm,
        c.neve_cm,
        coalesce(c.condicao, 'Sem dado') as condicao
    from por_hora as p
    left join {{ ref('stg_clima_hora') }} as c on c.hora = p.hora
),

-- Referência: a mediana das horas secas com o mesmo dia da semana e horário, no mesmo mês.
referencia as (
    select mes, dia_semana_num, hora_do_dia, median(viagens) as viagens_referencia_seco
    from com_clima
    where condicao = 'Seco'
    group by all
)

select
    c.mes,
    c.hora,
    c.dia_semana_num,
    c.hora_do_dia,
    c.viagens,
    round(c.total_usd, 2) as total_usd,
    round(100 * c.gorjeta_cartao_usd / nullif(c.tarifa_cartao_usd, 0), 2) as gorjeta_pct_cartao,
    round(c.minutos_medios, 2) as minutos_medios,
    round(c.velocidade_media_mph, 2) as velocidade_media_mph,
    c.temperatura_c,
    c.precipitacao_mm,
    c.neve_cm,
    c.condicao,
    r.viagens_referencia_seco,
    round(c.viagens / nullif(r.viagens_referencia_seco, 0), 4) as indice_demanda
from com_clima as c
left join referencia as r using (mes, dia_semana_num, hora_do_dia)
