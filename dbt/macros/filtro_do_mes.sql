{# Nos incrementais, processa só o mês pedido em var('mes'); sem ele, processa tudo. #}
{% macro filtro_do_mes(coluna='mes') -%}
    {%- if is_incremental() and var('mes') %} where {{ coluna }} = '{{ var("mes") }}'{% endif -%}
{%- endmacro %}
