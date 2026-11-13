{% test combinacao_unica(model, colunas) %}
select {{ colunas | join(', ') }}, count(*) as repeticoes
from {{ model }}
group by {{ colunas | join(', ') }}
having count(*) > 1
{% endtest %}
