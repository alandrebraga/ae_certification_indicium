{% macro generate_dynamic_date_dimension() %}
    {% set query %}
        select cast({{ dbt_date.n_months_away(60, tz="America/Sao_Paulo") }} as string) as end_date
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set end_date = results.rows[0][0] %}
    {% else %}
        {% set end_date = [] %}
    {% endif %}

    {% set date_dimension_sql = dbt_date.get_date_dimension("1990-01-01", end_date) %}
    {{ return(date_dimension_sql) }}
{% endmacro %}