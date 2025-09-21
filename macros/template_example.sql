{% macro template_test(args) %}

 {% set query %}
    select true as boolean
 {% endset %}

 {% if execute %}
    {% set results=run_query(query).columns[0].values()[0] %}
    {{ log("Sql Result" ~ results, info=true)}}

    select {{results}} as _is_real from _a_real_table

 {% endif %}

{% endmacro %}