{% for i in range (1,10) %}
  select {{i}} as number {% if not loop.last %} union all {% endif %}
{% endfor %}