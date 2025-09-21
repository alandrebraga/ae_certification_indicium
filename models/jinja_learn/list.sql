{#
{% set cool_string = 'Cool String!' %}
{% set cool_string_two = 'Jinja is cool!' %}
{% set fav_number = 26 %} 
#}

{% set lst = ['um', 'dois', 'tres'] %}

{%for i in lst %}

    {{i}}

{% endfor %}