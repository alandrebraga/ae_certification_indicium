{%
    set pydict = { 
        2: 1,
        "dois": 2,
        "tres": 3
    }
%}

{{ pydict.get(2) }}

{{ pydict["dois"] }}

{{ pydict["asdasdad"] }}