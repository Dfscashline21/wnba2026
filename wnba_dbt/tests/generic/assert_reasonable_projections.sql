{% test assert_reasonable_projections(model, column_name, min_value=0, max_value=100) %}

select 
    player_name,
    "TeamAbbrev",
    {{ column_name }} as projection_value,
    'Player ' || player_name || ' has unrealistic ' || '{{ column_name }}' || ': ' || {{ column_name }} as error_message
from {{ model }}
where {{ column_name }} < {{ min_value }} 
   or {{ column_name }} > {{ max_value }}
   or {{ column_name }} is null

{% endtest %} 