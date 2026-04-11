{% test assert_positive_salary(model, column_name) %}

select 
    name,
    {{ column_name }} as salary,
    'Player ' || name || ' has invalid salary: ' || {{ column_name }} as error_message
from {{ model }}
where {{ column_name }} <= 0 
   or {{ column_name }} is null

{% endtest %} 