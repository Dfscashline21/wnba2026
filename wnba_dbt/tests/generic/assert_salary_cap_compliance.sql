{% test assert_salary_cap_compliance(model, column_name, salary_column=none, cap_limit=50000) %}

-- This test checks if there are any individual players whose salary exceeds the cap
-- (which would make it impossible to build a legal lineup)
select 
    name,
    "TeamAbbrev",
    {{ salary_column if salary_column else column_name }} as salary,
    'Player ' || name || ' salary (' || {{ salary_column if salary_column else column_name }} || ') exceeds reasonable cap limit (' || {{ cap_limit }} || ')' as error_message
from {{ model }}
where {{ salary_column if salary_column else column_name }} > {{ cap_limit }}

{% endtest %} 