{% test assert_game_date_validity(model, date_column) %}

select 
    "Home_abb",
    "Away_abb", 
    {{ date_column }} as game_date,
    'Invalid game date: ' || {{ date_column }} || ' for ' || "Home_abb" || ' vs ' || "Away_abb" as error_message
from {{ model }}
where {{ date_column }} is null
   or {{ date_column }} > current_date + interval '30 days'  -- No games more than 30 days in future
   or {{ date_column }} < '2020-01-01'  -- No games before reasonable start date

{% endtest %} 