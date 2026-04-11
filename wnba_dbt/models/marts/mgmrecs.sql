{% set stat_map = [
    {'stat': 'Points', 'line_col': '"Points"', 'over_col': 'pointsover','hist_over_col': 'pointsover'},
    {'stat': 'Rebounds', 'line_col': '"Rebounds"', 'over_col': 'rebover','hist_over_col': 'rebover'},
    {'stat': 'Assists', 'line_col': '"Assists"', 'over_col': 'astover','hist_over_col': 'astover'},
    {'stat': '3-Pointers', 'line_col': '"3-Pointers"', 'over_col': 'threesover','hist_over_col': 'threesover'}
] %}

with
pp_current as (select * from {{ref('mgmovers')}} ),
pp_last5 as (select * from {{ref('mgm5over')}} ),
pp_last10 as (select * from {{ref('mgm10over')}} )

{% for stat in stat_map %}
  {% if not loop.first %}union all{% endif %}
  select
    curr.player,
    curr.team,
    '{{ stat.stat }}' as stat,
    curr.{{ stat.line_col }} as line,
    curr.{{ stat.over_col }} as over_value,
    case 
      when curr.{{ stat.over_col }} <= 0.35 then 'Under'
      else 'Over'
    end as bettype,
    last5.{{ stat.hist_over_col }} as over_value5,
    last10.{{ stat.hist_over_col }} as over_value10
  from pp_current curr
  left join pp_last5 last5
    on curr.player = last5.player
    and curr.{{ stat.line_col }} = last5.{{ stat.line_col }}
  left join pp_last10 last10
    on curr.player = last10.player
    and curr.{{ stat.line_col }} = last10.{{ stat.line_col }}
  where
    curr.{{ stat.line_col }} is not null
    and (
      (curr.{{ stat.over_col }} >= 0.65 and last5.{{ stat.hist_over_col }} >= 0.80 and last10.{{ stat.hist_over_col }} >= 0.80)
      or
      ((curr.{{ stat.over_col }} <= 0.35 and last5.{{ stat.hist_over_col}} <= 0.20 and last10.{{ stat.hist_over_col }} <= 0.20) )
    )
{% endfor %}