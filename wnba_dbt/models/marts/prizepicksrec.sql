{% set stat_map = [
    {'stat': 'Points', 'line_col': '"Points"', 'over_col': 'pointsover','hist_over_col': 'pointsover'},
    {'stat': 'Rebounds', 'line_col': '"Rebounds"', 'over_col': 'rebover','hist_over_col': 'rebover'},
    {'stat': 'Assists', 'line_col': '"Assists"', 'over_col': 'astover','hist_over_col': 'astover'},
    {'stat': 'Pts+Rebs', 'line_col': '"Pts+Rebs"', 'over_col': 'prover','hist_over_col': 'points_rebover'},
    {'stat': 'Pts+Asts', 'line_col': '"Pts+Asts"', 'over_col': 'paover','hist_over_col': 'points_ast_over'},
    {'stat': 'Pts+Rebs+Asts', 'line_col': '"Pts+Rebs+Asts"', 'over_col': 'parover','hist_over_col': 'praover'},
    {'stat': 'Rebs+Asts', 'line_col': '"Rebs+Asts"', 'over_col': 'raover','hist_over_col': 'reb_astover'},
    {'stat': 'Fantasy Score', 'line_col': '"Fantasy Score"', 'over_col': 'fantover','hist_over_col': 'fantasyscoreover'},
    {'stat': '3-PT Made', 'line_col': '"3-PT Made"', 'over_col': 'threesover','hist_over_col': 'threesover'}
] %}

with
pp_current as (select * from {{ref('ppovers')}}),
pp_last5 as (select * from {{ref('pp5over')}}),
pp_last10 as (select * from {{ref('pp10over')}})

{% for stat in stat_map %}
  {% if not loop.first %}union all{% endif %}
  select
    curr.player_name,
    curr."attributes.odds_type" as odds_type,
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
    on curr.player_name = last5.player_name
    and curr."attributes.odds_type" = last5."attributes.odds_type"
    and curr.{{ stat.line_col }} = last5.{{ stat.line_col }}
  left join pp_last10 last10
    on curr.player_name = last10.player_name
    and curr."attributes.odds_type" = last10."attributes.odds_type"
    and curr.{{ stat.line_col }} = last10.{{ stat.line_col }}
  where
    curr.{{ stat.line_col }} is not null
    and (
      (curr.{{ stat.over_col }} >= 0.65 and last5.{{ stat.hist_over_col }} >= 0.80 and last10.{{ stat.hist_over_col }} >= 0.80)
      or
      ((curr.{{ stat.over_col }} <= 0.35 and last5.{{ stat.hist_over_col}} <= 0.20 and last10.{{ stat.hist_over_col }} <= 0.20) and curr."attributes.odds_type" like 'standard%')
    )
{% endfor %}