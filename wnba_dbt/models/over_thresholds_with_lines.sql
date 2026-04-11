{{ config(materialized='table') }}

{% set props = [
  {'stat_type': 'points', 'over_col': 'pointsover', 'line_col': 'points'},
  {'stat_type': 'pts_asts', 'over_col': 'paover', 'line_col': 'pts_asts'},
  {'stat_type': 'pts_rebs_asts', 'over_col': 'parover', 'line_col': 'pts_rebs_asts'},
  {'stat_type': 'rebounds', 'over_col': 'rebover', 'line_col': 'rebounds'},
  {'stat_type': 'assists', 'over_col': 'astover', 'line_col': 'assists'},
  {'stat_type': 'rebs_asts', 'over_col': 'raover', 'line_col': 'rebs_asts'},
  {'stat_type': 'fantasy_points', 'over_col': 'fantover', 'line_col': 'fantasy_points'},
  {'stat_type': 'three_made', 'over_col': 'threesover', 'line_col': 'three_points_made'}
] %}

{{ unpivot_over_props_with_lines(ref('udovers_history'), props) }}