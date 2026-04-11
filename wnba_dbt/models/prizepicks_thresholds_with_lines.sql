{{ config(materialized='table') }}

{% set props = [
    {'stat_type': 'points', 'line_col': '"Points"', 'over_col': 'pointsover'},
    {'stat_type': 'pa', 'line_col': '"Pts+Asts"', 'over_col': 'paover'},
    {'stat_type': 'par', 'line_col': '"Pts+Rebs+Asts"', 'over_col': 'parover'},
    {'stat_type': 'rebounds', 'line_col': '"Rebounds"', 'over_col': 'rebover'},
    {'stat_type': 'assists', 'line_col': '"Assists"', 'over_col': 'astover'},
    {'stat_type': 'ra', 'line_col': '"Rebs+Asts"', 'over_col': 'raover'},
    {'stat_type': 'fantasy_points', 'line_col': '"Fantasy Score"', 'over_col': 'fantover'},
    {'stat_type': 'threes_made', 'line_col': '"3-PT Made"', 'over_col': 'threesover'},
    {'stat_type': 'pts_rebs', 'line_col': '"Pts+Rebs"', 'over_col': 'prover'}
] %}

{{ unpivot_prizepicks_props(ref('ppovers_history'), props) }}