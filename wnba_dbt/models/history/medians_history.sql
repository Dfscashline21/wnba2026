{{ config(
    materialized='incremental',
    unique_key=['player_name', '"TeamAbbrev"', 'load_date','load_ts']
) }}

select load_date,load_ts,player_name,"TeamAbbrev",minutes_projection,medianppm,medianreb,medianast,medianblk,medianstl,medianthrees,medianft,dkpts from {{ ref('medians_staged') }}

{% if is_incremental() %}
  where load_date > (select max(load_date) from {{ this }})
{% endif %}
