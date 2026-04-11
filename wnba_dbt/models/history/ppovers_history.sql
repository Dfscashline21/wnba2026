{{ config(
    materialized='incremental',
    unique_key=['player_name', '"attributes.odds_type"', 'load_date','load_ts']
) }}

select * from {{ ref('ppovers_staged') }}

{% if is_incremental() %}
  where load_date > (select max(load_date) from {{ this }})
{% endif %}