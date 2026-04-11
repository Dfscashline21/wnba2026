{{ config(
    materialized='incremental',
    unique_key=['player_name',  'load_date','load_ts']
) }}

select * from {{ ref('udovers_staged') }}

{% if is_incremental() %}
  where load_date > (select max(load_date) from {{ this }})
{% endif %}
