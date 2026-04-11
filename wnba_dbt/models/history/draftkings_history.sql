{{ config(
    materialized='incremental',
    unique_key=['name', '"Game Info"', 'load_date','load_ts']
) }}

select * from {{ ref('draftkings_staged') }}

{% if is_incremental() %}
  where load_date > (select max(load_date) from {{ this }})
{% endif %}
