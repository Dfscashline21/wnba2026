{{ config(
    materialized='incremental',
    unique_key=['player', 'team','prop_date', 'load_date', 'load_ts']
) }}

select *
from {{ ref('caesarsovers_staged') }}

{% if is_incremental() %}
    where load_date > (select max(load_date) from {{ this }})
{% endif %}


