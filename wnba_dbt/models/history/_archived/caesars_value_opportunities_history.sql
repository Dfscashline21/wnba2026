{{ config(
    materialized='incremental',
    unique_key=['player', 'team', 'stat', 'threshold', 'load_date', 'load_ts']
) }}

select *
from {{ ref('caesars_value_opportunities_staged') }}

{% if is_incremental() %}
    where load_date > (select max(load_date) from {{ this }})
{% endif %}


