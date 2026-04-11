select
    current_date as load_date,
    current_timestamp as load_ts,
    *
from {{ ref('clean_draftkings') }}
