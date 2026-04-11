-- models/marts/minutes_projection_staged.sql
-- Staged version of minutes projection model

select * from {{ ref('minutes_projection') }}
