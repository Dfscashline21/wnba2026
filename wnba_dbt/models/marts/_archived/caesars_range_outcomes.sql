{{config(
    materialized='table',
    alias='caesars_range_outcomes'
)}}

with base_sims as (
    {{ player_sims(100, 6800) }}
    union all
    {{ player_sims(200, 2700) }}
    union all 
    {{ player_sims(300, 500) }}
),

-- Generate threshold ranges for each stat type
-- Points: 1-30, Rebounds: 1-15, Assists: 1-12, 3-Pointers: 1-5
points_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
    select 16 union all select 17 union all select 18 union all select 19 union all select 20 union all
    select 21 union all select 22 union all select 23 union all select 24 union all select 25 union all
    select 26 union all select 27 union all select 28 union all select 29 union all select 30
),
rebounds_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15
),
assists_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12
),
threes_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5
),
-- Combined stat thresholds
pra_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
    select 16 union all select 17 union all select 18 union all select 19 union all select 20 union all
    select 21 union all select 22 union all select 23 union all select 24 union all select 25 union all
    select 26 union all select 27 union all select 28 union all select 29 union all select 30 union all
    select 31 union all select 32 union all select 33 union all select 34 union all select 35 union all
    select 36 union all select 37 union all select 38 union all select 39 union all select 40 union all
    select 41 union all select 42 union all select 43 union all select 44 union all select 45 union all
    select 46 union all select 47 union all select 48 union all select 49 union all select 50 union all
    select 51 union all select 52 union all select 53 union all select 54 union all select 55
),
pa_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
    select 16 union all select 17 union all select 18 union all select 19 union all select 20 union all
    select 21 union all select 22 union all select 23 union all select 24 union all select 25 union all
    select 26 union all select 27 union all select 28 union all select 29 union all select 30 union all
    select 31 union all select 32 union all select 33 union all select 34 union all select 35 union all
    select 36 union all select 37 union all select 38 union all select 39 union all select 40
),
pr_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
    select 16 union all select 17 union all select 18 union all select 19 union all select 20 union all
    select 21 union all select 22 union all select 23 union all select 24 union all select 25 union all
    select 26 union all select 27 union all select 28 union all select 29 union all select 30 union all
    select 31 union all select 32 union all select 33 union all select 34 union all select 35 union all
    select 36 union all select 37 union all select 38 union all select 39 union all select 40
),
ra_thresholds as (
    select 1 as threshold union all select 2 union all select 3 union all select 4 union all select 5 union all
    select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
    select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
    select 16 union all select 17 union all select 18 union all select 19 union all select 20
),

-- Calculate probabilities for each threshold and stat type
range_outcomes as (
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'Points' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when sims.ppmsim >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join points_thresholds t
    where sims.ppmsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'Rebounds' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when sims.rebsim >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join rebounds_thresholds t
    where sims.rebsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'Assists' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when sims.apmsim >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join assists_thresholds t
    where sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        '3-Pointers' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when sims.threessim >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join threes_thresholds t
    where sims.threessim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'PRA' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when (sims.ppmsim + sims.rebsim + sims.apmsim) >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join pra_thresholds t
    where sims.ppmsim is not null and sims.rebsim is not null and sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'PA' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when (sims.ppmsim + sims.apmsim) >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join pa_thresholds t
    where sims.ppmsim is not null and sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'PR' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when (sims.ppmsim + sims.rebsim) >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join pr_thresholds t
    where sims.ppmsim is not null and sims.rebsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'RA' as stat,
        t.threshold,
        concat(t.threshold, '+') as threshold_label,
        sum(case when (sims.rebsim + sims.apmsim) >= t.threshold then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    cross join ra_thresholds t
    where sims.rebsim is not null and sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev", t.threshold
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'Double Double' as stat,
        1 as threshold,
        'Double Double' as threshold_label,
        sum(case when 
            (sims.ppmsim >= 10 and sims.rebsim >= 10) or
            (sims.ppmsim >= 10 and sims.apmsim >= 10) or
            (sims.rebsim >= 10 and sims.apmsim >= 10)
        then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    where sims.ppmsim is not null and sims.rebsim is not null and sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev"
    
    union all
    
    select 
        sims.player_name as player,
        sims."TeamAbbrev" as team,
        'Triple Double' as stat,
        1 as threshold,
        'Triple Double' as threshold_label,
        sum(case when 
            sims.ppmsim >= 10 and sims.rebsim >= 10 and sims.apmsim >= 10
        then 1 else 0 end) / 10000.0 as probability
    from base_sims sims
    where sims.ppmsim is not null and sims.rebsim is not null and sims.apmsim is not null
    group by sims.player_name, sims."TeamAbbrev"
)

select 
    player,
    team,
    stat,
    threshold,
    threshold_label,
    round(probability, 4) as probability,
    round(probability * 100, 2) as probability_percent,
    case
        when probability = 0.5 then 0
        when probability = 1.0 then -9999  -- Handle 100% probability
        when probability = 0.0 then 9999   -- Handle 0% probability
        when probability > 0.5 then 
            round(-100 * probability / (1 - probability))
        else 
            round(100 * (1 - probability) / probability)
    end as american_odds,
    round(1 - probability, 4) as under_probability,
    round((1 - probability) * 100, 2) as under_probability_percent,
    case
        when (1 - probability) = 0.5 then 0
        when (1 - probability) = 1.0 then -9999  -- Handle 100% under probability
        when (1 - probability) = 0.0 then 9999   -- Handle 0% under probability
        when (1 - probability) > 0.5 then 
            round(-100 * (1 - probability) / probability)
        else 
            round(100 * probability / (1 - probability))
    end as under_american_odds
from range_outcomes
where probability > 0  -- Only show thresholds that have a non-zero probability
order by 
    player, 
    stat, 
    threshold
