-- models/analyses/minutes_projection_analysis.sql
-- Analysis of today's minutes projections

-- High confidence projections (confidence >= 0.8)
with high_confidence as (
    select 
        player_name,
        team_abbreviation,
        minutes_projection,
        confidence_score,
        base_minutes,
        injury_adjusted_minutes,
        minutes_volatility,
        games_played_last_10
    from {{ ref('minutes_projection') }}
    where confidence_score >= 0.8
      and projection_date = current_date
    order by minutes_projection desc
),

-- Team minutes distribution
team_distribution as (
    select 
        team_abbreviation,
        count(*) as active_players,
        sum(minutes_projection) as total_projected_minutes,
        avg(minutes_projection) as avg_minutes_per_player,
        max(minutes_projection) as max_minutes,
        min(minutes_projection) as min_minutes
    from {{ ref('minutes_projection') }}
    where projection_date = current_date
    group by team_abbreviation
    order by total_projected_minutes desc
),

-- Players with significant minutes increases due to injuries
injury_beneficiaries as (
    select 
        mp.player_name,
        mp.team_abbreviation,
        mp.minutes_projection,
        mp.base_minutes,
        mp.injury_adjusted_minutes,
        (mp.injury_adjusted_minutes - mp.base_minutes) as minutes_increase,
        mp.confidence_score
    from {{ ref('minutes_projection') }} mp
    where mp.projection_date = current_date
      and mp.injured_players_count > 0
      and mp.injury_adjusted_minutes > mp.base_minutes
    order by (mp.injury_adjusted_minutes - mp.base_minutes) desc
)

-- Main analysis output
select 
    'High Confidence Projections' as analysis_type,
    player_name,
    team_abbreviation,
    minutes_projection::text as value,
    confidence_score::text as confidence
from high_confidence

union all

select 
    'Team Minutes Distribution' as analysis_type,
    team_abbreviation as player_name,
    'Total: ' || total_projected_minutes::text as team_abbreviation,
    'Avg: ' || round(avg_minutes_per_player, 1)::text as value,
    active_players::text as confidence
from team_distribution

union all

select 
    'Injury Beneficiaries' as analysis_type,
    player_name,
    team_abbreviation,
    'Increase: ' || round(minutes_increase, 1)::text as value,
    confidence_score::text as confidence
from injury_beneficiaries

order by analysis_type, value desc;
