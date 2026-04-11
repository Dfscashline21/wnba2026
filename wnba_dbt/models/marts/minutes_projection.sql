-- models/marts/minutes_projection.sql
-- Minutes projection model using clean_logs and injuries data
-- Core Requirements: Exact 200 minutes per team, 70%/30% weighted average, sanity checks

with base_players as (
    -- Get all active players from clean_logs
    select distinct 
        player_name,
        team_abbreviation,
        player_id
    from {{ ref('clean_logs') }}
    where game_date::date >= current_date - interval '90 days'  -- Recent games only
),

recent_performance as (
    -- Calculate 10-game weighted average minutes (70% recent 5 games, 30% games 6-10)
    select 
        gl.player_name,
        gl.team_abbreviation,
        gl.player_id,
        -- Weighted average: 70% recent 5 games, 30% games 6-10
        (avg(case when gl.game_number <= 5 then gl.min end) * 0.7) + 
        (avg(case when gl.game_number > 5 and gl.game_number <= 10 then gl.min end) * 0.3) as weighted_avg_minutes,
        -- Recent 5 games average for sanity checks
        avg(case when gl.game_number <= 5 then gl.min end) as recent_5_avg,
        -- Games 6-10 average for sanity checks
        avg(case when gl.game_number > 5 and gl.game_number <= 10 then gl.min end) as older_5_avg,
        -- Games played in last 10
        count(*) as games_played_last_10,
        -- Minutes consistency (standard deviation)
        stddev(gl.min) as minutes_stddev
    from (
        select 
            *,
            row_number() over (partition by player_name order by game_date::date desc) as game_number
        from {{ ref('clean_logs') }}
        where game_date::date >= current_date - interval '90 days'
    ) gl
    where gl.game_number <= 10
    group by gl.player_name, gl.team_abbreviation, gl.player_id
),

season_averages as (
    -- Calculate season-long averages as fallback
    select 
        gl.player_name,
        gl.team_abbreviation,
        avg(gl.min) as season_avg_minutes,
        count(*) as season_games_played
    from {{ ref('clean_logs') }} gl
    where gl.season_year::integer = extract(year from current_date)
    group by gl.player_name, gl.team_abbreviation
),

injury_adjustments as (
    -- Get injured players and calculate available minutes
    select 
        i.team,
        count(*) as injured_players_count,
        -- Estimate total minutes lost due to injuries
        count(*) * 20 as estimated_minutes_lost
    from {{ source('wnba', 'injuries') }} i
    group by i.team
),

base_projection as (
    select 
        bp.player_name,
        bp.team_abbreviation,
        bp.player_id,
        
        -- Use weighted 10-game average as base, fallback to season average
        coalesce(rp.weighted_avg_minutes, sa.season_avg_minutes, 20) as base_minutes_projection,
        
        -- Apply injury adjustments to redistribute lost minutes
        case 
            when ia.injured_players_count > 0 then
                -- Increase minutes for remaining players when teammates are injured
                coalesce(rp.weighted_avg_minutes, sa.season_avg_minutes, 20) * 1.15  -- 15% boost
            else 
                coalesce(rp.weighted_avg_minutes, sa.season_avg_minutes, 20)
        end as injury_adjusted_minutes,
        
        -- Data for sanity checks
        rp.recent_5_avg,
        rp.older_5_avg,
        rp.weighted_avg_minutes,
        
        -- Confidence score based on data quality
        case
            when rp.games_played_last_10 >= 8 and rp.minutes_stddev < 5 then 0.9
            when rp.games_played_last_10 >= 6 and rp.minutes_stddev < 8 then 0.8
            when rp.games_played_last_10 >= 4 and rp.minutes_stddev < 10 then 0.7
            when sa.season_games_played >= 10 then 0.6
            else 0.5
        end as projection_confidence,
        
        -- Data quality indicators
        rp.games_played_last_10,
        rp.minutes_stddev,
        sa.season_games_played,
        ia.injured_players_count,
        
        -- Metadata
        current_timestamp as projection_timestamp,
        current_date as projection_date
        
    from base_players bp
    left join recent_performance rp on bp.player_name = rp.player_name and bp.team_abbreviation = rp.team_abbreviation
    left join season_averages sa on bp.player_name = sa.player_name and bp.team_abbreviation = sa.team_abbreviation
    left join injury_adjustments ia on bp.team_abbreviation = ia.team
),

sanity_check_adjusted as (
    -- Apply sanity check rules to prevent unreasonable drops
    select 
        *,
        case
            -- Sanity Check Rule: Players averaging 30+ minutes should not be projected below 24 minutes
            when recent_5_avg >= 30 and injury_adjusted_minutes < 24 then 24
            -- Role Consistency: Starters averaging 32+ minutes should typically project 28+ minutes
            when recent_5_avg >= 32 and injury_adjusted_minutes < 28 then 28
            -- Bench players with established roles (15+ minutes average) should not drop below 8 minutes
            when recent_5_avg >= 15 and injury_adjusted_minutes < 8 then 8
            else injury_adjusted_minutes
        end as sanity_check_adjusted_minutes
    from base_projection
),

team_totals as (
    -- Calculate team totals for exact 200-minute scaling
    select 
        team_abbreviation,
        sum(sanity_check_adjusted_minutes) as total_team_minutes
    from sanity_check_adjusted
    group by team_abbreviation
),

final_projection as (
    select 
        sca.player_name,
        sca.team_abbreviation,
        sca.player_id,
        sca.base_minutes_projection,
        sca.injury_adjusted_minutes,
        sca.sanity_check_adjusted_minutes,
        
        -- CRITICAL: Scale to exactly 200 minutes per team, cap at 40 minutes per player
        least(
            sca.sanity_check_adjusted_minutes * (200.0 / tt.total_team_minutes),
            40  -- Maximum 40 minutes per player
        ) as final_minutes_projection,
        
        sca.projection_confidence,
        sca.games_played_last_10,
        sca.minutes_stddev,
        sca.season_games_played,
        sca.injured_players_count,
        sca.projection_timestamp,
        sca.projection_date,
        
        -- Team totals for transparency
        tt.total_team_minutes as team_total_before_scaling,
        200.0 as team_total_after_scaling  -- All teams will have exactly 200 minutes
        
    from sanity_check_adjusted sca
    left join team_totals tt on sca.team_abbreviation = tt.team_abbreviation
)

-- Final output with unique player-team combinations
select 
    player_name,
    team_abbreviation,
    player_id,
    round(final_minutes_projection::numeric, 1) as minutes_projection,
    round(projection_confidence::numeric, 2) as confidence_score,
    round(base_minutes_projection::numeric, 1) as base_minutes,
    round(injury_adjusted_minutes::numeric, 1) as injury_adjusted_minutes,
    round(sanity_check_adjusted_minutes::numeric, 1) as sanity_check_adjusted_minutes,
    games_played_last_10,
    round(minutes_stddev::numeric, 1) as minutes_volatility,
    season_games_played,
    injured_players_count,
    round(team_total_before_scaling::numeric, 1) as team_total_before_scaling,
    round(team_total_after_scaling::numeric, 1) as team_total_after_scaling,
    projection_timestamp,
    projection_date
from (
    -- Ensure unique records per player-team combination
    select 
        *,
        row_number() over (partition by player_name, team_abbreviation order by projection_confidence desc, final_minutes_projection desc) as rn
    from final_projection
) ranked
where ranked.rn = 1  -- Keep only the best record per player-team
  and final_minutes_projection >= 0  -- Allow 0 minutes for DNP scenarios
order by team_abbreviation, final_minutes_projection desc
