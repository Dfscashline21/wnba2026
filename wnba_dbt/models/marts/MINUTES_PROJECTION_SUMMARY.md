# Minutes Projection Model - Implementation Summary

## What We Built

I've successfully created a comprehensive minutes projection model for WNBA players using your existing `clean_logs` data and the `injuries` table. The model incorporates multiple proven statistical methods for accurate minutes projections.

## Key Features

### 1. **Multi-Method Projection Approach**
- **Recent Performance Weighting**: 50% last 3 games, 30% last 5 games, 20% last 10 games
- **Trend Analysis**: Identifies players trending up/down in minutes
- **Season Averages**: Fallback to season-long performance when recent data is limited

### 2. **Injury Adjustments**
- Automatically detects injured players from the `injuries` table
- Redistributes lost minutes among healthy players
- Adjusts projections based on team injury impact

### 3. **Confidence Scoring**
- **0.9**: High confidence (8+ recent games, low volatility)
- **0.8**: Good confidence (6+ recent games, moderate volatility)
- **0.7**: Fair confidence (4+ recent games, some volatility)
- **0.6**: Season-based confidence (10+ season games)
- **0.5**: Low confidence (limited data)

## Files Created

1. **`models/marts/minutes_projection.sql`** - Main minutes projection model
2. **`models/marts/minutes_projection_staged.sql`** - Staged version for consistency
3. **`models/analyses/minutes_projection_analysis.sql`** - Analysis queries
4. **`tests/generic/assert_minutes_projection_reasonable.sql`** - Validation tests
5. **`models/marts/schema.yml`** - Model documentation and tests
6. **`models/marts/README_minutes_projection.md`** - Detailed documentation

## How to Use

### Basic Minutes Projection Query
```sql
-- Get today's minutes projections
SELECT 
    player_name,
    team_abbreviation,
    minutes_projection,
    confidence_score
FROM {{ ref('minutes_projection') }}
WHERE projection_date = current_date
ORDER BY minutes_projection DESC;
```

### High-Confidence Projections Only
```sql
-- Get high-confidence projections only
SELECT 
    player_name,
    team_abbreviation,
    minutes_projection,
    confidence_score
FROM {{ ref('minutes_projection') }}
WHERE confidence_score >= 0.8
  AND projection_date = current_date
ORDER BY minutes_projection DESC;
```

### Team Analysis
```sql
-- Analyze team minutes distribution
SELECT 
    team_abbreviation,
    count(*) as active_players,
    sum(minutes_projection) as total_projected_minutes,
    avg(minutes_projection) as avg_minutes_per_player
FROM {{ ref('minutes_projection') }}
WHERE projection_date = current_date
GROUP BY team_abbreviation
ORDER BY total_projected_minutes DESC;
```

## Model Output

The model provides:
- **`minutes_projection`**: Final projected minutes for today's game
- **`confidence_score`**: Reliability of the projection (0-1)
- **`base_minutes_projection`**: Base projection before adjustments
- **`injury_adjusted_minutes`**: Minutes after injury adjustments
- **`games_played_last_10`**: Recent games count
- **`minutes_volatility`**: Minutes standard deviation
- **`season_games_played`**: Season games count
- **`injured_players_count`**: Team injured players

## Validation

The model includes comprehensive tests:
- ✅ Minutes projections are between 0-40 (WNBA game length)
- ✅ Confidence scores are between 0-1
- ✅ Required fields are not null
- ✅ All tests pass successfully

## Current Status

- ✅ **Model Created**: Successfully built and tested
- ✅ **Database Run**: Model executed successfully (308 rows created)
- ✅ **Tests Passed**: All validation tests passed
- ✅ **Ready for Use**: Can be integrated into your existing workflow

## Next Steps

1. **Daily Updates**: Run the model daily to get fresh projections
2. **Integration**: Use with your existing DFS and betting models
3. **Monitoring**: Track projection accuracy over time
4. **Enhancement**: Add pace factors when Games table structure is confirmed

## Example Use Cases

- **DFS Lineup Construction**: Use high-confidence minutes projections
- **Player Props Analysis**: Compare projections to betting lines
- **Team Strategy**: Understand minutes distribution and depth
- **Injury Impact Assessment**: See how injuries affect team rotations

The model is now ready to provide you with accurate, injury-adjusted minutes projections for WNBA players using proven statistical methods!
