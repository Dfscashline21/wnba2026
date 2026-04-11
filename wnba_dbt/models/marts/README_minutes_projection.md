# Minutes Projection Model

## Overview
The `minutes_projection` model provides accurate minutes projections for WNBA players using multiple proven statistical methods and incorporating injury data.

## Key Features

### 1. **Multi-Method Projection Approach**
- **Recent Performance Weighting**: 50% last 3 games, 30% last 5 games, 20% last 10 games
- **Trend Analysis**: Identifies players trending up/down in minutes
- **Season Averages**: Fallback to season-long performance when recent data is limited

### 2. **Injury Adjustments**
- Automatically detects injured players from the `injuries` table
- Redistributes lost minutes among healthy players
- Adjusts projections based on team injury impact

### 3. **Pace Adjustments**
- Incorporates team pace factors from the `pace` table
- Higher pace teams may see increased minutes projections
- Accounts for game tempo variations

### 4. **Confidence Scoring**
- **0.9**: High confidence (8+ recent games, low volatility)
- **0.8**: Good confidence (6+ recent games, moderate volatility)
- **0.7**: Fair confidence (4+ recent games, some volatility)
- **0.6**: Season-based confidence (10+ season games)
- **0.5**: Low confidence (limited data)

## Usage

### Basic Query
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

### With Confidence Filtering
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

## Model Dependencies

- `clean_logs`: Historical game data
- `injuries`: Current injury information
- `pace`: Team pace statistics
- `Games`: Today's game schedule

## Output Columns

| Column | Description | Type |
|--------|-------------|------|
| `player_name` | Player's full name | text |
| `team_abbreviation` | Team abbreviation | text |
| `player_id` | Unique player identifier | integer |
| `minutes_projection` | Final projected minutes | numeric |
| `confidence_score` | Projection confidence (0-1) | numeric |
| `base_minutes_projection` | Base projection before adjustments | numeric |
| `injury_adjusted_minutes` | Minutes after injury adjustments | numeric |
| `pace_adjusted_minutes` | Minutes after pace adjustments | numeric |
| `games_played_last_10` | Recent games count | integer |
| `minutes_volatility` | Minutes standard deviation | numeric |
| `season_games_played` | Season games count | integer |
| `injured_players_count` | Team injured players | integer |
| `pace_factor` | Team pace adjustment | numeric |
| `projection_timestamp` | Calculation timestamp | timestamp |
| `projection_date` | Projection date | date |

## Validation

The model includes several validation tests:
- Minutes projections are between 0-40 (WNBA game length)
- Confidence scores are between 0-1
- Required fields are not null

## Best Practices

1. **Use confidence scores** to filter projections by reliability
2. **Monitor team totals** to ensure they sum to ~200 minutes
3. **Check injury updates** regularly for accurate adjustments
4. **Combine with other models** for comprehensive analysis

## Example Use Cases

- **DFS Lineup Construction**: Use high-confidence minutes projections
- **Player Props Analysis**: Compare projections to betting lines
- **Team Strategy**: Understand minutes distribution and depth
- **Injury Impact Assessment**: See how injuries affect team rotations
