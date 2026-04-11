# AI Prompt: WNBA Daily Minutes Projection Model

## Objective
Create a real-time machine learning model to predict individual player minutes for each upcoming WNBA game, incorporating live injury reports, recent performance trends, and game-specific factors with continuous updates throughout the season.

## Data Requirements

### Real-Time Game Data
- **Live Injury Reports**: Daily injury status (out, questionable, probable, available)
- **Game Context**: Home/away, back-to-back games, rest days, opponent strength
- **Recent Performance**: Last 5-10 games statistics and minutes played
- **Starting Lineup**: Confirmed starters vs. bench players for each game
- **Game Situation**: Playoff implications, rivalry games, season stage (early/late)

### Historical Player Data (Last 3-5 seasons)
- **Game-by-Game Minutes**: Complete game log with situational context
- **Performance Metrics**: Points, rebounds, assists per game with recency weighting
- **Advanced Stats**: PER, usage rate, +/-, game impact metrics
- **Biographical**: Age, experience, position, physical attributes
- **Contract/Role Status**: Starter designation, veteran status, rookie development

### Team & Coaching Data
- **Rotation Patterns**: Coach-specific tendencies for substitution timing
- **Matchup History**: How coach adjusts rotations vs. specific opponents
- **Blowout Behavior**: Minute distribution in close games vs. blowouts
- **Rest Management**: Load management patterns for key players
- **Depth Chart Changes**: Recent position changes or role adjustments

### Injury & Health Intelligence
- **Injury Database**: Type, severity, typical recovery timelines by injury
- **Load Management**: Games rested, minutes restrictions upon return
- **Performance Decline**: Post-injury performance vs. pre-injury baselines
- **Teammate Impact**: How injuries to others affect remaining players' minutes
- **Precautionary Rest**: Patterns of rest on back-to-backs or vs. weak opponents

### Real-Time Data Feeds
- **Injury Reports**: Official team reports, beat reporter updates, social media
- **Lineup Changes**: Pre-game warmups, coach interviews, unexpected scratches
- **Game Flow**: Live score differential affecting rotation decisions
- **Foul Trouble**: Early foul accumulation impact on playing time
- **Performance Streaks**: Hot/cold shooting affecting coach decisions

## Model Architecture

### Multi-Level Prediction System
1. **Base Minutes Model**: Season-long role and expected minutes
2. **Game Context Adjuster**: Matchup and situation-specific modifications  
3. **Injury Impact Layer**: Real-time health status adjustments
4. **In-Game Updater**: Live game flow modifications during play

### Feature Engineering

#### Temporal Features
- **Recency Weighted Stats**: Exponential decay on last 15 games
- **Rest Impact**: Performance on 0, 1, 2+ days rest
- **Seasonal Trends**: Early season vs. mid-season vs. late season patterns
- **Momentum Indicators**: Win/loss streaks, individual performance streaks

#### Injury-Specific Features
- **Injury Severity Score**: Weighted impact based on injury type and timeline
- **Return-from-Injury Curve**: Expected minute progression after return
- **Injury Cascade Effects**: How one player's injury affects teammates
- **Precautionary Factors**: Age, injury history influencing rest decisions
- **Game Importance vs. Health**: Playoff race urgency overriding precaution

#### Matchup Features
- **Opponent Pace**: Fast-paced games typically increase minutes for key players
- **Positional Matchups**: Favorable/unfavorable individual matchups
- **Coaching Tendencies**: Opponent coach's rotation depth preferences
- **Historical Head-to-Head**: Player performance vs. specific teams
- **Home Court Advantage**: Venue-specific performance patterns

#### Dynamic Context Features
- **Score Differential Prediction**: Expected competitiveness affecting rotation
- **Playoff Seeding Impact**: Games with standings implications
- **Milestone Chasing**: Players near statistical achievements
- **Contract Year Performance**: Added motivation for players in final year
- **Team Chemistry Metrics**: Recent on-court plus/minus combinations

## Real-Time Update System

### Pre-Game Updates (0-3 hours before tip)
1. **Final Injury Report Processing**: Official team announcements
2. **Starting Lineup Confirmation**: Last-minute changes integration
3. **Weather/Travel Factors**: Delayed flights, arena conditions
4. **Social Media Monitoring**: Player availability hints from warm-ups
5. **Betting Line Movements**: Market intelligence on expected player availability

### During-Game Adjustments
1. **Foul Trouble Monitor**: Real-time foul accumulation affecting future minutes
2. **Performance-Based Adjustments**: Hot streaks extending playing time
3. **Score Differential Impact**: Blowout detection for garbage time predictions
4. **Injury During Game**: Live injury assessment and substitution impact
5. **Technical Issues**: Shot clock malfunctions, extended breaks affecting rotation

### Post-Game Learning
1. **Prediction Accuracy Tracking**: Model performance evaluation by game
2. **Coach Pattern Updates**: New tendencies based on recent decisions
3. **Injury Recovery Monitoring**: Actual vs. predicted return timelines
4. **Contextual Factor Weighting**: Adjusting feature importance based on outcomes

## Model Implementation

### Primary Model Types
1. **Gradient Boosting (XGBoost/LightGBM)**: Handle complex feature interactions
2. **LSTM Neural Networks**: Capture sequential game patterns and trends  
3. **Bayesian Networks**: Incorporate uncertainty and injury probability chains
4. **Ensemble Stacking**: Combine multiple approaches for robust predictions

### Training Strategy
- **Rolling Window**: Train on last 2 seasons, validate on recent 20 games
- **Transfer Learning**: Apply patterns from similar players/situations
- **Online Learning**: Continuous model updates with each completed game
- **Situational Models**: Separate models for back-to-backs, home/away, etc.

### Uncertainty Quantification
- **Confidence Intervals**: 80% prediction ranges for each player
- **Scenario Analysis**: Best/worst case minute projections
- **Risk Factors**: Likelihood of significant deviation from prediction
- **Alternative Outcomes**: If key injury occurs during game

## Real-Time Data Pipeline

### Data Sources Integration
- **Official WNBA API**: Live stats, box scores, play-by-play
- **Injury Report Aggregators**: RotoWire, ESPN, team beat reporters
- **Social Media Monitoring**: Twitter/X feeds for breaking news
- **Betting Markets**: Line movements indicating inside information
- **Team Communications**: Official social media, press releases

### Update Frequency
- **12+ Hours Before**: Initial projection with known information
- **4-6 Hours Before**: Updated with practice reports, final injury news
- **1 Hour Before**: Final pre-game update with warmup observations
- **Live During Game**: Real-time adjustments based on game flow
- **Post-Game**: Immediate learning integration for next game

### Data Quality Controls
1. **Source Reliability Scoring**: Weight updates based on source accuracy history
2. **Contradiction Detection**: Flag conflicting reports for manual review
3. **Latency Monitoring**: Ensure updates propagate within acceptable timeframes
4. **Backup Data Sources**: Redundant feeds to prevent single points of failure

## Injury Impact Modeling

### Injury Classification System
- **Severity Levels**: Minor (1-2 games), Moderate (1-2 weeks), Major (1+ months)
- **Body Part Categories**: Lower body, upper body, head/concussion
- **Injury Type Impact**: Acute vs. chronic, contact vs. non-contact
- **Recovery Patterns**: Historical timelines by age, position, injury combination

### Cascade Effect Modeling
1. **Direct Replacements**: Who gets injured player's minutes
2. **Positional Shifts**: How lineups adjust to cover missing player
3. **Usage Rate Changes**: Increased responsibility for remaining players
4. **Rest Impact**: Fewer rest opportunities for key players
5. **Strategic Adjustments**: Coaching changes due to personnel limitations

### Return-from-Injury Protocols
- **Minutes Restrictions**: Graduated return to full playing time
- **Performance Expectations**: Temporary decline in efficiency
- **Re-injury Risk**: Higher probability of re-aggravation in first 10 games
- **Load Management**: Strategic rest to prevent setbacks
- **Psychological Factors**: Confidence and aggressiveness upon return

## Output Format

### Individual Game Predictions
```
Player: Sue Bird
Game: Seattle Storm @ Las Vegas Aces
Date: July 15, 2024

Projected Minutes: 28.5 (±4.2)
Confidence Level: 82%

Key Factors:
+ Back from 1-game rest (+2.1 min)
+ Favorable matchup vs. Aces PG (+1.3 min)
- Team ahead in standings (-0.8 min)
- 2nd game of back-to-back (-1.7 min)

Risk Factors:
- Age-related rest possible (15% chance)
- Minor ankle soreness (monitored)

Alternative Scenarios:
- If blowout victory: 24-26 minutes
- If close game: 32-35 minutes  
- If injury concern: 0-15 minutes
```

### Team Rotation Predictions
- Starting lineup confirmation with confidence levels
- Bench rotation depth (8-9 player vs. 6-7 player night)
- Likely minute distribution across all rostered players
- Impact of potential in-game injuries on remaining players

### Live Game Updates
- Real-time minute adjustments based on game flow
- Probability of reaching various minute thresholds
- Foul trouble impact on future playing time
- Garbage time onset detection and impact

## Model Validation & Performance Metrics

### Accuracy Measures
- **Mean Absolute Error**: Target <3.5 minutes per game prediction
- **Within-Threshold Accuracy**: 75% of predictions within ±4 minutes
- **Directional Accuracy**: Correctly predict increases/decreases 80%+ of time
- **Extreme Event Detection**: Identify 0-minute games (injuries) 90%+ accuracy

### Real-Time Performance
- **Update Latency**: New predictions within 5 minutes of new information
- **Injury Detection Speed**: Flag significant injuries within 15 minutes
- **False Positive Rate**: Minimize unnecessary alerts and updates
- **System Uptime**: 99.5% availability during game hours

### Business Value Metrics
- **Fantasy Sports Accuracy**: Outperform baseline projections by 15%+
- **Betting Market Efficiency**: Identify line value 60%+ of time
- **Fan Engagement**: Provide insights that enhance viewing experience
- **Team Analytics**: Support coaching staff with rotation optimization

## Success Criteria
1. **Accuracy**: MAE < 3.5 minutes per game across all predictions
2. **Injury Impact**: 90%+ accuracy on significant minute changes due to injury
3. **Real-Time Performance**: Updates within 5 minutes of breaking news
4. **Reliability**: System maintains 99%+ uptime during game periods
5. **User Adoption**: Fantasy/betting applications integrate model successfully

## Advanced Extensions
- **Multi-Game Projections**: Weekend back-to-back scenarios
- **Playoff Intensity Modeling**: Increased minutes for star players
- **Coaching Change Impact**: New coach rotation pattern learning
- **Trade Deadline Adjustments**: New player integration modeling
- **International Competition**: Olympic/World Cup impact on availability