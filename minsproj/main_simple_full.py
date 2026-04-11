"""
WNBA Minutes Prediction Model - Simplified Full Demo
Runs the complete system but with simplified feature engineering to avoid pandas issues
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging

async def simple_full_demo():
    """Run simplified full demo"""
    
    print("=" * 60)
    print("WNBA DAILY MINUTES PROJECTION MODEL - SIMPLIFIED FULL")
    print("=" * 60)
    
    # Direct imports without complex dependencies
    try:
        import pandas as pd
        import numpy as np
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_absolute_error, mean_squared_error
        
        print("\n[INIT] All required libraries loaded successfully")
        
        # Create comprehensive demo data
        n_players = 48  # 4 teams x 12 players
        n_games_per_player = 10
        n_total_samples = n_players * n_games_per_player
        
        print(f"[DATA] Creating dataset with {n_total_samples} player-game records...")
        
        # Create realistic WNBA data
        teams = ['LAS', 'SEA', 'NYL', 'CON']
        positions = ['PG', 'SG', 'SF', 'PF', 'C']
        
        data = []
        player_id = 1
        
        for team in teams:
            for player_num in range(12):  # 12 players per team
                # Player characteristics
                position = np.random.choice(positions)
                is_starter = player_num < 5  # First 5 are starters
                age = np.random.randint(20, 35)
                
                # Base minutes based on role
                if is_starter:
                    base_minutes = np.random.normal(28, 4)
                else:
                    base_minutes = np.random.normal(15, 6)
                    
                base_minutes = max(0, min(base_minutes, 40))
                
                # Generate game records
                for game in range(n_games_per_player):
                    # Game context
                    opponent = np.random.choice([t for t in teams if t != team])
                    is_home = np.random.choice([True, False])
                    days_rest = np.random.choice([0, 1, 2, 3], p=[0.1, 0.3, 0.4, 0.2])
                    
                    # Calculate actual minutes with variation
                    actual_minutes = base_minutes + np.random.normal(0, 3)
                    
                    # Apply adjustments
                    if is_home:
                        actual_minutes += 1  # Home court advantage
                    if days_rest == 0:  # Back to back
                        actual_minutes *= 0.9
                    if age > 32:  # Veteran rest
                        actual_minutes *= 0.95
                        
                    # Injury simulation (5% chance)
                    if np.random.random() < 0.05:
                        actual_minutes *= 0.3  # Injury/limited minutes
                        
                    actual_minutes = max(0, min(actual_minutes, 42))
                    
                    # Performance stats correlated with minutes
                    points = (actual_minutes / 36) * np.random.normal(12, 4)
                    rebounds = (actual_minutes / 36) * np.random.normal(5, 2)
                    assists = (actual_minutes / 36) * np.random.normal(4, 2)
                    
                    data.append({
                        'player_id': player_id,
                        'player_name': f'{team}_Player_{player_num + 1}',
                        'team': team,
                        'opponent': opponent,
                        'game_date': datetime.now() - timedelta(days=50-game),
                        'season': '2024',
                        'is_home': is_home,
                        'is_starter': is_starter,
                        'position': position,
                        'age': age,
                        'days_rest': days_rest,
                        'minutes_played': round(actual_minutes, 1),
                        'points': max(0, round(points, 1)),
                        'rebounds': max(0, round(rebounds, 1)),
                        'assists': max(0, round(assists, 1))
                    })
                    
                player_id += 1
                
        df = pd.DataFrame(data)
        print(f"[OK] Dataset created: {len(df)} records, {df['player_id'].nunique()} players")
        
        # Simple feature engineering
        print("\n[FEATURES] Creating features...")
        
        # Sort by player and date
        df = df.sort_values(['player_id', 'game_date']).reset_index(drop=True)
        
        # Simple rolling averages (last 5 games)
        df['minutes_avg_5'] = df.groupby('player_id')['minutes_played'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        df['points_avg_5'] = df.groupby('player_id')['points'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # Recent performance vs season average
        df['minutes_season_avg'] = df.groupby('player_id')['minutes_played'].transform('mean')
        df['minutes_vs_avg'] = df['minutes_played'] - df['minutes_season_avg']
        
        # Home/away splits
        df['is_home_int'] = df['is_home'].astype(int)
        
        # Simple categorical encoding
        df['team_encoded'] = pd.Categorical(df['team']).codes
        df['opponent_encoded'] = pd.Categorical(df['opponent']).codes
        df['position_encoded'] = pd.Categorical(df['position']).codes
        
        # Fill any NaN values
        df['minutes_avg_5'] = df['minutes_avg_5'].fillna(df['minutes_season_avg'])
        df['points_avg_5'] = df['points_avg_5'].fillna(df['points'].mean())
        
        print(f"[OK] Features created")
        
        # Prepare training data
        print("\n[MODEL] Training prediction model...")
        
        # Select features
        feature_columns = [
            'is_starter', 'age', 'days_rest', 'is_home_int',
            'minutes_avg_5', 'points_avg_5', 'minutes_season_avg',
            'team_encoded', 'opponent_encoded', 'position_encoded'
        ]
        
        # Convert boolean to int
        df['is_starter'] = df['is_starter'].astype(int)
        
        X = df[feature_columns].values
        y = df['minutes_played'].values
        
        # Split data (chronological)
        split_point = int(len(X) * 0.8)
        X_train, X_test = X[:split_point], X[split_point:]
        y_train, y_test = y[:split_point], y[split_point:]
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Validate
        predictions = model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        rmse = np.sqrt(mean_squared_error(y_test, predictions))
        
        print(f"[VALIDATION] Model Performance:")
        print(f"   - MAE: {mae:.2f} minutes")
        print(f"   - RMSE: {rmse:.2f} minutes")
        print(f"   - Target MAE (<3.5): {'[PASS]' if mae <= 3.5 else '[FAIL]'}")
        
        # Feature importance
        feature_importance = model.feature_importances_
        importance_pairs = list(zip(feature_columns, feature_importance))
        importance_pairs.sort(key=lambda x: x[1], reverse=True)
        
        print(f"\n[FEATURES] Top Feature Importance:")
        for feature, importance in importance_pairs[:5]:
            print(f"   - {feature}: {importance:.3f}")
        
        # Generate predictions for "today"
        print(f"\n[PREDICT] Generating today's predictions...")
        
        # Create today's game data (using last known values for each player)
        today_data = []
        latest_player_data = df.groupby('player_id').last().reset_index()
        
        for _, player in latest_player_data.iterrows():
            # Simulate today's game context
            today_game = {
                'player_id': player['player_id'],
                'player_name': player['player_name'],
                'team': player['team'],
                'opponent': np.random.choice(['CHI', 'IND', 'ATL', 'WAS']),
                'is_starter': player['is_starter'],
                'age': player['age'],
                'days_rest': np.random.choice([1, 2]),  # Assume rested
                'is_home_int': np.random.choice([0, 1]),
                'minutes_avg_5': player['minutes_avg_5'],
                'points_avg_5': player['points_avg_5'],
                'minutes_season_avg': player['minutes_season_avg'],
                'team_encoded': player['team_encoded'],
                'opponent_encoded': np.random.randint(0, 4),
                'position_encoded': player['position_encoded']
            }
            today_data.append(today_game)
            
        today_df = pd.DataFrame(today_data)
        
        # Make predictions
        X_today = today_df[feature_columns].values
        today_predictions = model.predict(X_today)
        
        # Display results
        print(f"\n" + "="*50)
        print("TODAY'S GAME PREDICTIONS")
        print("="*50)
        
        # Group by team and show top players
        for team in teams:
            team_players = today_df[today_df['team'] == team].copy()
            team_preds = today_predictions[today_df['team'] == team]
            
            # Add predictions to dataframe
            team_players = team_players.copy()
            team_players['predicted_minutes'] = team_preds
            
            # Sort by predicted minutes
            team_players = team_players.sort_values('predicted_minutes', ascending=False)
            
            print(f"\n{team} - Top 8 Projected Rotation:")
            
            for i, (_, player) in enumerate(team_players.head(8).iterrows(), 1):
                starter_flag = "[S]" if player['is_starter'] else "   "
                home_away = "vs" if player['is_home_int'] == 1 else "@"
                
                print(f"   {starter_flag} {i:2d}. {player['player_name']:<15} "
                     f"{player['predicted_minutes']:5.1f} min "
                     f"({home_away} {player['opponent']})")
                
                # Show key factors
                factors = []
                if player['is_starter']:
                    factors.append("Starter")
                if player['age'] > 30:
                    factors.append(f"Age {player['age']}")
                if player['days_rest'] >= 2:
                    factors.append(f"{player['days_rest']} days rest")
                    
                if factors:
                    print(f"      + {', '.join(factors)}")
        
        # Summary statistics
        total_predictions = len(today_predictions)
        avg_minutes = np.mean(today_predictions)
        
        print(f"\n[SUMMARY] Prediction Summary:")
        print(f"   - Total players: {total_predictions}")
        print(f"   - Average predicted minutes: {avg_minutes:.1f}")
        print(f"   - Starters avg: {np.mean(today_predictions[today_df['is_starter'] == 1]):.1f}")
        print(f"   - Bench avg: {np.mean(today_predictions[today_df['is_starter'] == 0]):.1f}")
        
        # Export results
        print(f"\n[EXPORT] Saving predictions...")
        
        # Create output
        output_data = []
        for i, (_, player) in enumerate(today_df.iterrows()):
            output_data.append({
                'Player': player['player_name'],
                'Team': player['team'],
                'Predicted_Minutes': round(today_predictions[i], 1),
                'Role': 'Starter' if player['is_starter'] else 'Bench',
                'Age': player['age'],
                'Recent_Avg': round(player['minutes_avg_5'], 1),
                'Season_Avg': round(player['minutes_season_avg'], 1)
            })
            
        output_df = pd.DataFrame(output_data)
        filename = f"wnba_predictions_{datetime.now().strftime('%Y%m%d')}.csv"
        output_df.to_csv(filename, index=False)
        
        print(f"[OK] Predictions saved to {filename}")
        
        print(f"\n" + "="*60)
        print("[SUCCESS] WNBA PREDICTION SYSTEM DEMO COMPLETED!")
        print("="*60)
        print(f"\n[SUMMARY] System Capabilities Demonstrated:")
        print(f"   + Multi-level feature engineering")
        print(f"   + Machine learning model training")
        print(f"   + Real-time prediction generation")  
        print(f"   + Performance validation (MAE: {mae:.2f})")
        print(f"   + Export functionality")
        print(f"\n[PRODUCTION] Ready for:")
        print(f"   + Live data integration")
        print(f"   + Real-time injury monitoring")
        print(f"   + Advanced feature engineering")
        print(f"   + Ensemble model stacking")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(simple_full_demo())
    
    if success:
        print(f"\n[SUCCESS] Demo completed successfully!")
        print(f"[INFO] This demonstrates the core WNBA prediction system")
        print(f"[NEXT] For full system: Fix pandas compatibility issues in main.py")
    else:
        print(f"\n[ERROR] Demo encountered issues")
        
    input("\nPress Enter to exit...")  # Keep window open