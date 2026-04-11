"""
WNBA Feature Engineering Module
Creates features for minutes prediction model including temporal, injury, matchup, and dynamic context features
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder
import warnings
warnings.filterwarnings('ignore')

class WNBAFeatureEngineer:
    """Feature engineering class for WNBA minutes prediction"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.scalers = {}
        self.encoders = {}
        self.feature_columns = []
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('WNBA_FeatureEngineer')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def create_all_features(self, df: pd.DataFrame, real_time_data: Dict = None) -> pd.DataFrame:
        """
        Create all features for the minutes prediction model
        
        Args:
            df: Historical game data DataFrame
            real_time_data: Current real-time data dictionary
            
        Returns:
            DataFrame with all engineered features
        """
        self.logger.info("Starting comprehensive feature engineering")
        
        # Ensure data is sorted by player and date
        df = df.sort_values(['player_id', 'game_date']).reset_index(drop=True)
        
        # Create base features
        df = self._create_temporal_features(df)
        df = self._create_performance_features(df)
        df = self._create_matchup_features(df)
        df = self._create_context_features(df)
        df = self._create_injury_features(df, real_time_data)
        df = self._create_coaching_features(df)
        df = self._create_dynamic_features(df)
        
        # Create interaction features
        df = self._create_interaction_features(df)
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        self.feature_columns = [col for col in df.columns if col not in ['player_id', 'game_id', 'game_date', 'minutes_played']]
        
        self.logger.info(f"Feature engineering complete. Created {len(self.feature_columns)} features")
        
        return df
        
    def _create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create temporal and recency-weighted features"""
        self.logger.info("Creating temporal features")
        
        # Calculate days since last game
        df['days_since_last_game'] = df.groupby('player_id')['game_date'].diff().dt.days
        df['days_since_last_game'] = df['days_since_last_game'].fillna(2)  # Default 2 days
        
        # Rest categories
        df['rest_category'] = pd.cut(df['days_since_last_game'], 
                                   bins=[-1, 0, 1, 2, float('inf')], 
                                   labels=['back_to_back', 'one_day', 'two_days', 'extended'])
        
        # Season progress (0 to 1)
        df['season_progress'] = df.groupby(['player_id', 'season'])['game_date'].rank(pct=True)
        
        # Month and day of week effects
        df['month'] = df['game_date'].dt.month
        df['day_of_week'] = df['game_date'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        # Rolling averages with exponential decay (last 15 games)
        rolling_windows = [3, 5, 10, 15]
        
        for window in rolling_windows:
            # Minutes rolling averages
            df[f'minutes_avg_{window}'] = df.groupby('player_id')['minutes_played'].transform(
                lambda x: x.shift(1).rolling(window, min_periods=1).mean()
            )
            
            # Performance rolling averages
            for stat in ['points', 'rebounds', 'assists']:
                if stat in df.columns:
                    df[f'{stat}_avg_{window}'] = df.groupby('player_id')[stat].transform(
                        lambda x: x.shift(1).rolling(window, min_periods=1).mean()
                    )
                    
            # Exponentially weighted averages
            df[f'minutes_ewm_{window}'] = df.groupby('player_id')['minutes_played'].transform(
                lambda x: x.shift(1).ewm(span=window).mean()
            )
            
        # Momentum and trend features
        df['minutes_trend_5'] = df.groupby('player_id')['minutes_played'].transform(
            lambda x: self._calculate_trend(x, 5)
        )
        
        df['minutes_volatility_10'] = df.groupby('player_id')['minutes_played'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=3).std()
        )
        
        # Performance streaks - Fix DataFrame assignment issue
        streak_results = []
        for player_id, group in df.groupby('player_id'):
            streak = self._calculate_streak(group, 'minutes_played', 'minutes_avg_10', above=True)
            streak_results.append(streak)
        df['minutes_above_avg_streak'] = pd.concat(streak_results)
        
        return df
        
    def _calculate_trend(self, series: pd.Series, window: int) -> pd.Series:
        """Calculate trend slope over window"""
        def trend_slope(x):
            if len(x) < 3:
                return 0
            x_vals = np.arange(len(x))
            try:
                slope = np.polyfit(x_vals, x, 1)[0]
                return slope
            except:
                return 0
                
        return series.shift(1).rolling(window, min_periods=3).apply(trend_slope)
        
    def _calculate_streak(self, group_df: pd.DataFrame, target_col: str, 
                         comparison_col: str, above: bool = True) -> pd.Series:
        """Calculate consecutive streak of performance above/below average"""
        if comparison_col not in group_df.columns:
            return pd.Series(0, index=group_df.index)
            
        # Use a default comparison if the comparison column doesn't have valid values
        if group_df[comparison_col].isna().all():
            return pd.Series(0, index=group_df.index)
            
        comparison_values = group_df[comparison_col].fillna(group_df[target_col].mean())
            
        if above:
            condition = group_df[target_col] > comparison_values
        else:
            condition = group_df[target_col] <= comparison_values
            
        streaks = condition.astype(int).groupby((condition != condition.shift()).cumsum()).cumcount() + 1
        streaks = streaks * condition  # Zero out negative streaks
        
        return streaks
        
    def _create_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create performance-based features"""
        self.logger.info("Creating performance features")
        
        # Usage rate proxies
        team_totals = df.groupby(['team', 'game_date'])[['points', 'rebounds', 'assists']].sum().reset_index()
        team_totals.columns = ['team', 'game_date', 'team_points', 'team_rebounds', 'team_assists']
        
        df = df.merge(team_totals, on=['team', 'game_date'], how='left')
        
        # Usage percentages
        df['points_usage'] = np.where(df['team_points'] > 0, df['points'] / df['team_points'], 0)
        df['rebounds_usage'] = np.where(df['team_rebounds'] > 0, df['rebounds'] / df['team_rebounds'], 0)
        df['assists_usage'] = np.where(df['team_assists'] > 0, df['assists'] / df['team_assists'], 0)
        
        # Efficiency metrics
        df['points_per_minute'] = np.where(df['minutes_played'] > 0, 
                                         df['points'] / df['minutes_played'], 0)
        
        # Performance relative to season average
        season_avgs = df.groupby(['player_id', 'season'])[['points', 'rebounds', 'assists', 'minutes_played']].mean()
        season_avgs.columns = [f'{col}_season_avg' for col in season_avgs.columns]
        
        df = df.merge(season_avgs, left_on=['player_id', 'season'], right_index=True, how='left')
        
        for stat in ['points', 'rebounds', 'assists']:
            df[f'{stat}_vs_season_avg'] = df[stat] - df[f'{stat}_season_avg']
            df[f'{stat}_pct_of_season'] = np.where(df[f'{stat}_season_avg'] > 0,
                                                 df[stat] / df[f'{stat}_season_avg'], 1)
        
        # Hot/cold streaks - Fix DataFrame assignment issues
        for stat in ['points', 'rebounds', 'assists']:
            if stat in df.columns:
                # Hot streak
                hot_results = []
                for player_id, group in df.groupby('player_id'):
                    hot_streak = self._calculate_hot_cold_streak(group, stat, hot=True)
                    hot_results.append(hot_streak)
                df[f'{stat}_hot_streak'] = pd.concat(hot_results)
                
                # Cold streak
                cold_results = []
                for player_id, group in df.groupby('player_id'):
                    cold_streak = self._calculate_hot_cold_streak(group, stat, hot=False)
                    cold_results.append(cold_streak)
                df[f'{stat}_cold_streak'] = pd.concat(cold_results)
            
        return df
        
    def _calculate_hot_cold_streak(self, group_df: pd.DataFrame, stat: str, hot: bool = True) -> pd.Series:
        """Calculate hot or cold performance streaks"""
        season_avg_col = f'{stat}_season_avg'
        if season_avg_col not in group_df.columns:
            return pd.Series(0, index=group_df.index)
            
        # Define hot as significantly above average, cold as significantly below
        threshold_multiplier = 1.25 if hot else 0.75
        
        if hot:
            condition = group_df[stat] >= (group_df[season_avg_col] * threshold_multiplier)
        else:
            condition = group_df[stat] <= (group_df[season_avg_col] * threshold_multiplier)
            
        streaks = condition.astype(int).groupby((condition != condition.shift()).cumsum()).cumcount() + 1
        streaks = streaks * condition
        
        return streaks
        
    def _create_matchup_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create opponent and matchup-specific features"""
        self.logger.info("Creating matchup features")
        
        # Opponent strength metrics
        opponent_stats = df.groupby(['opponent', 'season'])[['points', 'rebounds', 'assists']].mean()
        opponent_stats.columns = [f'opp_{col}_allowed' for col in opponent_stats.columns]
        
        df = df.merge(opponent_stats, left_on=['opponent', 'season'], right_index=True, how='left')
        
        # Historical performance vs opponent
        vs_opponent = df.groupby(['player_id', 'opponent'])[['minutes_played', 'points', 'rebounds', 'assists']].agg(['mean', 'count'])
        vs_opponent.columns = [f'{col[0]}_vs_opp_{col[1]}' for col in vs_opponent.columns]
        
        df = df.merge(vs_opponent, left_on=['player_id', 'opponent'], right_index=True, how='left')
        
        # Fill missing opponent data with overall averages
        for col in vs_opponent.columns:
            if col.endswith('_count'):
                df[col] = df[col].fillna(0)
            else:
                overall_col = col.replace('_vs_opp_mean', '_season_avg')
                if overall_col in df.columns:
                    df[col] = df[col].fillna(df[overall_col])
                else:
                    df[col] = df[col].fillna(df[col].mean())
                    
        # Home vs away performance
        home_away_stats = df.groupby(['player_id', 'is_home'])[['minutes_played', 'points']].mean().unstack(fill_value=0)
        home_away_stats.columns = [f'{col[0]}_{col[1]}' for col in home_away_stats.columns]
        home_away_stats.columns = [col.replace('True', 'home').replace('False', 'away') for col in home_away_stats.columns]
        
        df = df.merge(home_away_stats, left_on='player_id', right_index=True, how='left')
        
        # Pace and style matchups (estimated)
        df['estimated_pace'] = 80 + np.random.normal(0, 5, len(df))  # Placeholder
        df['pace_advantage'] = df['estimated_pace'] - 82  # Relative to league average
        
        return df
        
    def _create_context_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create game context features"""
        self.logger.info("Creating context features")
        
        # Back-to-back games
        df['is_back_to_back'] = (df['days_since_last_game'] == 0).astype(int)
        df['second_of_back_to_back'] = df.groupby('player_id')['is_back_to_back'].shift(1).fillna(0)
        
        # Extended rest (3+ days)
        df['extended_rest'] = (df['days_since_last_game'] >= 3).astype(int)
        
        # Games in last N days - fix DataFrame assignment issue
        for days in [7, 14, 21]:
            # Use manual iteration to avoid DataFrame assignment issues
            results = []
            for player_id, group in df.groupby('player_id'):
                series_result = self._count_games_in_last_n_days(group, days)
                results.append(series_result)
            df[f'games_last_{days}_days'] = pd.concat(results)
            
        # Team schedule density  
        team_results = []
        for (team, season), group in df.groupby(['team', 'season']):
            series_result = self._count_games_in_last_n_days(group, 7)
            team_results.append(series_result)
        df['team_games_last_7'] = pd.concat(team_results)
        
        # Rivalry games (placeholder - would need actual rivalry definitions)
        rivalry_pairs = [('LAS', 'SEA'), ('NYL', 'CON'), ('CHI', 'IND'), ('ATL', 'WAS')]
        df['is_rivalry'] = df.apply(lambda row: (row['team'], row['opponent']) in rivalry_pairs or 
                                              (row['opponent'], row['team']) in rivalry_pairs, axis=1)
        
        # Playoff implications (simplified - based on late season)
        df['late_season'] = (df['season_progress'] > 0.75).astype(int)
        df['playoff_implications'] = df['late_season']  # Simplified
        
        # Win/loss streaks (would need actual game results)
        # Placeholder implementation
        df['team_win_streak'] = np.random.randint(0, 5, len(df))  # Placeholder
        df['team_loss_streak'] = np.where(df['team_win_streak'] == 0, 
                                        np.random.randint(0, 4, len(df)), 0)
        
        return df
    
    def _count_games_in_last_n_days(self, group_df: pd.DataFrame, days: int) -> pd.Series:
        """Count games in last N days using date difference instead of rolling window"""
        result = pd.Series(index=group_df.index, dtype=int)
        
        for idx, row in group_df.iterrows():
            current_date = row['game_date']
            cutoff_date = current_date - pd.Timedelta(days=days)
            
            # Count games in the last N days (excluding current game)
            mask = (group_df['game_date'] >= cutoff_date) & (group_df['game_date'] < current_date)
            result[idx] = mask.sum()
            
        return result
        
    def _create_injury_features(self, df: pd.DataFrame, real_time_data: Dict = None) -> pd.DataFrame:
        """Create injury-related features"""
        self.logger.info("Creating injury features")
        
        # Injury history features (placeholder - would need injury database)
        df['games_since_injury'] = np.random.randint(0, 50, len(df))  # Placeholder
        df['injury_prone_flag'] = (df['games_since_injury'] < 10).astype(int)
        
        # Age-related load management
        # Would need actual player ages
        df['estimated_age'] = 25 + np.random.randint(-3, 8, len(df))  # Placeholder
        df['veteran_flag'] = (df['estimated_age'] >= 30).astype(int)
        df['load_management_candidate'] = ((df['veteran_flag'] == 1) | 
                                         (df['injury_prone_flag'] == 1)).astype(int)
        
        # Real-time injury status integration
        if real_time_data and 'injury_reports' in real_time_data:
            df = self._integrate_real_time_injuries(df, real_time_data['injury_reports'])
            
        # Minutes restrictions after return from injury
        df['minutes_restricted'] = np.where(df['games_since_injury'] <= 3, 1, 0)
        df['expected_minutes_restriction'] = np.where(df['minutes_restricted'] == 1,
                                                    0.8, 1.0)  # 20% reduction
        
        return df
        
    def _integrate_real_time_injuries(self, df: pd.DataFrame, injury_data: Dict) -> pd.DataFrame:
        """Integrate real-time injury reports"""
        # Create injury status mapping
        injury_status_map = {}
        
        for source, injuries in injury_data.items():
            for injury in injuries:
                player_name = injury.get('player', '').lower()
                status = injury.get('status', 'unknown').lower()
                
                # Map status to numeric values
                status_value = {
                    'out': 0,
                    'doubtful': 0.2,
                    'questionable': 0.7,
                    'probable': 0.9,
                    'available': 1.0,
                    'unknown': 0.8
                }.get(status, 0.8)
                
                injury_status_map[player_name] = status_value
                
        # Apply injury status to dataframe
        df['injury_availability'] = df['player_name'].str.lower().map(injury_status_map).fillna(1.0)
        
        return df
        
    def _create_coaching_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create coaching tendency features"""
        self.logger.info("Creating coaching features")
        
        # Coach rotation depth (placeholder - would need coaching data)
        df['coach_rotation_depth'] = 8.5 + np.random.normal(0, 1, len(df))  # Average players used
        df['deep_rotation_coach'] = (df['coach_rotation_depth'] > 9).astype(int)
        
        # Coaching tenure effects
        df['coach_tenure'] = 2.5 + np.random.exponential(1.5, len(df))  # Years
        df['new_coach'] = (df['coach_tenure'] < 1).astype(int)
        
        # Blowout tendencies
        df['coach_blowout_tendency'] = np.random.uniform(0.5, 1.0, len(df))  # How much they rest starters
        
        return df
        
    def _create_dynamic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dynamic context features"""
        self.logger.info("Creating dynamic context features")
        
        # Contract year motivation (placeholder)
        df['contract_year'] = np.random.choice([0, 1], len(df), p=[0.8, 0.2])
        
        # Milestone chasing
        df['points_to_milestone'] = np.where(df['points_season_avg'] * df['season_progress'] > 900,
                                           1000 - (df['points_season_avg'] * df['season_progress']), 
                                           float('inf'))
        df['chasing_milestone'] = (df['points_to_milestone'] < 50).astype(int)
        
        # Team chemistry metrics (placeholder)
        df['team_chemistry'] = 0.7 + np.random.uniform(-0.2, 0.3, len(df))
        
        # Expected game competitiveness
        df['expected_close_game'] = np.random.choice([0, 1], len(df), p=[0.3, 0.7])  # Most games competitive
        
        # Load management schedule optimization
        df['optimal_rest_game'] = ((df['is_back_to_back'] == 1) & 
                                 (df['veteran_flag'] == 1) & 
                                 (df['playoff_implications'] == 0)).astype(int)
        
        return df
        
    def _create_interaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create interaction features between key variables"""
        self.logger.info("Creating interaction features")
        
        # Age and rest interactions
        if 'estimated_age' in df.columns and 'days_since_last_game' in df.columns:
            df['age_rest_interaction'] = df['estimated_age'] * df['days_since_last_game']
            
        # Performance and usage interactions
        if 'points_usage' in df.columns and 'minutes_avg_10' in df.columns:
            df['usage_minutes_interaction'] = df['points_usage'] * df['minutes_avg_10']
            
        # Injury and back-to-back interactions
        if 'injury_availability' in df.columns:
            df['injury_b2b_interaction'] = df['injury_availability'] * (1 - df['is_back_to_back'])
            
        # Team strength and opponent interactions
        if 'team_win_streak' in df.columns and 'opp_points_allowed' in df.columns:
            df['team_strength_opp_interaction'] = df['team_win_streak'] * df['opp_points_allowed']
            
        # Hot streak and home court interactions
        if 'points_hot_streak' in df.columns:
            df['hot_streak_home_interaction'] = df['points_hot_streak'] * df['is_home']
            
        return df
        
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in features"""
        self.logger.info("Handling missing values")
        
        # Fill numeric features with appropriate defaults
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in ['minutes_played', 'player_id', 'game_id']:
                continue
                
            if 'avg' in col or 'usage' in col or 'pct' in col:
                df[col] = df[col].fillna(df[col].median())
            elif 'streak' in col or 'days' in col:
                df[col] = df[col].fillna(0)
            elif 'flag' in col or col.startswith('is_') or col.startswith('has_'):
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna(df[col].mean())
                
        # Handle categorical features - fix categorical column issue
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            if col not in ['player_id', 'game_id', 'player_name', 'team', 'opponent']:
                if df[col].dtype.name == 'category':
                    # Add 'unknown' to categories first
                    if 'unknown' not in df[col].cat.categories:
                        df[col] = df[col].cat.add_categories(['unknown'])
                    df[col] = df[col].fillna('unknown')
                else:
                    df[col] = df[col].fillna('unknown')
        
        # Handle infinite values and very large numbers
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            # Replace infinite values with NaN
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            # Replace very large values (above 99th percentile * 10) with 99th percentile
            if df[col].notna().sum() > 0:  # Only if there are non-NaN values
                q99 = df[col].quantile(0.99)
                if not np.isnan(q99) and q99 != 0:
                    max_allowed = abs(q99) * 10
                    df[col] = df[col].clip(-max_allowed, max_allowed)
            
            # Fill remaining NaN values with 0 for numeric columns
            df[col] = df[col].fillna(0)
                
        return df
        
    def prepare_features_for_modeling(self, df: pd.DataFrame, fit_encoders: bool = True) -> np.ndarray:
        """
        Prepare features for machine learning models
        
        Args:
            df: DataFrame with engineered features
            fit_encoders: Whether to fit encoders (True for training, False for prediction)
            
        Returns:
            Numpy array of processed features
        """
        self.logger.info("Preparing features for modeling")
        
        # Select feature columns
        feature_df = df[self.feature_columns].copy()
        
        # Encode categorical variables
        categorical_cols = feature_df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_cols:
            if fit_encoders:
                if col not in self.encoders:
                    self.encoders[col] = LabelEncoder()
                feature_df[col] = self.encoders[col].fit_transform(feature_df[col].astype(str))
            else:
                if col in self.encoders:
                    # Handle unseen categories
                    unique_vals = feature_df[col].unique()
                    known_vals = self.encoders[col].classes_
                    
                    # Map unseen values to most common category
                    feature_df[col] = feature_df[col].astype(str)
                    unseen_mask = ~feature_df[col].isin(known_vals)
                    if unseen_mask.any():
                        most_common = known_vals[0]  # Use first class as default
                        feature_df.loc[unseen_mask, col] = most_common
                        
                    feature_df[col] = self.encoders[col].transform(feature_df[col])
                else:
                    # If encoder doesn't exist, create dummy encoding
                    feature_df[col] = 0
                    
        # Scale numeric features
        numeric_cols = feature_df.select_dtypes(include=[np.number]).columns
        
        if fit_encoders:
            if 'scaler' not in self.scalers:
                self.scalers['scaler'] = StandardScaler()
            feature_df[numeric_cols] = self.scalers['scaler'].fit_transform(feature_df[numeric_cols])
        else:
            if 'scaler' in self.scalers:
                feature_df[numeric_cols] = self.scalers['scaler'].transform(feature_df[numeric_cols])
            else:
                # If scaler doesn't exist, return unscaled features
                pass
                
        self.logger.info(f"Feature preparation complete. Shape: {feature_df.shape}")
        
        return feature_df.values
        
    def get_feature_names(self) -> List[str]:
        """Get list of feature names"""
        return self.feature_columns.copy()
        
    def get_feature_importance_mapping(self, feature_importance: np.ndarray) -> Dict[str, float]:
        """
        Create mapping of feature names to importance scores
        
        Args:
            feature_importance: Array of feature importance scores
            
        Returns:
            Dictionary mapping feature names to importance scores
        """
        if len(feature_importance) != len(self.feature_columns):
            self.logger.warning("Feature importance array length doesn't match feature columns")
            return {}
            
        importance_dict = dict(zip(self.feature_columns, feature_importance))
        
        # Sort by importance
        sorted_importance = dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))
        
        return sorted_importance