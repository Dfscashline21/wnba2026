"""
WNBA Multi-Level Prediction System
Implements the four-tier prediction architecture: Base Minutes, Game Context, Injury Impact, and In-Game Updates
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
import pickle
import json

# ML Models
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error
import xgboost as xgb
import lightgbm as lgb

# Deep Learning
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, Model
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Input, Concatenate
    from tensorflow.keras.optimizers import Adam
    from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

# Bayesian
try:
    import pymc as pm
    import arviz as az
    PYMC_AVAILABLE = True
except ImportError:
    PYMC_AVAILABLE = False

class BaseMinutesModel:
    """Base model for season-long role and expected minutes prediction"""
    
    def __init__(self, model_type: str = 'xgboost'):
        self.model_type = model_type
        self.model = None
        self.feature_importance = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(f'BaseMinutesModel_{self.model_type}')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def train(self, X: np.ndarray, y: np.ndarray, validation_split: float = 0.2) -> Dict:
        """Train the base minutes prediction model"""
        self.logger.info(f"Training base minutes model with {self.model_type}")
        
        # Split data chronologically
        split_idx = int(len(X) * (1 - validation_split))
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        # Initialize model based on type
        if self.model_type == 'xgboost':
            self.model = xgb.XGBRegressor(
                n_estimators=500,
                learning_rate=0.05,
                max_depth=6,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                early_stopping_rounds=50,
                eval_metric='mae'
            )
            
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False
            )
            
        elif self.model_type == 'lightgbm':
            self.model = lgb.LGBMRegressor(
                n_estimators=500,
                learning_rate=0.05,
                max_depth=6,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42
            )
            
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
            )
            
        elif self.model_type == 'gradient_boosting':
            self.model = GradientBoostingRegressor(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=6,
                subsample=0.8,
                random_state=42
            )
            
            self.model.fit(X_train, y_train)
            
        elif self.model_type == 'random_forest':
            self.model = RandomForestRegressor(
                n_estimators=200,
                max_depth=10,
                min_samples_split=10,
                min_samples_leaf=5,
                random_state=42
            )
            
            self.model.fit(X_train, y_train)
            
        # Get feature importance
        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = self.model.feature_importances_
            
        # Validate model
        val_predictions = self.model.predict(X_val)
        val_mae = mean_absolute_error(y_val, val_predictions)
        val_rmse = np.sqrt(mean_squared_error(y_val, val_predictions))
        
        self.logger.info(f"Base model validation - MAE: {val_mae:.3f}, RMSE: {val_rmse:.3f}")
        
        return {
            'validation_mae': val_mae,
            'validation_rmse': val_rmse,
            'feature_importance': self.feature_importance
        }
        
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict base minutes for given features"""
        if self.model is None:
            raise ValueError("Model not trained yet")
        return self.model.predict(X)
        
    def get_uncertainty(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Get prediction uncertainties (confidence intervals)"""
        base_pred = self.predict(X)
        
        if self.model_type in ['random_forest']:
            # Use tree variance for uncertainty estimation
            tree_predictions = np.array([tree.predict(X) for tree in self.model.estimators_])
            std_pred = np.std(tree_predictions, axis=0)
            
            # 80% confidence intervals
            lower = base_pred - 1.28 * std_pred
            upper = base_pred + 1.28 * std_pred
            
        else:
            # Simple heuristic uncertainty based on model performance
            uncertainty = 3.5  # Based on target MAE
            lower = base_pred - uncertainty
            upper = base_pred + uncertainty
            
        return lower, upper

class GameContextAdjuster:
    """Adjusts base minutes predictions based on game-specific context"""
    
    def __init__(self):
        self.context_adjustments = {}
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('GameContextAdjuster')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def train_adjustments(self, df: pd.DataFrame) -> Dict:
        """Learn context-based adjustments from historical data"""
        self.logger.info("Learning context adjustments from historical data")
        
        adjustments = {}
        
        # Back-to-back game adjustments
        b2b_data = df[df['is_back_to_back'] == 1]['minutes_played']
        regular_data = df[df['is_back_to_back'] == 0]['minutes_played']
        
        if len(b2b_data) > 0 and len(regular_data) > 0:
            b2b_adjustment = b2b_data.mean() - regular_data.mean()
            adjustments['back_to_back'] = b2b_adjustment
            
        # Home vs away adjustments
        home_data = df[df['is_home'] == True]['minutes_played']
        away_data = df[df['is_home'] == False]['minutes_played']
        
        if len(home_data) > 0 and len(away_data) > 0:
            home_advantage = home_data.mean() - away_data.mean()
            adjustments['home_advantage'] = home_advantage
            
        # Rest day adjustments
        for rest_days in [0, 1, 2, 3]:
            rest_data = df[df['days_since_last_game'] == rest_days]['minutes_played']
            if len(rest_data) > 10:  # Minimum sample size
                baseline = df['minutes_played'].mean()
                adjustments[f'rest_{rest_days}_days'] = rest_data.mean() - baseline
                
        # Opponent strength adjustments
        strong_opp = df[df['opp_points_allowed'] < df['opp_points_allowed'].quantile(0.33)]['minutes_played']
        weak_opp = df[df['opp_points_allowed'] > df['opp_points_allowed'].quantile(0.67)]['minutes_played']
        
        if len(strong_opp) > 0 and len(weak_opp) > 0:
            adjustments['vs_strong_opponent'] = strong_opp.mean() - weak_opp.mean()
            
        # Playoff implications
        playoff_games = df[df['playoff_implications'] == 1]['minutes_played']
        regular_games = df[df['playoff_implications'] == 0]['minutes_played']
        
        if len(playoff_games) > 0 and len(regular_games) > 0:
            adjustments['playoff_implications'] = playoff_games.mean() - regular_games.mean()
            
        self.context_adjustments = adjustments
        
        self.logger.info(f"Learned {len(adjustments)} context adjustments")
        
        return adjustments
        
    def apply_adjustments(self, base_predictions: np.ndarray, context_features: pd.DataFrame) -> np.ndarray:
        """Apply context adjustments to base predictions"""
        adjusted_predictions = base_predictions.copy()
        
        for i, row in context_features.iterrows():
            adjustment = 0
            
            # Back-to-back adjustment
            if row.get('is_back_to_back', 0) == 1 and 'back_to_back' in self.context_adjustments:
                adjustment += self.context_adjustments['back_to_back']
                
            # Home court adjustment
            if row.get('is_home', False) and 'home_advantage' in self.context_adjustments:
                adjustment += self.context_adjustments['home_advantage']
                
            # Rest adjustment
            rest_days = row.get('days_since_last_game', 1)
            rest_key = f'rest_{int(rest_days)}_days'
            if rest_key in self.context_adjustments:
                adjustment += self.context_adjustments[rest_key]
                
            # Opponent strength adjustment
            opp_strength = row.get('opp_points_allowed', 80)  # League average
            if opp_strength < 75 and 'vs_strong_opponent' in self.context_adjustments:  # Strong defense
                adjustment += self.context_adjustments['vs_strong_opponent']
                
            # Playoff implications
            if row.get('playoff_implications', 0) == 1 and 'playoff_implications' in self.context_adjustments:
                adjustment += self.context_adjustments['playoff_implications']
                
            adjusted_predictions[i] += adjustment
            
        return np.maximum(adjusted_predictions, 0)  # Ensure non-negative minutes

class InjuryImpactLayer:
    """Handles real-time injury status and its impact on minutes"""
    
    def __init__(self):
        self.injury_multipliers = {}
        self.cascade_effects = {}
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('InjuryImpactLayer')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def initialize_injury_mappings(self):
        """Initialize injury status to minute availability mappings"""
        self.injury_multipliers = {
            'out': 0.0,
            'doubtful': 0.3,
            'questionable': 0.8,
            'probable': 0.95,
            'available': 1.0,
            'unknown': 0.85
        }
        
        # Cascade effect multipliers for teammates when key players are out
        self.cascade_effects = {
            'star_player_out': 1.15,      # Other players get more minutes
            'starter_out': 1.08,          # Moderate increase
            'bench_player_out': 1.02      # Minimal increase
        }
        
    def apply_injury_impact(self, predictions: np.ndarray, player_data: pd.DataFrame, 
                           injury_reports: Dict) -> Tuple[np.ndarray, Dict]:
        """Apply injury impact to predictions"""
        self.logger.info("Applying injury impact adjustments")
        
        if not hasattr(self, 'injury_multipliers') or not self.injury_multipliers:
            self.initialize_injury_mappings()
            
        adjusted_predictions = predictions.copy()
        injury_impacts = {}
        
        # Create injury status mapping
        injury_status_map = self._process_injury_reports(injury_reports)
        
        for i, row in player_data.iterrows():
            player_name = row.get('player_name', '').lower()
            original_pred = predictions[i]
            
            # Direct injury impact
            if player_name in injury_status_map:
                status = injury_status_map[player_name]
                multiplier = self.injury_multipliers.get(status, 1.0)
                adjusted_predictions[i] = original_pred * multiplier
                
                injury_impacts[player_name] = {
                    'status': status,
                    'original_prediction': original_pred,
                    'adjusted_prediction': adjusted_predictions[i],
                    'impact': adjusted_predictions[i] - original_pred
                }
                
            # Cascade effects (teammates benefit from injured player's minutes)
            team = row.get('team', '')
            cascade_effect = self._calculate_cascade_effect(team, injury_status_map, player_data)
            
            if cascade_effect > 1.0 and player_name not in injury_status_map:
                adjusted_predictions[i] = original_pred * cascade_effect
                
                if 'cascade_benefit' not in injury_impacts:
                    injury_impacts['cascade_benefit'] = {}
                injury_impacts['cascade_benefit'][player_name] = {
                    'multiplier': cascade_effect,
                    'additional_minutes': adjusted_predictions[i] - original_pred
                }
                
        return adjusted_predictions, injury_impacts
        
    def _process_injury_reports(self, injury_reports: Dict) -> Dict[str, str]:
        """Process injury reports into player status mapping"""
        status_map = {}
        
        for source, injuries in injury_reports.items():
            for injury in injuries:
                player_name = injury.get('player', '').lower()
                status = injury.get('status', 'unknown').lower()
                
                # Clean status text
                status = self._clean_injury_status(status)
                
                # Use most recent/reliable source
                if player_name not in status_map or source == 'official':
                    status_map[player_name] = status
                    
        return status_map
        
    def _clean_injury_status(self, status: str) -> str:
        """Clean and standardize injury status text"""
        status = status.lower().strip()
        
        if 'out' in status:
            return 'out'
        elif 'doubtful' in status:
            return 'doubtful'
        elif 'questionable' in status:
            return 'questionable'
        elif 'probable' in status:
            return 'probable'
        elif 'available' in status or 'cleared' in status:
            return 'available'
        else:
            return 'unknown'
            
    def _calculate_cascade_effect(self, team: str, injury_map: Dict, player_data: pd.DataFrame) -> float:
        """Calculate cascade effect for team when players are injured"""
        team_players = player_data[player_data['team'] == team]
        
        # Count injured players by importance level
        injured_stars = 0
        injured_starters = 0
        injured_bench = 0
        
        for _, player in team_players.iterrows():
            player_name = player.get('player_name', '').lower()
            if player_name in injury_map and injury_map[player_name] == 'out':
                avg_minutes = player.get('minutes_avg_10', 20)
                
                if avg_minutes > 28:  # Star player
                    injured_stars += 1
                elif avg_minutes > 18:  # Starter
                    injured_starters += 1
                else:  # Bench player
                    injured_bench += 1
                    
        # Calculate cascade multiplier
        multiplier = 1.0
        multiplier += injured_stars * (self.cascade_effects['star_player_out'] - 1.0)
        multiplier += injured_starters * (self.cascade_effects['starter_out'] - 1.0)
        multiplier += injured_bench * (self.cascade_effects['bench_player_out'] - 1.0)
        
        return min(multiplier, 1.4)  # Cap at 40% increase

class InGameUpdater:
    """Handles live game adjustments based on real-time game flow"""
    
    def __init__(self):
        self.update_rules = {}
        self.logger = self._setup_logger()
        self._initialize_update_rules()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('InGameUpdater')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def _initialize_update_rules(self):
        """Initialize in-game update rules"""
        self.update_rules = {
            'foul_trouble': {
                '2_fouls_1st_half': 0.85,  # Reduce minutes by 15%
                '3_fouls_2nd_half': 0.75,  # Reduce minutes by 25%
                '4_fouls_anytime': 0.60,   # Reduce minutes by 40%
                '5_fouls': 0.30            # Very limited minutes
            },
            'blowout_thresholds': {
                'early_blowout': {'margin': 20, 'quarter': 2, 'star_reduction': 0.7, 'bench_increase': 1.3},
                'late_blowout': {'margin': 15, 'quarter': 3, 'star_reduction': 0.6, 'bench_increase': 1.4},
                'garbage_time': {'margin': 25, 'quarter': 4, 'star_reduction': 0.3, 'bench_increase': 1.6}
            },
            'performance_adjustments': {
                'hot_shooting': 1.1,        # 10% increase for hot players
                'cold_shooting': 0.95,      # 5% decrease for cold players
                'foul_prone': 0.9          # 10% decrease for players picking up fouls quickly
            }
        }
        
    def update_predictions_live(self, current_predictions: np.ndarray, game_state: Dict, 
                               player_data: pd.DataFrame) -> Tuple[np.ndarray, Dict]:
        """Update predictions based on live game state"""
        self.logger.info(f"Updating predictions for live game state: Q{game_state.get('quarter', 1)}")
        
        updated_predictions = current_predictions.copy()
        update_log = {'adjustments': [], 'reasoning': []}
        
        # Check for blowout conditions
        score_diff = abs(game_state.get('home_score', 0) - game_state.get('away_score', 0))
        quarter = game_state.get('quarter', 1)
        
        blowout_adjustment = self._check_blowout_conditions(score_diff, quarter)
        if blowout_adjustment:
            updated_predictions, adj_log = self._apply_blowout_adjustments(
                updated_predictions, player_data, blowout_adjustment)
            update_log['adjustments'].extend(adj_log)
            
        # Apply foul trouble adjustments
        if 'player_fouls' in game_state:
            updated_predictions, foul_log = self._apply_foul_adjustments(
                updated_predictions, player_data, game_state['player_fouls'], quarter)
            update_log['adjustments'].extend(foul_log)
            
        # Performance-based adjustments
        if 'player_performance' in game_state:
            updated_predictions, perf_log = self._apply_performance_adjustments(
                updated_predictions, player_data, game_state['player_performance'])
            update_log['adjustments'].extend(perf_log)
            
        # Injury during game
        if 'in_game_injuries' in game_state:
            updated_predictions, injury_log = self._handle_in_game_injuries(
                updated_predictions, player_data, game_state['in_game_injuries'])
            update_log['adjustments'].extend(injury_log)
            
        return updated_predictions, update_log
        
    def _check_blowout_conditions(self, score_diff: int, quarter: int) -> Optional[Dict]:
        """Check if game meets blowout conditions"""
        for condition_name, condition in self.update_rules['blowout_thresholds'].items():
            if score_diff >= condition['margin'] and quarter >= condition['quarter']:
                return condition
        return None
        
    def _apply_blowout_adjustments(self, predictions: np.ndarray, player_data: pd.DataFrame, 
                                  blowout_condition: Dict) -> Tuple[np.ndarray, List]:
        """Apply blowout-based minute adjustments"""
        adjusted_predictions = predictions.copy()
        adjustments_log = []
        
        for i, row in player_data.iterrows():
            avg_minutes = row.get('minutes_avg_10', 20)
            
            if avg_minutes > 25:  # Star player
                multiplier = blowout_condition['star_reduction']
                adjusted_predictions[i] = predictions[i] * multiplier
                adjustments_log.append({
                    'player': row.get('player_name', 'Unknown'),
                    'type': 'blowout_star_reduction',
                    'multiplier': multiplier,
                    'reason': f"Blowout condition: {blowout_condition}"
                })
            elif avg_minutes < 15:  # Bench player
                multiplier = blowout_condition['bench_increase']
                adjusted_predictions[i] = predictions[i] * multiplier
                adjustments_log.append({
                    'player': row.get('player_name', 'Unknown'),
                    'type': 'blowout_bench_increase',
                    'multiplier': multiplier,
                    'reason': f"Garbage time opportunity"
                })
                
        return adjusted_predictions, adjustments_log
        
    def _apply_foul_adjustments(self, predictions: np.ndarray, player_data: pd.DataFrame,
                               player_fouls: Dict, quarter: int) -> Tuple[np.ndarray, List]:
        """Apply foul trouble adjustments"""
        adjusted_predictions = predictions.copy()
        adjustments_log = []
        
        for i, row in player_data.iterrows():
            player_name = row.get('player_name', '').lower()
            fouls = player_fouls.get(player_name, 0)
            
            if fouls >= 5:
                multiplier = self.update_rules['foul_trouble']['5_fouls']
            elif fouls >= 4:
                multiplier = self.update_rules['foul_trouble']['4_fouls_anytime']
            elif fouls >= 3 and quarter >= 3:
                multiplier = self.update_rules['foul_trouble']['3_fouls_2nd_half']
            elif fouls >= 2 and quarter <= 2:
                multiplier = self.update_rules['foul_trouble']['2_fouls_1st_half']
            else:
                continue
                
            adjusted_predictions[i] = predictions[i] * multiplier
            adjustments_log.append({
                'player': row.get('player_name', 'Unknown'),
                'type': 'foul_trouble',
                'fouls': fouls,
                'multiplier': multiplier,
                'reason': f"{fouls} fouls in Q{quarter}"
            })
            
        return adjusted_predictions, adjustments_log
        
    def _apply_performance_adjustments(self, predictions: np.ndarray, player_data: pd.DataFrame,
                                     performance_data: Dict) -> Tuple[np.ndarray, List]:
        """Apply performance-based adjustments"""
        adjusted_predictions = predictions.copy()
        adjustments_log = []
        
        for i, row in player_data.iterrows():
            player_name = row.get('player_name', '').lower()
            perf = performance_data.get(player_name, {})
            
            # Hot shooting adjustment
            fg_pct = perf.get('field_goal_percentage', 0)
            if fg_pct > 0.6 and perf.get('field_goal_attempts', 0) >= 5:  # Hot shooter
                multiplier = self.update_rules['performance_adjustments']['hot_shooting']
                adjusted_predictions[i] = predictions[i] * multiplier
                adjustments_log.append({
                    'player': row.get('player_name', 'Unknown'),
                    'type': 'hot_shooting',
                    'multiplier': multiplier,
                    'fg_pct': fg_pct
                })
                
        return adjusted_predictions, adjustments_log
        
    def _handle_in_game_injuries(self, predictions: np.ndarray, player_data: pd.DataFrame,
                               injury_data: List) -> Tuple[np.ndarray, List]:
        """Handle players injured during the game"""
        adjusted_predictions = predictions.copy()
        adjustments_log = []
        
        for injury in injury_data:
            player_name = injury.get('player', '').lower()
            severity = injury.get('severity', 'unknown')  # 'minor', 'moderate', 'severe'
            
            # Find player index
            player_idx = None
            for i, row in player_data.iterrows():
                if row.get('player_name', '').lower() == player_name:
                    player_idx = i
                    break
                    
            if player_idx is not None:
                if severity == 'severe':
                    adjusted_predictions[player_idx] = 0  # Out for game
                elif severity == 'moderate':
                    adjusted_predictions[player_idx] = predictions[player_idx] * 0.3
                elif severity == 'minor':
                    adjusted_predictions[player_idx] = predictions[player_idx] * 0.7
                    
                adjustments_log.append({
                    'player': player_name,
                    'type': 'in_game_injury',
                    'severity': severity,
                    'new_prediction': adjusted_predictions[player_idx]
                })
                
        return adjusted_predictions, adjustments_log

class WNBAMinutesPredictionSystem:
    """Main prediction system orchestrating all models"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = self._setup_logger()
        
        # Initialize models
        self.base_model = BaseMinutesModel(self.config.get('base_model_type', 'xgboost'))
        self.context_adjuster = GameContextAdjuster()
        self.injury_layer = InjuryImpactLayer()
        self.in_game_updater = InGameUpdater()
        
        # Model state
        self.is_trained = False
        self.feature_names = []
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('WNBAMinutesPredictionSystem')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def train_system(self, training_data: pd.DataFrame, features: np.ndarray, 
                    targets: np.ndarray) -> Dict:
        """Train the complete prediction system"""
        self.logger.info("Training complete WNBA minutes prediction system")
        
        # Train base model
        base_results = self.base_model.train(features, targets)
        
        # Learn context adjustments
        context_results = self.context_adjuster.train_adjustments(training_data)
        
        # Initialize injury layer
        self.injury_layer.initialize_injury_mappings()
        
        self.is_trained = True
        
        results = {
            'base_model': base_results,
            'context_adjustments': context_results,
            'system_trained': True
        }
        
        self.logger.info("System training completed successfully")
        
        return results
        
    def predict_minutes(self, features: np.ndarray, player_data: pd.DataFrame,
                       real_time_data: Dict = None, game_state: Dict = None) -> Dict:
        """
        Complete minutes prediction pipeline
        
        Args:
            features: Feature matrix for prediction
            player_data: DataFrame with player information
            real_time_data: Real-time data including injury reports
            game_state: Live game state for in-game updates
            
        Returns:
            Dictionary with predictions and metadata
        """
        if not self.is_trained:
            raise ValueError("System must be trained before making predictions")
            
        self.logger.info(f"Predicting minutes for {len(features)} players")
        
        # Stage 1: Base minutes prediction
        base_predictions = self.base_model.predict(features)
        lower_bounds, upper_bounds = self.base_model.get_uncertainty(features)
        
        # Stage 2: Game context adjustments
        context_predictions = self.context_adjuster.apply_adjustments(
            base_predictions, player_data)
        
        # Stage 3: Injury impact layer
        injury_predictions = context_predictions
        injury_impacts = {}
        
        if real_time_data and 'injury_reports' in real_time_data:
            injury_predictions, injury_impacts = self.injury_layer.apply_injury_impact(
                context_predictions, player_data, real_time_data['injury_reports'])
                
        # Stage 4: In-game updates (if applicable)
        final_predictions = injury_predictions
        update_log = {}
        
        if game_state:
            final_predictions, update_log = self.in_game_updater.update_predictions_live(
                injury_predictions, game_state, player_data)
                
        # Compile results
        results = {
            'predictions': {
                'base_minutes': base_predictions,
                'context_adjusted': context_predictions,
                'injury_adjusted': injury_predictions,
                'final_minutes': final_predictions
            },
            'uncertainty': {
                'lower_bound': lower_bounds,
                'upper_bound': upper_bounds,
                'confidence_level': 0.8
            },
            'adjustments': {
                'injury_impacts': injury_impacts,
                'in_game_updates': update_log
            },
            'metadata': {
                'prediction_timestamp': datetime.now().isoformat(),
                'model_version': '1.0',
                'stages_applied': ['base', 'context', 'injury', 'live'] if game_state else ['base', 'context', 'injury']
            }
        }
        
        return results
        
    def save_model(self, filepath: str):
        """Save trained model to file"""
        model_data = {
            'base_model': self.base_model,
            'context_adjuster': self.context_adjuster,
            'injury_layer': self.injury_layer,
            'in_game_updater': self.in_game_updater,
            'config': self.config,
            'is_trained': self.is_trained
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
            
        self.logger.info(f"Model saved to {filepath}")
        
    @classmethod
    def load_model(cls, filepath: str) -> 'WNBAMinutesPredictionSystem':
        """Load trained model from file"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
            
        system = cls(model_data['config'])
        system.base_model = model_data['base_model']
        system.context_adjuster = model_data['context_adjuster']
        system.injury_layer = model_data['injury_layer']
        system.in_game_updater = model_data['in_game_updater']
        system.is_trained = model_data['is_trained']
        
        return system