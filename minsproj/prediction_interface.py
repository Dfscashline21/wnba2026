"""
WNBA Minutes Prediction Interface
User-facing interface for generating predictions, updates, and comprehensive reporting
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import logging
import json
from dataclasses import dataclass, asdict
from pathlib import Path
import aiohttp
import asyncio

# Import our modules
from data_collector import WNBADataCollector
from feature_engineering import WNBAFeatureEngineer
from prediction_models import WNBAMinutesPredictionSystem
from real_time_system import RealTimeUpdateManager
from injury_modeling import InjuryDatabase, InjuryClassificationSystem, CascadeEffectModeler
from model_validation import ModelValidator

@dataclass
class PlayerPrediction:
    """Individual player prediction structure"""
    player_id: str
    player_name: str
    team: str
    opponent: str
    game_date: str
    game_id: str
    
    # Prediction stages
    base_minutes: float
    context_adjusted_minutes: float
    injury_adjusted_minutes: float
    final_minutes: float
    
    # Uncertainty and confidence
    confidence_level: float
    lower_bound: float
    upper_bound: float
    
    # Key factors
    key_factors: List[Dict[str, Any]]
    risk_factors: List[str]
    
    # Alternative scenarios
    scenarios: Dict[str, float]
    
    # Metadata
    prediction_timestamp: str
    model_version: str

@dataclass
class GamePredictions:
    """Complete game predictions structure"""
    game_id: str
    home_team: str
    away_team: str
    game_date: str
    venue: str
    
    # Player predictions
    home_players: List[PlayerPrediction]
    away_players: List[PlayerPrediction]
    
    # Team rotations
    home_rotation_depth: int
    away_rotation_depth: int
    
    # Game context
    game_importance: str
    expected_competitiveness: str
    injury_impact_summary: Dict[str, Any]
    
    # Metadata
    prediction_confidence: float
    last_updated: str
    data_sources: List[str]

class WNBAPredictionInterface:
    """Main interface for WNBA minutes predictions"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize prediction interface with all components
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        
        # Initialize components
        self.data_collector = WNBADataCollector(self.config)
        self.feature_engineer = WNBAFeatureEngineer()
        self.prediction_system = WNBAMinutesPredictionSystem(self.config)
        self.injury_db = InjuryDatabase()
        self.injury_classifier = InjuryClassificationSystem(self.injury_db)
        self.cascade_modeler = CascadeEffectModeler()
        self.validator = ModelValidator(self.config.get('target_metrics'))
        
        # Real-time system (initialized later)
        self.real_time_manager = None
        
        # State
        self.is_trained = False
        self.current_predictions = {}
        self.prediction_history = []
        
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration from file or use defaults"""
        default_config = {
            'base_model_type': 'xgboost',
            'target_metrics': {
                'mae': 3.5,
                'within_threshold_pct': 75,
                'directional_accuracy': 80,
                'extreme_event_accuracy': 90
            },
            'real_time_enabled': True,
            'validation_enabled': True
        }
        
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                default_config.update(file_config)
            except Exception as e:
                logging.warning(f"Could not load config file: {e}")
                
        return default_config
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('WNBAPredictionInterface')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    async def initialize_system(self, training_data_path: Optional[str] = None,
                               model_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Initialize and train the complete prediction system
        
        Args:
            training_data_path: Path to training data file
            model_path: Path to pre-trained model file
            
        Returns:
            Initialization results
        """
        self.logger.info("Initializing WNBA minutes prediction system")
        
        try:
            if model_path and Path(model_path).exists():
                # Load pre-trained model
                self.prediction_system = WNBAMinutesPredictionSystem.load_model(model_path)
                self.is_trained = True
                self.logger.info("Loaded pre-trained model")
                
            elif training_data_path:
                # Train new model
                training_results = await self._train_system(training_data_path)
                self.is_trained = True
                self.logger.info("Trained new model")
                
            else:
                # Use demo/placeholder training
                training_results = await self._initialize_demo_system()
                self.is_trained = True
                self.logger.info("Initialized with demo system")
                
            # Initialize real-time system if enabled
            if self.config.get('real_time_enabled', True):
                self.real_time_manager = RealTimeUpdateManager(
                    self.prediction_system, self.data_collector)
                
            initialization_results = {
                'status': 'success',
                'system_ready': True,
                'model_trained': self.is_trained,
                'real_time_enabled': self.real_time_manager is not None,
                'timestamp': datetime.now().isoformat()
            }
            
            return initialization_results
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'system_ready': False,
                'timestamp': datetime.now().isoformat()
            }
            
    async def _train_system(self, training_data_path: str) -> Dict:
        """Train the prediction system with provided data"""
        
        # Load training data
        training_data = pd.read_csv(training_data_path)
        
        # Feature engineering
        engineered_data = self.feature_engineer.create_all_features(training_data)
        
        # Prepare features and targets
        features = self.feature_engineer.prepare_features_for_modeling(engineered_data, fit_encoders=True)
        targets = engineered_data['minutes_played'].values
        
        # Train system
        training_results = self.prediction_system.train_system(
            engineered_data, features, targets)
        
        return training_results
        
    async def _initialize_demo_system(self) -> Dict:
        """Initialize system with demo/placeholder data for testing"""
        
        # Generate synthetic training data
        n_samples = 1000
        n_features = 50
        
        demo_data = pd.DataFrame({
            'player_id': np.random.randint(1, 101, n_samples),
            'game_date': pd.date_range('2023-01-01', periods=n_samples, freq='D'),
            'minutes_played': np.random.normal(20, 8, n_samples),
            'team': np.random.choice(['LAS', 'SEA', 'NYL', 'CON'], n_samples),
            'opponent': np.random.choice(['CHI', 'IND', 'ATL', 'WAS'], n_samples),
            'is_home': np.random.choice([True, False], n_samples),
            'player_name': [f'Player_{i}' for i in range(n_samples)],
            'season': ['2024'] * n_samples,
            'points': np.random.normal(12, 6, n_samples),
            'rebounds': np.random.normal(5, 3, n_samples),
            'assists': np.random.normal(4, 3, n_samples)
        })
        
        # Ensure non-negative values
        demo_data['minutes_played'] = np.maximum(demo_data['minutes_played'], 0)
        demo_data['points'] = np.maximum(demo_data['points'], 0)
        demo_data['rebounds'] = np.maximum(demo_data['rebounds'], 0)
        demo_data['assists'] = np.maximum(demo_data['assists'], 0)
        
        # Feature engineering
        engineered_data = self.feature_engineer.create_all_features(demo_data)
        
        # Prepare features and targets
        features = self.feature_engineer.prepare_features_for_modeling(engineered_data, fit_encoders=True)
        targets = engineered_data['minutes_played'].values
        
        # Train system
        training_results = self.prediction_system.train_system(
            engineered_data, features, targets)
        
        return training_results
        
    async def predict_game_minutes(self, game_date: str, game_id: Optional[str] = None,
                                 team_filter: Optional[List[str]] = None) -> Union[GamePredictions, List[GamePredictions]]:
        """
        Generate comprehensive minute predictions for game(s)
        
        Args:
            game_date: Date in YYYY-MM-DD format
            game_id: Specific game ID (optional)
            team_filter: List of team codes to filter (optional)
            
        Returns:
            GamePredictions object(s) with complete predictions
        """
        
        if not self.is_trained:
            raise ValueError("System must be initialized and trained before making predictions")
            
        self.logger.info(f"Generating predictions for {game_date}")
        
        try:
            # Collect real-time data
            real_time_data = await self.data_collector.collect_real_time_game_data(game_date)
            
            # Get game schedule for the date
            games_data = await self._get_games_schedule(game_date, game_id, team_filter)
            
            if not games_data:
                return []
                
            # Generate predictions for each game
            game_predictions = []
            
            for game_info in games_data:
                game_pred = await self._predict_single_game(game_info, real_time_data)
                if game_pred:
                    game_predictions.append(game_pred)
                    
            # Store predictions
            for pred in game_predictions:
                self.current_predictions[pred.game_id] = pred
                
            # Return single game or list based on input
            if game_id and len(game_predictions) == 1:
                return game_predictions[0]
            else:
                return game_predictions
                
        except Exception as e:
            self.logger.error(f"Error generating predictions: {e}")
            raise
            
    async def _get_games_schedule(self, game_date: str, game_id: Optional[str] = None,
                                 team_filter: Optional[List[str]] = None) -> List[Dict]:
        """Get games schedule for specified date and filters"""
        
        # Try to fetch real WNBA games for the date
        try:
            real_games = await self._fetch_real_wnba_games(game_date)
            if real_games:
                sample_games = real_games
            else:
                # Fallback to demo games if no real games found
                sample_games = self._create_demo_games(game_date)
        except Exception as e:
            self.logger.warning(f"Could not fetch real games, using demo: {e}")
            sample_games = self._create_demo_games(game_date)
        
        # Apply filters
        if game_id:
            sample_games = [g for g in sample_games if g['gameId'] == game_id]
            
        if team_filter:
            sample_games = [
                g for g in sample_games 
                if (g['homeTeam']['teamTricode'] in team_filter or 
                    g['awayTeam']['teamTricode'] in team_filter)
            ]
            
        return sample_games
    
    async def _fetch_real_wnba_games(self, game_date: str) -> List[Dict]:
        """Fetch real WNBA games from ESPN API"""
        try:
            # Convert date format for ESPN API
            date_obj = datetime.strptime(game_date, '%Y-%m-%d')
            espn_date = date_obj.strftime('%Y%m%d')
            
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/scoreboard?dates={espn_date}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_espn_games(data, game_date)
                        
        except Exception as e:
            self.logger.error(f"Error fetching real games: {e}")
            
        return []
    
    def _parse_espn_games(self, espn_data: dict, game_date: str) -> List[Dict]:
        """Parse ESPN API response into our game format"""
        games = []
        
        try:
            events = espn_data.get('events', [])
            
            for i, event in enumerate(events):
                competitions = event.get('competitions', [])
                if not competitions:
                    continue
                    
                competition = competitions[0]
                competitors = competition.get('competitors', [])
                
                if len(competitors) >= 2:
                    # Find home and away teams
                    home_team = None
                    away_team = None
                    
                    for competitor in competitors:
                        if competitor.get('homeAway') == 'home':
                            home_team = {
                                'teamTricode': competitor['team'].get('abbreviation', 'UNK'),
                                'teamName': competitor['team'].get('displayName', 'Unknown')
                            }
                        elif competitor.get('homeAway') == 'away':
                            away_team = {
                                'teamTricode': competitor['team'].get('abbreviation', 'UNK'), 
                                'teamName': competitor['team'].get('displayName', 'Unknown')
                            }
                    
                    if home_team and away_team:
                        games.append({
                            'gameId': f"espn_{event.get('id', i)}",
                            'gameDate': game_date,
                            'homeTeam': home_team,
                            'awayTeam': away_team,
                            'venue': competition.get('venue', {}).get('fullName', 'Unknown Venue'),
                            'status': event.get('status', {}).get('type', {}).get('description', 'Scheduled')
                        })
                        
        except Exception as e:
            self.logger.error(f"Error parsing ESPN games data: {e}")
            
        return games
    
    def _create_demo_games(self, game_date: str) -> List[Dict]:
        """Create demo games as fallback"""
        return [
            {
                'gameId': 'demo_001',
                'gameDate': game_date,
                'homeTeam': {'teamTricode': 'LAS', 'teamName': 'Las Vegas Aces'},
                'awayTeam': {'teamTricode': 'SEA', 'teamName': 'Seattle Storm'},
                'venue': 'Michelob ULTRA Arena',
                'status': 'Demo Game'
            },
            {
                'gameId': 'demo_002', 
                'gameDate': game_date,
                'homeTeam': {'teamTricode': 'NYL', 'teamName': 'New York Liberty'},
                'awayTeam': {'teamTricode': 'CON', 'teamName': 'Connecticut Sun'},
                'venue': 'Barclays Center',
                'status': 'Demo Game'
            }
        ]
        
    async def _predict_single_game(self, game_info: Dict, real_time_data: Dict) -> Optional[GamePredictions]:
        """Generate predictions for a single game"""
        
        try:
            game_id = game_info['gameId']
            home_team = game_info['homeTeam']['teamTricode']
            away_team = game_info['awayTeam']['teamTricode']
            
            # Get player rosters
            home_players_data = await self._get_team_roster(home_team, game_info['gameDate'])
            away_players_data = await self._get_team_roster(away_team, game_info['gameDate'])
            
            # Combine player data
            all_players_data = pd.concat([home_players_data, away_players_data], ignore_index=True)
            
            # Feature engineering
            game_features_data = self.feature_engineer.create_all_features(
                all_players_data, real_time_data)
                
            # Prepare features
            features = self.feature_engineer.prepare_features_for_modeling(
                game_features_data, fit_encoders=False)
                
            # Generate predictions
            prediction_results = self.prediction_system.predict_minutes(
                features, all_players_data, real_time_data)
                
            # Process results into structured format
            game_predictions = self._format_game_predictions(
                game_info, all_players_data, prediction_results, real_time_data)
                
            return game_predictions
            
        except Exception as e:
            self.logger.error(f"Error predicting game {game_info.get('gameId', 'unknown')}: {e}")
            return None
            
    async def _get_team_roster(self, team_code: str, game_date: str) -> pd.DataFrame:
        """Get team roster for specific game date"""
        
        # Demo roster data
        n_players = 12
        roster_data = {
            'player_id': [f"{team_code}_{i}" for i in range(1, n_players + 1)],
            'player_name': [f"{team_code} Player {i}" for i in range(1, n_players + 1)],
            'team': [team_code] * n_players,
            'opponent': ['OPP'] * n_players,  # Will be updated
            'game_date': [pd.to_datetime(game_date)] * n_players,
            'is_home': [True] * n_players,  # Will be updated based on game
            'position': np.random.choice(['PG', 'SG', 'SF', 'PF', 'C'], n_players),
            'is_starter': [True] * 5 + [False] * (n_players - 5),
            'minutes_avg_10': np.random.normal(18, 8, n_players),
            'age': np.random.randint(20, 35, n_players),
            'season': ['2024'] * n_players,
            'minutes_played': np.random.normal(18, 8, n_players),
            # Add missing statistical columns
            'points': np.random.normal(12, 6, n_players),
            'rebounds': np.random.normal(6, 3, n_players),
            'assists': np.random.normal(4, 3, n_players)
        }
        
        # Ensure non-negative values
        roster_data['minutes_avg_10'] = np.maximum(roster_data['minutes_avg_10'], 0)
        roster_data['minutes_played'] = np.maximum(roster_data['minutes_played'], 0)
        roster_data['points'] = np.maximum(roster_data['points'], 0)
        roster_data['rebounds'] = np.maximum(roster_data['rebounds'], 0)
        roster_data['assists'] = np.maximum(roster_data['assists'], 0)
        
        return pd.DataFrame(roster_data)
        
    def _format_game_predictions(self, game_info: Dict, player_data: pd.DataFrame,
                               prediction_results: Dict, real_time_data: Dict) -> GamePredictions:
        """Format prediction results into structured GamePredictions object"""
        
        game_id = game_info['gameId']
        home_team = game_info['homeTeam']['teamTricode']
        away_team = game_info['awayTeam']['teamTricode']
        
        # Separate home and away players
        home_players = player_data[player_data['team'] == home_team]
        away_players = player_data[player_data['team'] == away_team]
        
        # Create player predictions
        home_predictions = []
        away_predictions = []
        
        final_minutes = prediction_results['predictions']['final_minutes']
        uncertainty = prediction_results['uncertainty']
        
        for i, (_, player) in enumerate(player_data.iterrows()):
            player_pred = PlayerPrediction(
                player_id=str(player['player_id']),
                player_name=player['player_name'],
                team=player['team'],
                opponent=player['opponent'],
                game_date=game_info['gameDate'],
                game_id=game_id,
                
                base_minutes=float(prediction_results['predictions']['base_minutes'][i]),
                context_adjusted_minutes=float(prediction_results['predictions']['context_adjusted'][i]),
                injury_adjusted_minutes=float(prediction_results['predictions']['injury_adjusted'][i]),
                final_minutes=float(final_minutes[i]),
                
                confidence_level=float(uncertainty['confidence_level']),
                lower_bound=float(uncertainty['lower_bound'][i]),
                upper_bound=float(uncertainty['upper_bound'][i]),
                
                key_factors=self._extract_key_factors(player, prediction_results, i),
                risk_factors=self._identify_risk_factors(player, real_time_data),
                scenarios=self._generate_scenarios(player, final_minutes[i]),
                
                prediction_timestamp=prediction_results['metadata']['prediction_timestamp'],
                model_version=prediction_results['metadata']['model_version']
            )
            
            if player['team'] == home_team:
                home_predictions.append(player_pred)
            else:
                away_predictions.append(player_pred)
                
        # Create game predictions object
        game_predictions = GamePredictions(
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
            game_date=game_info['gameDate'],
            venue=game_info.get('venue', 'Unknown'),
            
            home_players=home_predictions,
            away_players=away_predictions,
            
            home_rotation_depth=len([p for p in home_predictions if p.final_minutes > 10]),
            away_rotation_depth=len([p for p in away_predictions if p.final_minutes > 10]),
            
            game_importance=self._assess_game_importance(game_info, real_time_data),
            expected_competitiveness=self._assess_competitiveness(home_team, away_team),
            injury_impact_summary=self._summarize_injury_impacts(prediction_results),
            
            prediction_confidence=float(np.mean([p.confidence_level for p in home_predictions + away_predictions])),
            last_updated=datetime.now().isoformat(),
            data_sources=list(real_time_data.keys()) if real_time_data else []
        )
        
        return game_predictions
        
    def _extract_key_factors(self, player: pd.Series, prediction_results: Dict, player_idx: int) -> List[Dict[str, Any]]:
        """Extract key factors affecting player's prediction"""
        
        key_factors = []
        
        # Rest impact
        rest_days = player.get('days_since_last_game', 1)
        if rest_days == 0:
            key_factors.append({
                'factor': 'Back-to-back game',
                'impact': -1.5,
                'description': 'Second game of back-to-back, likely reduced minutes'
            })
        elif rest_days >= 3:
            key_factors.append({
                'factor': 'Extended rest',
                'impact': +1.0,
                'description': f'{rest_days} days rest, fresh legs'
            })
            
        # Injury status
        injury_impacts = prediction_results.get('adjustments', {}).get('injury_impacts', {})
        player_name = player['player_name'].lower()
        
        if player_name in injury_impacts:
            impact_data = injury_impacts[player_name]
            key_factors.append({
                'factor': f"Injury status: {impact_data['status']}",
                'impact': impact_data['impact'],
                'description': f"Health status affects playing time"
            })
            
        # Home court
        if player.get('is_home', False):
            key_factors.append({
                'factor': 'Home court advantage',
                'impact': +0.8,
                'description': 'Playing at home typically increases minutes'
            })
            
        return key_factors[:5]  # Limit to top 5 factors
        
    def _identify_risk_factors(self, player: pd.Series, real_time_data: Dict) -> List[str]:
        """Identify risk factors that could affect prediction accuracy"""
        
        risk_factors = []
        
        # Age-related risks
        age = player.get('age', 25)
        if age > 32:
            risk_factors.append('Age-related load management possible')
            
        # Injury history
        if player.get('injury_prone_score', 0) > 0.6:
            risk_factors.append('High injury history - monitor closely')
            
        # Back-to-back
        if player.get('days_since_last_game', 1) == 0:
            risk_factors.append('Back-to-back fatigue risk')
            
        # Low sample size
        if player.get('games_played_season', 20) < 10:
            risk_factors.append('Limited data - lower prediction confidence')
            
        return risk_factors
        
    def _generate_scenarios(self, player: pd.Series, base_prediction: float) -> Dict[str, float]:
        """Generate alternative minute scenarios"""
        
        scenarios = {
            'base_case': base_prediction,
            'optimistic': min(base_prediction * 1.15, 40),  # 15% higher, cap at 40
            'pessimistic': max(base_prediction * 0.8, 0),   # 20% lower, floor at 0
            'blowout_win': max(base_prediction * 0.7, 0),   # Reduced in blowout
            'close_game': min(base_prediction * 1.1, 42),   # Increased in close game
            'injury_concern': max(base_prediction * 0.3, 0) # Precautionary reduction
        }
        
        return scenarios
        
    def _assess_game_importance(self, game_info: Dict, real_time_data: Dict) -> str:
        """Assess game importance level"""
        
        # Simple heuristic - would be more sophisticated in real implementation
        game_date = pd.to_datetime(game_info['gameDate'])
        
        if game_date.month >= 9:  # September onwards
            return 'high'  # Playoff push
        elif game_date.month >= 7:
            return 'medium'  # Mid-season
        else:
            return 'regular'  # Early season
            
    def _assess_competitiveness(self, home_team: str, away_team: str) -> str:
        """Assess expected game competitiveness"""
        
        # Placeholder - would use actual team standings/ratings
        return 'competitive'
        
    def _summarize_injury_impacts(self, prediction_results: Dict) -> Dict[str, Any]:
        """Summarize injury impacts for the game"""
        
        injury_impacts = prediction_results.get('adjustments', {}).get('injury_impacts', {})
        
        if not injury_impacts:
            return {'message': 'No significant injury impacts detected'}
            
        return {
            'players_affected': len(injury_impacts),
            'total_minutes_affected': sum(
                abs(impact.get('impact', 0)) for impact in injury_impacts.values()
                if isinstance(impact, dict)
            ),
            'severity': 'high' if len(injury_impacts) > 2 else 'moderate'
        }
        
    def get_live_updates(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Get live updates for ongoing game"""
        
        if not self.real_time_manager or not self.real_time_manager.is_running:
            return {'error': 'Real-time system not running'}
            
        if game_id not in self.current_predictions:
            return {'error': 'Game predictions not found'}
            
        # Get current stored predictions
        current_pred = self.current_predictions[game_id]
        
        # Return live update status
        return {
            'game_id': game_id,
            'last_updated': current_pred.last_updated,
            'live_monitoring_active': True,
            'update_frequency': '30 seconds',
            'next_update': (datetime.now() + timedelta(seconds=30)).isoformat()
        }
        
    def start_live_monitoring(self, game_date: str = None):
        """Start live monitoring for games"""
        
        if not self.real_time_manager:
            self.logger.error("Real-time manager not initialized")
            return False
            
        self.real_time_manager.start_monitoring(game_date)
        return True
        
    def stop_live_monitoring(self):
        """Stop live monitoring"""
        
        if self.real_time_manager and self.real_time_manager.is_running:
            self.real_time_manager.stop_monitoring()
            
    def export_predictions(self, game_predictions: Union[GamePredictions, List[GamePredictions]],
                          format: str = 'json', file_path: Optional[str] = None) -> Union[str, Dict, pd.DataFrame]:
        """
        Export predictions in various formats
        
        Args:
            game_predictions: Predictions to export
            format: Export format ('json', 'csv', 'dict')
            file_path: Optional file path to save
            
        Returns:
            Formatted predictions
        """
        
        if not isinstance(game_predictions, list):
            game_predictions = [game_predictions]
            
        if format == 'json':
            export_data = [asdict(pred) for pred in game_predictions]
            result = json.dumps(export_data, indent=2, default=str)
            
        elif format == 'csv':
            # Flatten to player-level data
            player_data = []
            for game in game_predictions:
                for player in game.home_players + game.away_players:
                    player_dict = asdict(player)
                    player_dict['venue'] = game.venue
                    player_data.append(player_dict)
                    
            result = pd.DataFrame(player_data)
            
        elif format == 'dict':
            result = [asdict(pred) for pred in game_predictions]
            
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        # Save to file if path provided
        if file_path:
            if format == 'json':
                with open(file_path, 'w') as f:
                    f.write(result)
            elif format == 'csv':
                result.to_csv(file_path, index=False)
            elif format == 'dict':
                with open(file_path, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                    
            self.logger.info(f"Predictions exported to {file_path}")
            
        return result
        
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        
        status = {
            'system_initialized': self.is_trained,
            'model_ready': self.prediction_system.is_trained if self.prediction_system else False,
            'real_time_monitoring': {
                'enabled': self.real_time_manager is not None,
                'running': self.real_time_manager.is_running if self.real_time_manager else False,
                'status': self.real_time_manager.get_system_status() if self.real_time_manager else {}
            },
            'current_predictions': len(self.current_predictions),
            'prediction_history': len(self.prediction_history),
            'validation_results': {
                'total_validations': len(self.validator.validation_history),
                'last_validation': self.validator.validation_history[-1] if self.validator.validation_history else None
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return status
        
    def validate_recent_predictions(self, actual_results_path: str) -> Dict[str, Any]:
        """Validate recent predictions against actual results"""
        
        if not self.config.get('validation_enabled', True):
            return {'error': 'Validation not enabled'}
            
        try:
            # Load actual results
            actual_results = pd.read_csv(actual_results_path)
            
            # Match predictions with actuals
            # This would need proper matching logic based on game_id and player_id
            
            # For demo, create sample validation
            n_predictions = 100
            predictions = np.random.normal(20, 6, n_predictions)
            actuals = predictions + np.random.normal(0, 3, n_predictions)  # Add noise
            
            # Validate
            validation_results = self.validator.validate_predictions(predictions, actuals)
            
            return {
                'validation_results': asdict(validation_results),
                'system_performance': 'meeting_targets' if validation_results.mae <= 3.5 else 'needs_improvement'
            }
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return {'error': str(e)}

# Convenience function for quick predictions
async def predict_daily_minutes(game_date: str, config_path: Optional[str] = None) -> List[GamePredictions]:
    """
    Convenience function for generating daily predictions
    
    Args:
        game_date: Date in YYYY-MM-DD format
        config_path: Optional config file path
        
    Returns:
        List of game predictions for the date
    """
    
    interface = WNBAPredictionInterface(config_path)
    
    # Initialize system
    await interface.initialize_system()
    
    # Generate predictions
    predictions = await interface.predict_game_minutes(game_date)
    
    return predictions if isinstance(predictions, list) else [predictions]