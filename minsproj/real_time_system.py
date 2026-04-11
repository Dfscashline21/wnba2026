"""
WNBA Real-Time Update System
Handles continuous data pipeline updates, monitoring, and live prediction adjustments
"""

import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional, Callable, Any
import schedule
import time
import threading
from dataclasses import dataclass
from enum import Enum
import websocket as ws
import hashlib

from data_collector import WNBADataCollector
from prediction_models import WNBAMinutesPredictionSystem

class UpdateFrequency(Enum):
    """Update frequency constants"""
    PRE_GAME_INITIAL = "12_hours_before"
    PRE_GAME_FINAL = "1_hour_before"
    LIVE_GAME = "every_5_minutes"
    POST_GAME = "immediately"

@dataclass
class PredictionUpdate:
    """Structure for prediction updates"""
    player_id: str
    player_name: str
    old_prediction: float
    new_prediction: float
    change_reason: str
    confidence_level: float
    timestamp: str
    update_source: str

class DataQualityMonitor:
    """Monitors data quality and source reliability"""
    
    def __init__(self):
        self.source_reliability = {}
        self.data_freshness = {}
        self.contradiction_log = []
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('DataQualityMonitor')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def update_source_reliability(self, source: str, accuracy: float):
        """Update reliability score for a data source"""
        if source not in self.source_reliability:
            self.source_reliability[source] = {'accuracy': accuracy, 'updates': 1}
        else:
            current = self.source_reliability[source]
            # Weighted average with more weight on recent updates
            new_accuracy = (current['accuracy'] * current['updates'] + accuracy * 2) / (current['updates'] + 2)
            self.source_reliability[source] = {
                'accuracy': new_accuracy,
                'updates': current['updates'] + 1
            }
            
    def check_data_freshness(self, data_timestamp: str, source: str) -> bool:
        """Check if data is fresh enough for use"""
        try:
            data_time = datetime.fromisoformat(data_timestamp.replace('Z', '+00:00'))
            age_minutes = (datetime.now() - data_time).total_seconds() / 60
            
            # Different freshness requirements by source
            max_age = {
                'injury_reports': 60,    # 1 hour
                'lineups': 30,          # 30 minutes
                'betting_lines': 15,    # 15 minutes
                'social_media': 10      # 10 minutes
            }.get(source, 30)
            
            is_fresh = age_minutes <= max_age
            
            self.data_freshness[source] = {
                'last_update': data_timestamp,
                'age_minutes': age_minutes,
                'is_fresh': is_fresh
            }
            
            return is_fresh
            
        except Exception as e:
            self.logger.error(f"Error checking data freshness for {source}: {e}")
            return False
            
    def detect_contradictions(self, new_data: Dict, existing_data: Dict, source: str) -> List[Dict]:
        """Detect contradictions between data sources"""
        contradictions = []
        
        try:
            # Check for conflicting injury reports
            if 'injury_reports' in new_data and 'injury_reports' in existing_data:
                new_injuries = new_data['injury_reports']
                existing_injuries = existing_data['injury_reports']
                
                for new_source, new_reports in new_injuries.items():
                    for new_report in new_reports:
                        player = new_report.get('player', '').lower()
                        new_status = new_report.get('status', '').lower()
                        
                        # Check against existing reports
                        for existing_source, existing_reports in existing_injuries.items():
                            if existing_source != new_source:
                                for existing_report in existing_reports:
                                    if existing_report.get('player', '').lower() == player:
                                        existing_status = existing_report.get('status', '').lower()
                                        
                                        if new_status != existing_status and self._significant_status_difference(new_status, existing_status):
                                            contradiction = {
                                                'player': player,
                                                'source1': existing_source,
                                                'status1': existing_status,
                                                'source2': new_source,
                                                'status2': new_status,
                                                'timestamp': datetime.now().isoformat(),
                                                'reliability_score1': self.source_reliability.get(existing_source, {}).get('accuracy', 0.5),
                                                'reliability_score2': self.source_reliability.get(new_source, {}).get('accuracy', 0.5)
                                            }
                                            contradictions.append(contradiction)
                                            self.contradiction_log.append(contradiction)
                                            
        except Exception as e:
            self.logger.error(f"Error detecting contradictions: {e}")
            
        return contradictions
        
    def _significant_status_difference(self, status1: str, status2: str) -> bool:
        """Check if two injury statuses are significantly different"""
        status_levels = {'out': 0, 'doubtful': 1, 'questionable': 2, 'probable': 3, 'available': 4}
        
        level1 = status_levels.get(status1, 2)
        level2 = status_levels.get(status2, 2)
        
        return abs(level1 - level2) >= 2  # Significant if 2+ levels apart
        
    def get_weighted_data(self, conflicted_data: List[Dict]) -> Dict:
        """Return weighted average of conflicted data based on source reliability"""
        if not conflicted_data:
            return {}
            
        weighted_result = {}
        
        for data_point in conflicted_data:
            source = data_point.get('source', 'unknown')
            reliability = self.source_reliability.get(source, {}).get('accuracy', 0.5)
            
            for key, value in data_point.items():
                if key != 'source':
                    if key not in weighted_result:
                        weighted_result[key] = {'total_weight': 0, 'weighted_sum': 0}
                        
                    if isinstance(value, (int, float)):
                        weighted_result[key]['weighted_sum'] += value * reliability
                        weighted_result[key]['total_weight'] += reliability
                        
        # Calculate final weighted averages
        final_result = {}
        for key, weights in weighted_result.items():
            if weights['total_weight'] > 0:
                final_result[key] = weights['weighted_sum'] / weights['total_weight']
                
        return final_result

class RealTimeUpdateManager:
    """Manages real-time data updates and prediction refreshes"""
    
    def __init__(self, prediction_system: WNBAMinutesPredictionSystem, 
                 data_collector: WNBADataCollector):
        self.prediction_system = prediction_system
        self.data_collector = data_collector
        self.quality_monitor = DataQualityMonitor()
        
        self.current_predictions = {}
        self.update_callbacks = []
        self.is_running = False
        self.update_threads = {}
        
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('RealTimeUpdateManager')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def start_monitoring(self, game_date: str = None):
        """Start real-time monitoring and updates"""
        if self.is_running:
            self.logger.warning("Monitoring already running")
            return
            
        self.is_running = True
        self.logger.info(f"Starting real-time monitoring for {game_date or 'today'}")
        
        # Schedule different update frequencies
        self._schedule_updates(game_date)
        
        # Start background monitoring thread
        monitor_thread = threading.Thread(target=self._run_scheduled_updates, daemon=True)
        monitor_thread.start()
        
        # Start live game monitoring if games are in progress
        self._start_live_game_monitoring(game_date)
        
    def stop_monitoring(self):
        """Stop real-time monitoring"""
        self.is_running = False
        schedule.clear()
        
        # Stop all update threads
        for thread_name, thread in self.update_threads.items():
            if thread.is_alive():
                self.logger.info(f"Stopping {thread_name} thread")
                
        self.logger.info("Real-time monitoring stopped")
        
    def _schedule_updates(self, game_date: str):
        """Schedule updates based on game timeline"""
        
        # Pre-game updates (12+ hours before)
        schedule.every().hour.do(self._update_pre_game_data, game_date, "initial")
        
        # Pre-game final updates (1-4 hours before)
        schedule.every(15).minutes.do(self._update_pre_game_data, game_date, "final")
        
        # Post-game learning updates
        schedule.every().day.at("02:00").do(self._post_game_learning_update, game_date)
        
    def _run_scheduled_updates(self):
        """Run scheduled updates in background thread"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    async def _update_pre_game_data(self, game_date: str, update_type: str):
        """Update pre-game data and predictions"""
        self.logger.info(f"Running {update_type} pre-game update for {game_date}")
        
        try:
            # Collect fresh data
            real_time_data = await self.data_collector.collect_real_time_game_data(game_date)
            
            # Quality check
            if not self.quality_monitor.check_data_freshness(
                real_time_data.get('collection_timestamp', ''), 'injury_reports'):
                self.logger.warning("Data not fresh enough, skipping update")
                return
                
            # Check for contradictions with existing data
            if hasattr(self, 'last_real_time_data'):
                contradictions = self.quality_monitor.detect_contradictions(
                    real_time_data, self.last_real_time_data, 'pre_game')
                
                if contradictions:
                    self.logger.warning(f"Found {len(contradictions)} data contradictions")
                    for contradiction in contradictions:
                        self.logger.warning(f"Contradiction: {contradiction}")
                        
            # Update predictions if significant changes
            prediction_updates = await self._check_for_prediction_updates(
                real_time_data, update_type)
                
            if prediction_updates:
                await self._broadcast_updates(prediction_updates)
                
            self.last_real_time_data = real_time_data
            
        except Exception as e:
            self.logger.error(f"Error in pre-game update: {e}")
            
    def _start_live_game_monitoring(self, game_date: str):
        """Start live game monitoring for in-progress games"""
        
        def live_monitor():
            asyncio.run(self._live_game_monitor_loop(game_date))
            
        live_thread = threading.Thread(target=live_monitor, daemon=True)
        self.update_threads['live_monitor'] = live_thread
        live_thread.start()
        
    async def _live_game_monitor_loop(self, game_date: str):
        """Main loop for live game monitoring"""
        self.logger.info(f"Starting live game monitoring for {game_date}")
        
        while self.is_running:
            try:
                # Check for active games
                active_games = await self._get_active_games(game_date)
                
                for game in active_games:
                    await self._monitor_live_game(game)
                    
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in live monitoring: {e}")
                await asyncio.sleep(60)  # Wait longer on error
                
    async def _get_active_games(self, game_date: str) -> List[Dict]:
        """Get list of currently active games"""
        try:
            # This would connect to live game APIs
            games = []  # Placeholder
            return games
        except Exception as e:
            self.logger.error(f"Error getting active games: {e}")
            return []
            
    async def _monitor_live_game(self, game: Dict):
        """Monitor individual live game for prediction updates"""
        game_id = game.get('gameId')
        
        try:
            # Get current game state
            game_state = await self._get_live_game_state(game_id)
            
            if not game_state:
                return
                
            # Check if significant events occurred
            significant_events = self._detect_significant_events(game_state)
            
            if significant_events:
                self.logger.info(f"Detected significant events in game {game_id}: {significant_events}")
                
                # Update predictions
                updated_predictions = await self._update_live_predictions(game_id, game_state)
                
                if updated_predictions:
                    await self._broadcast_live_updates(game_id, updated_predictions, significant_events)
                    
        except Exception as e:
            self.logger.error(f"Error monitoring game {game_id}: {e}")
            
    async def _get_live_game_state(self, game_id: str) -> Optional[Dict]:
        """Get current state of live game"""
        try:
            # This would connect to live game data APIs
            game_state = {
                'game_id': game_id,
                'quarter': 2,
                'time_remaining': '8:45',
                'home_score': 45,
                'away_score': 42,
                'player_fouls': {},
                'player_performance': {},
                'last_update': datetime.now().isoformat()
            }
            return game_state
        except Exception as e:
            self.logger.error(f"Error getting game state for {game_id}: {e}")
            return None
            
    def _detect_significant_events(self, game_state: Dict) -> List[str]:
        """Detect significant events that might affect minutes"""
        events = []
        
        # Blowout detection
        score_diff = abs(game_state.get('home_score', 0) - game_state.get('away_score', 0))
        quarter = game_state.get('quarter', 1)
        
        if score_diff >= 20 and quarter >= 2:
            events.append(f"blowout_{score_diff}pt_Q{quarter}")
            
        # Foul trouble detection
        player_fouls = game_state.get('player_fouls', {})
        for player, fouls in player_fouls.items():
            if fouls >= 4:
                events.append(f"foul_trouble_{player}_{fouls}fouls")
                
        # Performance streaks
        player_performance = game_state.get('player_performance', {})
        for player, stats in player_performance.items():
            fg_pct = stats.get('field_goal_percentage', 0)
            attempts = stats.get('field_goal_attempts', 0)
            
            if attempts >= 5 and fg_pct >= 0.7:
                events.append(f"hot_shooting_{player}")
            elif attempts >= 5 and fg_pct <= 0.2:
                events.append(f"cold_shooting_{player}")
                
        return events
        
    async def _update_live_predictions(self, game_id: str, game_state: Dict) -> Optional[Dict]:
        """Update predictions based on live game state"""
        try:
            # Get current predictions for this game
            game_predictions = self.current_predictions.get(game_id, {})
            
            if not game_predictions:
                self.logger.warning(f"No current predictions found for game {game_id}")
                return None
                
            # Apply live updates using the prediction system
            player_data = game_predictions.get('player_data')
            current_preds = game_predictions.get('predictions', {}).get('final_minutes', [])
            
            if player_data is None or len(current_preds) == 0:
                return None
                
            # Get updated predictions
            updated_results = self.prediction_system.predict_minutes(
                features=game_predictions.get('features'),
                player_data=player_data,
                real_time_data=game_predictions.get('real_time_data'),
                game_state=game_state
            )
            
            # Calculate changes
            prediction_changes = []
            new_predictions = updated_results['predictions']['final_minutes']
            
            for i, (old_pred, new_pred) in enumerate(zip(current_preds, new_predictions)):
                if abs(new_pred - old_pred) > 2.0:  # Significant change threshold
                    player_name = player_data.iloc[i]['player_name']
                    change = PredictionUpdate(
                        player_id=str(player_data.iloc[i]['player_id']),
                        player_name=player_name,
                        old_prediction=old_pred,
                        new_prediction=new_pred,
                        change_reason="live_game_adjustment",
                        confidence_level=0.8,
                        timestamp=datetime.now().isoformat(),
                        update_source="live_monitor"
                    )
                    prediction_changes.append(change)
                    
            # Update stored predictions
            self.current_predictions[game_id] = {
                **game_predictions,
                'predictions': updated_results['predictions'],
                'last_update': datetime.now().isoformat(),
                'update_log': updated_results['adjustments'].get('in_game_updates', {})
            }
            
            return {
                'game_id': game_id,
                'prediction_changes': prediction_changes,
                'update_metadata': updated_results['metadata']
            }
            
        except Exception as e:
            self.logger.error(f"Error updating live predictions: {e}")
            return None
            
    async def _check_for_prediction_updates(self, real_time_data: Dict, 
                                          update_type: str) -> List[PredictionUpdate]:
        """Check if real-time data requires prediction updates"""
        updates = []
        
        try:
            # Check injury report changes
            injury_reports = real_time_data.get('injury_reports', {})
            
            for source, injuries in injury_reports.items():
                for injury in injuries:
                    player_name = injury.get('player', '')
                    status = injury.get('status', '')
                    
                    # Check if this is a new or changed injury status
                    if self._is_significant_injury_change(player_name, status, source):
                        # Would trigger prediction update for affected player
                        update = PredictionUpdate(
                            player_id="unknown",  # Would need to lookup
                            player_name=player_name,
                            old_prediction=0.0,   # Would get from current predictions
                            new_prediction=0.0,   # Would calculate new prediction
                            change_reason=f"injury_status_change_{status}",
                            confidence_level=0.9,
                            timestamp=datetime.now().isoformat(),
                            update_source=source
                        )
                        updates.append(update)
                        
            # Check lineup changes
            lineup_data = real_time_data.get('starting_lineups', {})
            
            for game_id, lineup in lineup_data.items():
                if lineup.get('confirmed', False):
                    # Check for lineup changes that affect predictions
                    lineup_changes = self._detect_lineup_changes(game_id, lineup)
                    updates.extend(lineup_changes)
                    
        except Exception as e:
            self.logger.error(f"Error checking for prediction updates: {e}")
            
        return updates
        
    def _is_significant_injury_change(self, player_name: str, status: str, source: str) -> bool:
        """Check if injury status change is significant enough to update predictions"""
        # Would check against stored injury statuses
        return True  # Placeholder
        
    def _detect_lineup_changes(self, game_id: str, lineup: Dict) -> List[PredictionUpdate]:
        """Detect significant lineup changes"""
        changes = []
        # Would compare against stored lineups and create updates
        return changes
        
    async def _broadcast_updates(self, updates: List[PredictionUpdate]):
        """Broadcast prediction updates to registered callbacks"""
        if not updates:
            return
            
        self.logger.info(f"Broadcasting {len(updates)} prediction updates")
        
        for callback in self.update_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(updates)
                else:
                    callback(updates)
            except Exception as e:
                self.logger.error(f"Error in update callback: {e}")
                
    async def _broadcast_live_updates(self, game_id: str, update_data: Dict, events: List[str]):
        """Broadcast live game prediction updates"""
        self.logger.info(f"Broadcasting live updates for game {game_id}")
        
        broadcast_data = {
            'type': 'live_update',
            'game_id': game_id,
            'events': events,
            'prediction_changes': [update.__dict__ for update in update_data['prediction_changes']],
            'timestamp': datetime.now().isoformat()
        }
        
        for callback in self.update_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(broadcast_data)
                else:
                    callback(broadcast_data)
            except Exception as e:
                self.logger.error(f"Error in live update callback: {e}")
                
    async def _post_game_learning_update(self, game_date: str):
        """Post-game learning and model updates"""
        self.logger.info(f"Running post-game learning for {game_date}")
        
        try:
            # Collect final game results
            completed_games = await self._get_completed_games(game_date)
            
            for game in completed_games:
                await self._analyze_prediction_accuracy(game)
                await self._update_model_parameters(game)
                
        except Exception as e:
            self.logger.error(f"Error in post-game learning: {e}")
            
    async def _get_completed_games(self, game_date: str) -> List[Dict]:
        """Get completed games for the date"""
        # Would fetch completed games data
        return []  # Placeholder
        
    async def _analyze_prediction_accuracy(self, game: Dict):
        """Analyze how accurate predictions were for a game"""
        game_id = game.get('gameId')
        
        # Would compare final predictions with actual minutes played
        # Update source reliability scores
        # Log prediction errors for model improvement
        
    async def _update_model_parameters(self, game: Dict):
        """Update model parameters based on game results"""
        # Would update context adjusters, injury multipliers, etc.
        # based on observed outcomes
        pass
        
    def register_update_callback(self, callback: Callable):
        """Register callback function for prediction updates"""
        self.update_callbacks.append(callback)
        self.logger.info(f"Registered update callback: {callback.__name__}")
        
    def unregister_update_callback(self, callback: Callable):
        """Unregister callback function"""
        if callback in self.update_callbacks:
            self.update_callbacks.remove(callback)
            self.logger.info(f"Unregistered update callback: {callback.__name__}")
            
    def get_system_status(self) -> Dict:
        """Get current system status and health metrics"""
        return {
            'is_running': self.is_running,
            'active_threads': len([t for t in self.update_threads.values() if t.is_alive()]),
            'registered_callbacks': len(self.update_callbacks),
            'current_predictions': len(self.current_predictions),
            'source_reliability': self.quality_monitor.source_reliability,
            'data_freshness': self.quality_monitor.data_freshness,
            'recent_contradictions': len(self.quality_monitor.contradiction_log[-10:]),
            'last_update': datetime.now().isoformat()
        }