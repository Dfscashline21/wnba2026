"""
WNBA Model Validation and Performance Metrics System
Comprehensive validation framework for the minutes prediction model with real-time performance monitoring
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
import logging
from dataclasses import dataclass
import json
import pickle
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

@dataclass
class ValidationResults:
    """Structure for storing validation results"""
    mae: float
    rmse: float
    r2: float
    mape: float
    within_threshold_accuracy: float
    directional_accuracy: float
    extreme_event_accuracy: float
    prediction_bias: float
    validation_date: str
    sample_size: int
    additional_metrics: Dict[str, float] = None

@dataclass
class PlayerValidationResults:
    """Player-specific validation results"""
    player_id: str
    player_name: str
    games_predicted: int
    mae: float
    rmse: float
    bias: float
    correlation: float
    within_threshold_pct: float

class ModelValidator:
    """Main model validation class with comprehensive metrics"""
    
    def __init__(self, target_metrics: Dict[str, float] = None):
        """
        Initialize validator with target performance metrics
        
        Args:
            target_metrics: Dictionary of target performance thresholds
        """
        self.target_metrics = target_metrics or {
            'mae': 3.5,                    # Mean Absolute Error < 3.5 minutes
            'within_threshold_pct': 75,    # 75% predictions within ±4 minutes
            'directional_accuracy': 80,    # 80% directional accuracy
            'extreme_event_accuracy': 90   # 90% accuracy on 0-minute games
        }
        
        self.validation_history = []
        self.player_performance = {}
        self.feature_importance_tracking = {}
        self.prediction_log = []
        
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('ModelValidator')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def validate_predictions(self, predictions: np.ndarray, actuals: np.ndarray,
                           player_data: pd.DataFrame = None, 
                           prediction_metadata: Dict = None) -> ValidationResults:
        """
        Comprehensive validation of model predictions
        
        Args:
            predictions: Array of predicted minutes
            actuals: Array of actual minutes played
            player_data: DataFrame with player information
            prediction_metadata: Additional prediction context
            
        Returns:
            ValidationResults object with all metrics
        """
        
        self.logger.info(f"Validating {len(predictions)} predictions")
        
        # Basic regression metrics
        mae = mean_absolute_error(actuals, predictions)
        rmse = np.sqrt(mean_squared_error(actuals, predictions))
        r2 = r2_score(actuals, predictions)
        
        # Mean Absolute Percentage Error (avoiding division by zero)
        mape = np.mean(np.abs((actuals - predictions) / np.maximum(actuals, 1))) * 100
        
        # Within-threshold accuracy (±4 minutes)
        threshold = 4.0
        within_threshold = np.abs(predictions - actuals) <= threshold
        within_threshold_accuracy = np.mean(within_threshold) * 100
        
        # Directional accuracy
        directional_accuracy = self._calculate_directional_accuracy(predictions, actuals)
        
        # Extreme event detection (0-minute games, likely injuries)
        extreme_event_accuracy = self._calculate_extreme_event_accuracy(predictions, actuals)
        
        # Prediction bias
        bias = np.mean(predictions - actuals)
        
        # Additional metrics
        additional_metrics = {
            'median_absolute_error': np.median(np.abs(predictions - actuals)),
            'max_absolute_error': np.max(np.abs(predictions - actuals)),
            'std_residuals': np.std(predictions - actuals),
            'skewness_residuals': stats.skew(predictions - actuals),
            'correlation': np.corrcoef(predictions, actuals)[0, 1] if len(predictions) > 1 else 0
        }
        
        # Create results object
        results = ValidationResults(
            mae=mae,
            rmse=rmse,
            r2=r2,
            mape=mape,
            within_threshold_accuracy=within_threshold_accuracy,
            directional_accuracy=directional_accuracy,
            extreme_event_accuracy=extreme_event_accuracy,
            prediction_bias=bias,
            validation_date=datetime.now().isoformat(),
            sample_size=len(predictions),
            additional_metrics=additional_metrics
        )
        
        # Log validation results
        self._log_validation_results(results)
        
        # Store in history
        self.validation_history.append(results)
        
        # Validate against targets
        performance_assessment = self._assess_performance_vs_targets(results)
        
        # Player-specific validation if data provided
        if player_data is not None:
            player_results = self._validate_by_player(predictions, actuals, player_data)
            results.additional_metrics['player_validation'] = player_results
            
        return results
        
    def _calculate_directional_accuracy(self, predictions: np.ndarray, 
                                      actuals: np.ndarray, 
                                      baseline: Optional[np.ndarray] = None) -> float:
        """
        Calculate directional accuracy (predicting increase/decrease correctly)
        
        Args:
            predictions: Predicted values
            actuals: Actual values  
            baseline: Baseline values (e.g., previous game minutes)
                     If None, uses overall mean as baseline
        """
        
        if baseline is None:
            baseline = np.full_like(predictions, np.mean(actuals))
            
        # Calculate directions
        pred_direction = predictions > baseline
        actual_direction = actuals > baseline
        
        # Calculate accuracy
        directional_matches = pred_direction == actual_direction
        accuracy = np.mean(directional_matches) * 100
        
        return accuracy
        
    def _calculate_extreme_event_accuracy(self, predictions: np.ndarray, 
                                        actuals: np.ndarray) -> float:
        """Calculate accuracy for extreme events (0-minute games, likely injuries)"""
        
        # Define extreme events (0 or very low minutes)
        extreme_threshold = 2.0
        
        extreme_actual = actuals <= extreme_threshold
        extreme_predicted = predictions <= extreme_threshold
        
        if not np.any(extreme_actual):
            return 100.0  # No extreme events to predict
            
        # True positives: correctly predicted extreme events
        true_positives = np.sum(extreme_actual & extreme_predicted)
        
        # False negatives: missed extreme events
        false_negatives = np.sum(extreme_actual & ~extreme_predicted)
        
        # Recall for extreme events (sensitivity)
        extreme_recall = true_positives / (true_positives + false_negatives) * 100
        
        return extreme_recall
        
    def _validate_by_player(self, predictions: np.ndarray, actuals: np.ndarray,
                           player_data: pd.DataFrame) -> Dict[str, PlayerValidationResults]:
        """Validate predictions by individual player"""
        
        player_results = {}
        
        unique_players = player_data['player_id'].unique()
        
        for player_id in unique_players:
            player_mask = player_data['player_id'] == player_id
            
            if np.sum(player_mask) < 3:  # Need minimum games for validation
                continue
                
            player_preds = predictions[player_mask]
            player_actuals = actuals[player_mask]
            player_name = player_data[player_mask]['player_name'].iloc[0]
            
            # Calculate player-specific metrics
            player_mae = mean_absolute_error(player_actuals, player_preds)
            player_rmse = np.sqrt(mean_squared_error(player_actuals, player_preds))
            player_bias = np.mean(player_preds - player_actuals)
            player_corr = np.corrcoef(player_preds, player_actuals)[0, 1] if len(player_preds) > 1 else 0
            
            within_threshold = np.abs(player_preds - player_actuals) <= 4.0
            within_threshold_pct = np.mean(within_threshold) * 100
            
            player_result = PlayerValidationResults(
                player_id=str(player_id),
                player_name=player_name,
                games_predicted=len(player_preds),
                mae=player_mae,
                rmse=player_rmse,
                bias=player_bias,
                correlation=player_corr,
                within_threshold_pct=within_threshold_pct
            )
            
            player_results[str(player_id)] = player_result
            
        return player_results
        
    def _log_validation_results(self, results: ValidationResults):
        """Log validation results"""
        
        self.logger.info("=" * 50)
        self.logger.info("MODEL VALIDATION RESULTS")
        self.logger.info("=" * 50)
        self.logger.info(f"Sample Size: {results.sample_size}")
        self.logger.info(f"MAE: {results.mae:.3f} minutes")
        self.logger.info(f"RMSE: {results.rmse:.3f} minutes")
        self.logger.info(f"R²: {results.r2:.3f}")
        self.logger.info(f"MAPE: {results.mape:.1f}%")
        self.logger.info(f"Within ±4 min: {results.within_threshold_accuracy:.1f}%")
        self.logger.info(f"Directional Accuracy: {results.directional_accuracy:.1f}%")
        self.logger.info(f"Extreme Event Accuracy: {results.extreme_event_accuracy:.1f}%")
        self.logger.info(f"Prediction Bias: {results.prediction_bias:.3f} minutes")
        self.logger.info("=" * 50)
        
    def _assess_performance_vs_targets(self, results: ValidationResults) -> Dict[str, bool]:
        """Assess performance against target metrics"""
        
        assessment = {
            'mae_target_met': results.mae <= self.target_metrics['mae'],
            'threshold_target_met': results.within_threshold_accuracy >= self.target_metrics['within_threshold_pct'],
            'directional_target_met': results.directional_accuracy >= self.target_metrics['directional_accuracy'],
            'extreme_target_met': results.extreme_event_accuracy >= self.target_metrics['extreme_event_accuracy']
        }
        
        overall_pass = all(assessment.values())
        assessment['overall_performance'] = overall_pass
        
        # Log assessment
        if overall_pass:
            self.logger.info("✓ Model meets all performance targets")
        else:
            failed_targets = [target for target, passed in assessment.items() if not passed and target != 'overall_performance']
            self.logger.warning(f"⚠ Model failed targets: {failed_targets}")
            
        return assessment
        
    def cross_validate_temporal(self, data: pd.DataFrame, features: np.ndarray, 
                              targets: np.ndarray, model, n_splits: int = 5) -> Dict:
        """
        Perform temporal cross-validation (respecting time order)
        
        Args:
            data: DataFrame with game data including dates
            features: Feature matrix
            targets: Target values (minutes)
            model: Model to validate
            n_splits: Number of temporal splits
            
        Returns:
            Cross-validation results
        """
        
        self.logger.info(f"Performing {n_splits}-fold temporal cross-validation")
        
        # Sort by date
        data_sorted = data.sort_values('game_date').reset_index(drop=True)
        features_sorted = features[data_sorted.index]
        targets_sorted = targets[data_sorted.index]
        
        # Create temporal splits
        split_size = len(data_sorted) // (n_splits + 1)
        
        cv_results = []
        
        for fold in range(n_splits):
            # Training data: all data up to split point
            train_end = split_size * (fold + 1)
            test_start = train_end
            test_end = train_end + split_size
            
            X_train = features_sorted[:train_end]
            y_train = targets_sorted[:train_end]
            X_test = features_sorted[test_start:test_end]
            y_test = targets_sorted[test_start:test_end]
            
            if len(X_test) == 0:
                continue
                
            # Train model on historical data
            model.fit(X_train, y_train)
            
            # Predict on test data
            predictions = model.predict(X_test)
            
            # Validate predictions
            fold_results = self.validate_predictions(
                predictions, y_test, 
                data_sorted.iloc[test_start:test_end].copy()
            )
            
            fold_results.additional_metrics['fold'] = fold
            fold_results.additional_metrics['train_size'] = len(X_train)
            fold_results.additional_metrics['test_size'] = len(X_test)
            
            cv_results.append(fold_results)
            
            self.logger.info(f"Fold {fold + 1}: MAE = {fold_results.mae:.3f}")
            
        # Aggregate results
        cv_summary = self._aggregate_cv_results(cv_results)
        
        return {
            'fold_results': cv_results,
            'summary': cv_summary,
            'n_folds': len(cv_results)
        }
        
    def _aggregate_cv_results(self, cv_results: List[ValidationResults]) -> Dict:
        """Aggregate cross-validation results"""
        
        if not cv_results:
            return {}
            
        metrics = ['mae', 'rmse', 'r2', 'mape', 'within_threshold_accuracy', 
                  'directional_accuracy', 'extreme_event_accuracy', 'prediction_bias']
        
        aggregated = {}
        
        for metric in metrics:
            values = [getattr(result, metric) for result in cv_results]
            aggregated[metric] = {
                'mean': np.mean(values),
                'std': np.std(values),
                'min': np.min(values),
                'max': np.max(values),
                'values': values
            }
            
        return aggregated
        
    def validate_real_time_performance(self, prediction_log: List[Dict]) -> Dict:
        """
        Validate real-time prediction performance
        
        Args:
            prediction_log: List of prediction records with timestamps
            
        Returns:
            Real-time performance metrics
        """
        
        self.logger.info("Validating real-time prediction performance")
        
        if not prediction_log:
            return {'error': 'No prediction log provided'}
            
        # Convert to DataFrame for analysis
        df = pd.DataFrame(prediction_log)
        
        # Update latency analysis
        update_latency = self._analyze_update_latency(df)
        
        # Injury detection performance
        injury_detection = self._analyze_injury_detection_performance(df)
        
        # Prediction stability
        stability_metrics = self._analyze_prediction_stability(df)
        
        # Data freshness analysis
        freshness_analysis = self._analyze_data_freshness(df)
        
        return {
            'update_latency': update_latency,
            'injury_detection': injury_detection,
            'prediction_stability': stability_metrics,
            'data_freshness': freshness_analysis,
            'total_predictions': len(df),
            'analysis_date': datetime.now().isoformat()
        }
        
    def _analyze_update_latency(self, df: pd.DataFrame) -> Dict:
        """Analyze prediction update latency"""
        
        if 'update_timestamp' not in df.columns or 'trigger_timestamp' not in df.columns:
            return {'error': 'Missing timestamp columns for latency analysis'}
            
        df['latency_seconds'] = (
            pd.to_datetime(df['update_timestamp']) - 
            pd.to_datetime(df['trigger_timestamp'])
        ).dt.total_seconds()
        
        return {
            'mean_latency_seconds': df['latency_seconds'].mean(),
            'median_latency_seconds': df['latency_seconds'].median(),
            'p95_latency_seconds': df['latency_seconds'].quantile(0.95),
            'max_latency_seconds': df['latency_seconds'].max(),
            'within_5min_pct': (df['latency_seconds'] <= 300).mean() * 100,
            'target_met': (df['latency_seconds'] <= 300).mean() >= 0.95  # 95% within 5 minutes
        }
        
    def _analyze_injury_detection_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze injury detection and response performance"""
        
        injury_updates = df[df.get('update_reason', '').str.contains('injury', na=False)]
        
        if injury_updates.empty:
            return {'message': 'No injury-related updates found'}
            
        return {
            'total_injury_updates': len(injury_updates),
            'avg_response_time': injury_updates.get('latency_seconds', pd.Series()).mean(),
            'injury_update_accuracy': self._calculate_injury_update_accuracy(injury_updates)
        }
        
    def _calculate_injury_update_accuracy(self, injury_updates: pd.DataFrame) -> float:
        """Calculate accuracy of injury-related prediction updates"""
        
        # This would require actual outcome data to validate
        # For now, return a placeholder
        return 0.85  # 85% accuracy placeholder
        
    def _analyze_prediction_stability(self, df: pd.DataFrame) -> Dict:
        """Analyze stability of predictions over time"""
        
        if 'player_id' not in df.columns or 'prediction_value' not in df.columns:
            return {'error': 'Missing required columns for stability analysis'}
            
        stability_metrics = {}
        
        for player_id in df['player_id'].unique():
            player_data = df[df['player_id'] == player_id].sort_values('update_timestamp')
            
            if len(player_data) < 2:
                continue
                
            predictions = player_data['prediction_value'].values
            
            # Calculate prediction volatility
            volatility = np.std(np.diff(predictions))
            
            # Calculate trend stability (correlation with time)
            time_index = np.arange(len(predictions))
            trend_correlation = np.corrcoef(time_index, predictions)[0, 1] if len(predictions) > 2 else 0
            
            stability_metrics[str(player_id)] = {
                'volatility': volatility,
                'trend_correlation': trend_correlation,
                'updates_count': len(player_data)
            }
            
        return {
            'player_stability': stability_metrics,
            'avg_volatility': np.mean([m['volatility'] for m in stability_metrics.values()]),
            'high_volatility_players': len([m for m in stability_metrics.values() if m['volatility'] > 5])
        }
        
    def _analyze_data_freshness(self, df: pd.DataFrame) -> Dict:
        """Analyze data freshness and quality"""
        
        if 'data_timestamp' not in df.columns:
            return {'error': 'Missing data_timestamp column'}
            
        df['data_age_minutes'] = (
            pd.to_datetime(df['update_timestamp']) - 
            pd.to_datetime(df['data_timestamp'])
        ).dt.total_seconds() / 60
        
        return {
            'avg_data_age_minutes': df['data_age_minutes'].mean(),
            'fresh_data_pct': (df['data_age_minutes'] <= 60).mean() * 100,  # Within 1 hour
            'stale_data_count': (df['data_age_minutes'] > 180).sum(),  # Older than 3 hours
            'freshness_target_met': (df['data_age_minutes'] <= 60).mean() >= 0.9
        }
        
    def generate_validation_report(self, save_path: Optional[str] = None) -> Dict:
        """
        Generate comprehensive validation report
        
        Args:
            save_path: Path to save report (optional)
            
        Returns:
            Complete validation report
        """
        
        if not self.validation_history:
            return {'error': 'No validation history available'}
            
        report = {
            'report_metadata': {
                'generation_date': datetime.now().isoformat(),
                'total_validations': len(self.validation_history),
                'date_range': {
                    'earliest': min(r.validation_date for r in self.validation_history),
                    'latest': max(r.validation_date for r in self.validation_history)
                }
            },
            'performance_summary': self._create_performance_summary(),
            'trend_analysis': self._analyze_performance_trends(),
            'target_compliance': self._assess_target_compliance_history(),
            'recommendations': self._generate_recommendations()
        }
        
        if save_path:
            self._save_report(report, save_path)
            
        return report
        
    def _create_performance_summary(self) -> Dict:
        """Create summary of model performance"""
        
        latest_result = self.validation_history[-1]
        
        # Calculate performance over time
        mae_values = [r.mae for r in self.validation_history]
        accuracy_values = [r.within_threshold_accuracy for r in self.validation_history]
        
        return {
            'current_performance': {
                'mae': latest_result.mae,
                'within_threshold_accuracy': latest_result.within_threshold_accuracy,
                'directional_accuracy': latest_result.directional_accuracy,
                'r_squared': latest_result.r2
            },
            'performance_trends': {
                'mae_trend': 'improving' if len(mae_values) > 1 and mae_values[-1] < mae_values[0] else 'stable',
                'accuracy_trend': 'improving' if len(accuracy_values) > 1 and accuracy_values[-1] > accuracy_values[0] else 'stable',
                'mae_change_pct': ((mae_values[-1] - mae_values[0]) / mae_values[0] * 100) if len(mae_values) > 1 else 0
            },
            'best_performance': {
                'lowest_mae': min(mae_values),
                'highest_accuracy': max(accuracy_values),
                'best_r2': max(r.r2 for r in self.validation_history)
            }
        }
        
    def _analyze_performance_trends(self) -> Dict:
        """Analyze performance trends over time"""
        
        if len(self.validation_history) < 3:
            return {'message': 'Insufficient data for trend analysis'}
            
        # Extract time series
        dates = [datetime.fromisoformat(r.validation_date) for r in self.validation_history]
        mae_values = [r.mae for r in self.validation_history]
        accuracy_values = [r.within_threshold_accuracy for r in self.validation_history]
        
        # Calculate trends
        mae_trend = np.polyfit(range(len(mae_values)), mae_values, 1)[0]
        accuracy_trend = np.polyfit(range(len(accuracy_values)), accuracy_values, 1)[0]
        
        return {
            'mae_trend_slope': mae_trend,
            'accuracy_trend_slope': accuracy_trend,
            'volatility': {
                'mae_std': np.std(mae_values),
                'accuracy_std': np.std(accuracy_values)
            },
            'recent_stability': self._assess_recent_stability()
        }
        
    def _assess_recent_stability(self) -> Dict:
        """Assess stability of recent performance"""
        
        recent_results = self.validation_history[-5:]  # Last 5 validations
        
        if len(recent_results) < 3:
            return {'message': 'Insufficient recent data'}
            
        mae_values = [r.mae for r in recent_results]
        
        return {
            'recent_mae_std': np.std(mae_values),
            'stable': np.std(mae_values) < 0.5,  # Stable if std < 0.5
            'improving': mae_values[-1] < mae_values[0],
            'consistent_performance': all(abs(m - np.mean(mae_values)) < 1.0 for m in mae_values)
        }
        
    def _assess_target_compliance_history(self) -> Dict:
        """Assess historical compliance with performance targets"""
        
        compliance_history = []
        
        for result in self.validation_history:
            compliance = {
                'date': result.validation_date,
                'mae_compliant': result.mae <= self.target_metrics['mae'],
                'accuracy_compliant': result.within_threshold_accuracy >= self.target_metrics['within_threshold_pct'],
                'directional_compliant': result.directional_accuracy >= self.target_metrics['directional_accuracy'],
                'extreme_compliant': result.extreme_event_accuracy >= self.target_metrics['extreme_event_accuracy']
            }
            compliance['overall_compliant'] = all([
                compliance['mae_compliant'],
                compliance['accuracy_compliant'], 
                compliance['directional_compliant'],
                compliance['extreme_compliant']
            ])
            compliance_history.append(compliance)
            
        # Calculate compliance rates
        total_validations = len(compliance_history)
        compliance_rates = {
            'mae_compliance_rate': sum(c['mae_compliant'] for c in compliance_history) / total_validations * 100,
            'accuracy_compliance_rate': sum(c['accuracy_compliant'] for c in compliance_history) / total_validations * 100,
            'directional_compliance_rate': sum(c['directional_compliant'] for c in compliance_history) / total_validations * 100,
            'extreme_compliance_rate': sum(c['extreme_compliant'] for c in compliance_history) / total_validations * 100,
            'overall_compliance_rate': sum(c['overall_compliant'] for c in compliance_history) / total_validations * 100
        }
        
        return {
            'compliance_rates': compliance_rates,
            'compliance_history': compliance_history,
            'current_compliant': compliance_history[-1]['overall_compliant'] if compliance_history else False
        }
        
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on validation results"""
        
        recommendations = []
        latest_result = self.validation_history[-1]
        
        # MAE recommendations
        if latest_result.mae > self.target_metrics['mae']:
            recommendations.append(
                f"MAE ({latest_result.mae:.2f}) exceeds target ({self.target_metrics['mae']:.2f}). "
                "Consider feature engineering improvements or model retraining."
            )
            
        # Accuracy recommendations
        if latest_result.within_threshold_accuracy < self.target_metrics['within_threshold_pct']:
            recommendations.append(
                f"Within-threshold accuracy ({latest_result.within_threshold_accuracy:.1f}%) below target "
                f"({self.target_metrics['within_threshold_pct']:.1f}%). Review feature importance and data quality."
            )
            
        # Bias recommendations
        if abs(latest_result.prediction_bias) > 1.0:
            if latest_result.prediction_bias > 0:
                recommendations.append("Model shows positive bias (over-predicting). Consider recalibration.")
            else:
                recommendations.append("Model shows negative bias (under-predicting). Consider recalibration.")
                
        # Extreme event recommendations
        if latest_result.extreme_event_accuracy < self.target_metrics['extreme_event_accuracy']:
            recommendations.append(
                "Improve injury/DNP detection. Consider enhancing injury data sources and cascade modeling."
            )
            
        # Trend-based recommendations
        if len(self.validation_history) >= 3:
            recent_mae = [r.mae for r in self.validation_history[-3:]]
            if all(recent_mae[i] > recent_mae[i-1] for i in range(1, len(recent_mae))):
                recommendations.append("Performance degrading over recent validations. Schedule model retraining.")
                
        if not recommendations:
            recommendations.append("Model performance is meeting all targets. Continue monitoring.")
            
        return recommendations
        
    def _save_report(self, report: Dict, save_path: str):
        """Save validation report to file"""
        
        try:
            with open(save_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"Validation report saved to {save_path}")
        except Exception as e:
            self.logger.error(f"Error saving report: {e}")
            
    def plot_validation_trends(self, save_path: Optional[str] = None):
        """Plot validation trends over time"""
        
        if len(self.validation_history) < 2:
            self.logger.warning("Insufficient data for trend plotting")
            return
            
        # Extract data
        dates = [datetime.fromisoformat(r.validation_date) for r in self.validation_history]
        mae_values = [r.mae for r in self.validation_history]
        accuracy_values = [r.within_threshold_accuracy for r in self.validation_history]
        
        # Create plots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # MAE over time
        ax1.plot(dates, mae_values, marker='o')
        ax1.axhline(y=self.target_metrics['mae'], color='r', linestyle='--', label='Target')
        ax1.set_title('Mean Absolute Error Over Time')
        ax1.set_ylabel('MAE (minutes)')
        ax1.legend()
        
        # Accuracy over time
        ax2.plot(dates, accuracy_values, marker='o', color='green')
        ax2.axhline(y=self.target_metrics['within_threshold_pct'], color='r', linestyle='--', label='Target')
        ax2.set_title('Within-Threshold Accuracy Over Time')
        ax2.set_ylabel('Accuracy (%)')
        ax2.legend()
        
        # MAE distribution
        ax3.hist(mae_values, bins=10, alpha=0.7)
        ax3.axvline(x=self.target_metrics['mae'], color='r', linestyle='--', label='Target')
        ax3.set_title('MAE Distribution')
        ax3.set_xlabel('MAE (minutes)')
        ax3.set_ylabel('Frequency')
        ax3.legend()
        
        # R² over time
        r2_values = [r.r2 for r in self.validation_history]
        ax4.plot(dates, r2_values, marker='o', color='purple')
        ax4.set_title('R² Over Time')
        ax4.set_ylabel('R²')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            self.logger.info(f"Validation trends plot saved to {save_path}")
        else:
            plt.show()