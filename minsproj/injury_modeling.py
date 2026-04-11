"""
WNBA Injury Impact Modeling System
Comprehensive injury classification, cascade effect modeling, and return-from-injury protocols
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass, field
from enum import Enum
import json
import pickle
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

class InjuryType(Enum):
    """Injury type classifications"""
    ANKLE = "ankle"
    KNEE = "knee"
    BACK = "back"
    HAMSTRING = "hamstring"
    SHOULDER = "shoulder"
    CONCUSSION = "concussion"
    WRIST = "wrist"
    HIP = "hip"
    FOOT = "foot"
    CALF = "calf"
    QUAD = "quad"
    OTHER = "other"

class InjurySeverity(Enum):
    """Injury severity levels"""
    MINOR = "minor"        # 1-3 games
    MODERATE = "moderate"  # 4-14 games  
    MAJOR = "major"        # 15+ games
    SEASON_ENDING = "season_ending"

class BodyPartCategory(Enum):
    """Body part categories for injury analysis"""
    LOWER_BODY = "lower_body"
    UPPER_BODY = "upper_body"
    HEAD = "head"
    CORE = "core"
    OTHER = "other"

@dataclass
class InjuryRecord:
    """Individual injury record structure"""
    player_id: str
    player_name: str
    injury_type: InjuryType
    severity: InjurySeverity
    body_part: BodyPartCategory
    injury_date: datetime
    expected_return_date: Optional[datetime] = None
    actual_return_date: Optional[datetime] = None
    games_missed: int = 0
    is_recurring: bool = False
    previous_same_injury: Optional[datetime] = None
    age_at_injury: Optional[float] = None
    minutes_per_game_before: Optional[float] = None
    performance_impact: Dict[str, float] = field(default_factory=dict)
    recovery_notes: str = ""

class InjuryDatabase:
    """Database for storing and querying injury history"""
    
    def __init__(self):
        self.injuries: List[InjuryRecord] = []
        self.player_profiles: Dict[str, Dict] = {}
        self.recovery_patterns: Dict[str, Dict] = {}
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('InjuryDatabase')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def add_injury(self, injury: InjuryRecord):
        """Add injury record to database"""
        # Check for recurring injuries
        self._check_recurring_injury(injury)
        
        self.injuries.append(injury)
        self._update_player_profile(injury)
        
        self.logger.info(f"Added injury record: {injury.player_name} - {injury.injury_type.value}")
        
    def _check_recurring_injury(self, injury: InjuryRecord):
        """Check if injury is recurring"""
        player_injuries = [inj for inj in self.injuries 
                          if inj.player_id == injury.player_id 
                          and inj.injury_type == injury.injury_type]
        
        if player_injuries:
            # Find most recent same injury
            latest_injury = max(player_injuries, key=lambda x: x.injury_date)
            days_between = (injury.injury_date - latest_injury.injury_date).days
            
            # Consider recurring if within 365 days
            if days_between <= 365:
                injury.is_recurring = True
                injury.previous_same_injury = latest_injury.injury_date
                
    def _update_player_profile(self, injury: InjuryRecord):
        """Update player injury profile"""
        player_id = injury.player_id
        
        if player_id not in self.player_profiles:
            self.player_profiles[player_id] = {
                'total_injuries': 0,
                'games_missed_total': 0,
                'injury_types': {},
                'body_parts_affected': {},
                'recurring_injuries': 0,
                'injury_prone_score': 0.0
            }
            
        profile = self.player_profiles[player_id]
        profile['total_injuries'] += 1
        profile['games_missed_total'] += injury.games_missed
        
        # Track injury types
        injury_type = injury.injury_type.value
        if injury_type not in profile['injury_types']:
            profile['injury_types'][injury_type] = 0
        profile['injury_types'][injury_type] += 1
        
        # Track body parts
        body_part = injury.body_part.value
        if body_part not in profile['body_parts_affected']:
            profile['body_parts_affected'][body_part] = 0
        profile['body_parts_affected'][body_part] += 1
        
        if injury.is_recurring:
            profile['recurring_injuries'] += 1
            
        # Calculate injury prone score
        profile['injury_prone_score'] = self._calculate_injury_prone_score(profile)
        
    def _calculate_injury_prone_score(self, profile: Dict) -> float:
        """Calculate injury proneness score (0-1, higher = more prone)"""
        total_injuries = profile['total_injuries']
        games_missed = profile['games_missed_total']
        recurring = profile['recurring_injuries']
        
        # Base score from injury frequency
        base_score = min(total_injuries / 10.0, 0.5)  # Cap at 0.5
        
        # Add games missed component
        games_component = min(games_missed / 100.0, 0.3)  # Cap at 0.3
        
        # Add recurring injury penalty
        recurring_component = min(recurring / 5.0, 0.2)  # Cap at 0.2
        
        return min(base_score + games_component + recurring_component, 1.0)
        
    def get_player_injury_history(self, player_id: str) -> List[InjuryRecord]:
        """Get injury history for specific player"""
        return [inj for inj in self.injuries if inj.player_id == player_id]
        
    def get_injury_patterns(self, injury_type: InjuryType = None, 
                           body_part: BodyPartCategory = None) -> Dict:
        """Analyze injury patterns and recovery times"""
        filtered_injuries = self.injuries
        
        if injury_type:
            filtered_injuries = [inj for inj in filtered_injuries if inj.injury_type == injury_type]
        if body_part:
            filtered_injuries = [inj for inj in filtered_injuries if inj.body_part == body_part]
            
        if not filtered_injuries:
            return {}
            
        # Calculate recovery statistics
        recovery_times = [inj.games_missed for inj in filtered_injuries if inj.games_missed > 0]
        
        return {
            'total_injuries': len(filtered_injuries),
            'avg_recovery_games': np.mean(recovery_times) if recovery_times else 0,
            'median_recovery_games': np.median(recovery_times) if recovery_times else 0,
            'std_recovery_games': np.std(recovery_times) if recovery_times else 0,
            'recurring_rate': sum(1 for inj in filtered_injuries if inj.is_recurring) / len(filtered_injuries),
            'severity_distribution': self._get_severity_distribution(filtered_injuries)
        }
        
    def _get_severity_distribution(self, injuries: List[InjuryRecord]) -> Dict:
        """Get distribution of injury severities"""
        severity_counts = {}
        for injury in injuries:
            severity = injury.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
        total = len(injuries)
        return {severity: count/total for severity, count in severity_counts.items()}

class InjuryClassificationSystem:
    """System for classifying injuries and predicting recovery patterns"""
    
    def __init__(self, injury_db: InjuryDatabase):
        self.injury_db = injury_db
        self.classification_rules = {}
        self.recovery_models = {}
        self.logger = self._setup_logger()
        self._initialize_classification_rules()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('InjuryClassificationSystem')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def _initialize_classification_rules(self):
        """Initialize injury classification rules"""
        self.classification_rules = {
            'ankle': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 3, 'moderate': 8, 'major': 20},
                'recurrence_risk': 0.25,
                'age_factor': 1.2,  # Older players take longer
                'position_impact': {'guard': 1.1, 'forward': 1.0, 'center': 0.9}
            },
            'knee': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 5, 'moderate': 12, 'major': 35},
                'recurrence_risk': 0.35,
                'age_factor': 1.4,
                'position_impact': {'guard': 1.2, 'forward': 1.1, 'center': 1.0}
            },
            'back': {
                'body_part': BodyPartCategory.CORE,
                'typical_recovery': {'minor': 4, 'moderate': 10, 'major': 25},
                'recurrence_risk': 0.4,
                'age_factor': 1.3,
                'position_impact': {'guard': 0.9, 'forward': 1.1, 'center': 1.2}
            },
            'concussion': {
                'body_part': BodyPartCategory.HEAD,
                'typical_recovery': {'minor': 7, 'moderate': 14, 'major': 30},
                'recurrence_risk': 0.2,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.0, 'forward': 1.0, 'center': 1.0}
            },
            'hamstring': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 2, 'moderate': 6, 'major': 15},
                'recurrence_risk': 0.3,
                'age_factor': 1.2,
                'position_impact': {'guard': 1.1, 'forward': 1.0, 'center': 0.9}
            },
            'shoulder': {
                'body_part': BodyPartCategory.UPPER_BODY,
                'typical_recovery': {'minor': 3, 'moderate': 10, 'major': 25},
                'recurrence_risk': 0.2,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.0, 'forward': 1.1, 'center': 1.2}
            },
            'wrist': {
                'body_part': BodyPartCategory.UPPER_BODY,
                'typical_recovery': {'minor': 2, 'moderate': 6, 'major': 15},
                'recurrence_risk': 0.15,
                'age_factor': 1.0,
                'position_impact': {'guard': 1.1, 'forward': 1.0, 'center': 0.9}
            },
            'hip': {
                'body_part': BodyPartCategory.CORE,
                'typical_recovery': {'minor': 4, 'moderate': 12, 'major': 30},
                'recurrence_risk': 0.25,
                'age_factor': 1.3,
                'position_impact': {'guard': 1.0, 'forward': 1.1, 'center': 1.0}
            },
            'foot': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 3, 'moderate': 8, 'major': 20},
                'recurrence_risk': 0.2,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.0, 'forward': 1.0, 'center': 1.0}
            },
            'calf': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 2, 'moderate': 5, 'major': 12},
                'recurrence_risk': 0.25,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.0, 'forward': 1.0, 'center': 0.9}
            },
            'quad': {
                'body_part': BodyPartCategory.LOWER_BODY,
                'typical_recovery': {'minor': 3, 'moderate': 7, 'major': 18},
                'recurrence_risk': 0.2,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.1, 'forward': 1.0, 'center': 0.9}
            },
            'other': {
                'body_part': BodyPartCategory.OTHER,
                'typical_recovery': {'minor': 3, 'moderate': 8, 'major': 20},
                'recurrence_risk': 0.2,
                'age_factor': 1.1,
                'position_impact': {'guard': 1.0, 'forward': 1.0, 'center': 1.0}
            }
        }
        
    def classify_injury(self, injury_description: str, player_data: Dict) -> Dict:
        """
        Classify injury and predict recovery timeline
        
        Args:
            injury_description: Text description of injury
            player_data: Player information (age, position, injury history)
            
        Returns:
            Classification results and recovery prediction
        """
        # Parse injury description
        injury_type = self._parse_injury_type(injury_description)
        severity = self._assess_injury_severity(injury_description, player_data)
        
        # Get classification rules
        rules = self.classification_rules.get(injury_type.value, {})
        
        # Predict recovery timeline
        recovery_prediction = self._predict_recovery_timeline(
            injury_type, severity, player_data, rules)
            
        # Calculate impact on minutes
        minutes_impact = self._calculate_minutes_impact(
            injury_type, severity, player_data, recovery_prediction)
            
        return {
            'injury_type': injury_type,
            'severity': severity,
            'body_part': rules.get('body_part', BodyPartCategory.OTHER),
            'recovery_prediction': recovery_prediction,
            'minutes_impact': minutes_impact,
            'recurrence_risk': rules.get('recurrence_risk', 0.2),
            'classification_confidence': self._calculate_confidence(injury_description)
        }
        
    def _parse_injury_type(self, description: str) -> InjuryType:
        """Parse injury type from description"""
        description_lower = description.lower()
        
        type_keywords = {
            InjuryType.ANKLE: ['ankle', 'sprain'],
            InjuryType.KNEE: ['knee', 'acl', 'mcl', 'meniscus'],
            InjuryType.BACK: ['back', 'spine', 'disc'],
            InjuryType.HAMSTRING: ['hamstring', 'hammy'],
            InjuryType.SHOULDER: ['shoulder', 'rotator'],
            InjuryType.CONCUSSION: ['concussion', 'head', 'protocol'],
            InjuryType.WRIST: ['wrist', 'hand'],
            InjuryType.HIP: ['hip', 'groin'],
            InjuryType.FOOT: ['foot', 'plantar', 'toe'],
            InjuryType.CALF: ['calf', 'strain'],
            InjuryType.QUAD: ['quad', 'thigh']
        }
        
        for injury_type, keywords in type_keywords.items():
            if any(keyword in description_lower for keyword in keywords):
                return injury_type
                
        return InjuryType.OTHER
        
    def _assess_injury_severity(self, description: str, player_data: Dict) -> InjurySeverity:
        """Assess injury severity from description and context"""
        description_lower = description.lower()
        
        # Severe indicators
        severe_keywords = ['surgery', 'torn', 'fracture', 'season', 'months']
        if any(keyword in description_lower for keyword in severe_keywords):
            return InjurySeverity.MAJOR
            
        # Moderate indicators
        moderate_keywords = ['strain', 'sprain', 'weeks', 'mri']
        if any(keyword in description_lower for keyword in moderate_keywords):
            return InjurySeverity.MODERATE
            
        # Minor indicators
        minor_keywords = ['minor', 'day-to-day', 'precautionary']
        if any(keyword in description_lower for keyword in minor_keywords):
            return InjurySeverity.MINOR
            
        # Default based on player age and history
        age = player_data.get('age', 25)
        injury_history = player_data.get('injury_prone_score', 0)
        
        if age > 30 or injury_history > 0.6:
            return InjurySeverity.MODERATE
        else:
            return InjurySeverity.MINOR
            
    def _predict_recovery_timeline(self, injury_type: InjuryType, severity: InjurySeverity,
                                 player_data: Dict, rules: Dict) -> Dict:
        """Predict injury recovery timeline"""
        base_recovery = rules.get('typical_recovery', {}).get(severity.value, 7)
        
        # Adjust for player factors
        age = player_data.get('age', 25)
        position = player_data.get('position', 'guard').lower()
        injury_history = player_data.get('injury_prone_score', 0)
        
        # Age adjustment
        age_factor = rules.get('age_factor', 1.0)
        if age > 30:
            age_adjustment = 1 + ((age - 30) / 30) * (age_factor - 1)
        else:
            age_adjustment = 1.0
            
        # Position adjustment
        position_factors = rules.get('position_impact', {})
        position_adjustment = position_factors.get(position, 1.0)
        
        # Injury history adjustment
        history_adjustment = 1 + (injury_history * 0.3)  # Up to 30% longer
        
        # Calculate adjusted timeline
        adjusted_recovery = base_recovery * age_adjustment * position_adjustment * history_adjustment
        
        return {
            'expected_games_missed': int(round(adjusted_recovery)),
            'optimistic_timeline': int(round(adjusted_recovery * 0.7)),
            'pessimistic_timeline': int(round(adjusted_recovery * 1.5)),
            'return_date_estimate': datetime.now() + timedelta(days=adjusted_recovery * 2.5),  # Assuming ~2.5 days per game
            'adjustment_factors': {
                'age_adjustment': age_adjustment,
                'position_adjustment': position_adjustment,
                'history_adjustment': history_adjustment
            }
        }
        
    def _calculate_minutes_impact(self, injury_type: InjuryType, severity: InjurySeverity,
                                player_data: Dict, recovery_prediction: Dict) -> Dict:
        """Calculate impact on playing minutes during recovery"""
        
        # Base impact by severity
        severity_impact = {
            InjurySeverity.MINOR: {'immediate': 0.8, 'return_phase': 0.9, 'full_recovery': 1.0},
            InjurySeverity.MODERATE: {'immediate': 0.0, 'return_phase': 0.7, 'full_recovery': 0.95},
            InjurySeverity.MAJOR: {'immediate': 0.0, 'return_phase': 0.5, 'full_recovery': 0.9},
            InjurySeverity.SEASON_ENDING: {'immediate': 0.0, 'return_phase': 0.0, 'full_recovery': 0.0}
        }
        
        impact_multipliers = severity_impact[severity]
        
        # Return progression timeline
        games_missed = recovery_prediction['expected_games_missed']
        progression_games = max(5, games_missed // 3)  # Gradual return phase
        
        return {
            'immediate_impact': impact_multipliers['immediate'],
            'games_completely_missed': games_missed,
            'return_phase_games': progression_games,
            'return_phase_multiplier': impact_multipliers['return_phase'],
            'full_recovery_multiplier': impact_multipliers['full_recovery'],
            'total_impact_games': games_missed + progression_games,
            'reinjury_risk_games': progression_games * 2  # Higher risk period
        }
        
    def _calculate_confidence(self, description: str) -> float:
        """Calculate confidence in injury classification"""
        # Simple heuristic based on description detail
        word_count = len(description.split())
        detail_keywords = ['mri', 'diagnosed', 'confirmed', 'doctor']
        detail_score = sum(1 for keyword in detail_keywords if keyword.lower() in description.lower())
        
        base_confidence = 0.6
        detail_bonus = min(detail_score * 0.1, 0.3)
        length_bonus = min(word_count / 50, 0.1)
        
        return min(base_confidence + detail_bonus + length_bonus, 1.0)

class CascadeEffectModeler:
    """Models how injuries affect team dynamics and other players' minutes"""
    
    def __init__(self):
        self.team_structures = {}
        self.position_dependencies = {}
        self.logger = self._setup_logger()
        self._initialize_cascade_rules()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('CascadeEffectModeler')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def _initialize_cascade_rules(self):
        """Initialize rules for cascade effects"""
        
        # Position substitution patterns
        self.position_dependencies = {
            'PG': {'primary_backup': ['PG'], 'secondary_backup': ['SG', 'SF']},
            'SG': {'primary_backup': ['SG'], 'secondary_backup': ['PG', 'SF']},
            'SF': {'primary_backup': ['SF'], 'secondary_backup': ['SG', 'PF']},
            'PF': {'primary_backup': ['PF'], 'secondary_backup': ['SF', 'C']},
            'C': {'primary_backup': ['C'], 'secondary_backup': ['PF']}
        }
        
        # Minute redistribution patterns
        self.redistribution_patterns = {
            'star_player': {
                'usage_redistribution': 0.8,  # 80% of minutes redistributed
                'primary_beneficiaries': 0.6,  # 60% to primary position
                'secondary_beneficiaries': 0.4  # 40% to other positions
            },
            'starter': {
                'usage_redistribution': 0.9,
                'primary_beneficiaries': 0.7,
                'secondary_beneficiaries': 0.3
            },
            'role_player': {
                'usage_redistribution': 1.0,
                'primary_beneficiaries': 0.8,
                'secondary_beneficiaries': 0.2
            }
        }
        
    def calculate_cascade_effects(self, injured_player: Dict, team_roster: List[Dict],
                                historical_data: pd.DataFrame = None) -> Dict:
        """
        Calculate cascade effects of player injury on team
        
        Args:
            injured_player: Information about injured player
            team_roster: List of team players with positions and roles
            historical_data: Historical minute distribution data
            
        Returns:
            Dictionary of cascade effects and minute redistributions
        """
        
        player_role = self._classify_player_role(injured_player, team_roster)
        available_minutes = injured_player.get('avg_minutes', 0)
        player_position = injured_player.get('position', 'SF')
        
        # Get redistribution pattern
        redistribution = self.redistribution_patterns.get(player_role, 
                                                         self.redistribution_patterns['role_player'])
        
        # Calculate minute redistribution
        minutes_to_redistribute = available_minutes * redistribution['usage_redistribution']
        
        # Find beneficiaries
        beneficiaries = self._identify_beneficiaries(
            injured_player, team_roster, player_position)
        
        # Distribute minutes
        minute_changes = self._distribute_minutes(
            minutes_to_redistribute, beneficiaries, redistribution)
        
        # Calculate team impact
        team_impact = self._calculate_team_impact(
            injured_player, minute_changes, team_roster)
        
        return {
            'injured_player': injured_player['name'],
            'player_role': player_role,
            'available_minutes': available_minutes,
            'minutes_redistributed': minutes_to_redistribute,
            'beneficiaries': minute_changes,
            'team_impact': team_impact,
            'rotation_changes': self._suggest_rotation_changes(beneficiaries, minute_changes)
        }
        
    def _classify_player_role(self, player: Dict, roster: List[Dict]) -> str:
        """Classify player role within team context"""
        avg_minutes = player.get('avg_minutes', 0)
        usage_rate = player.get('usage_rate', 0)
        is_starter = player.get('is_starter', False)
        
        # Sort roster by minutes to determine relative importance
        roster_sorted = sorted(roster, key=lambda x: x.get('avg_minutes', 0), reverse=True)
        player_rank = next((i for i, p in enumerate(roster_sorted) 
                          if p['name'] == player['name']), len(roster_sorted))
        
        if player_rank <= 2 and avg_minutes > 25:  # Top 3 players with high minutes
            return 'star_player'
        elif is_starter or avg_minutes > 18:
            return 'starter'
        else:
            return 'role_player'
            
    def _identify_beneficiaries(self, injured_player: Dict, roster: List[Dict], 
                              position: str) -> Dict:
        """Identify players who will benefit from injury"""
        
        # Get position substitution pattern
        position_deps = self.position_dependencies.get(position, 
                                                     {'primary_backup': [position], 
                                                      'secondary_backup': []})
        
        primary_candidates = []
        secondary_candidates = []
        
        for player in roster:
            if player['name'] == injured_player['name']:
                continue
                
            player_pos = player.get('position', 'SF')
            
            if player_pos in position_deps['primary_backup']:
                primary_candidates.append(player)
            elif player_pos in position_deps['secondary_backup']:
                secondary_candidates.append(player)
                
        # Sort by current minutes (lower minutes = more likely to benefit)
        primary_candidates.sort(key=lambda x: x.get('avg_minutes', 0))
        secondary_candidates.sort(key=lambda x: x.get('avg_minutes', 0))
        
        return {
            'primary': primary_candidates[:3],  # Top 3 primary candidates
            'secondary': secondary_candidates[:2]  # Top 2 secondary candidates
        }
        
    def _distribute_minutes(self, total_minutes: float, beneficiaries: Dict, 
                          redistribution: Dict) -> Dict:
        """Distribute available minutes among beneficiaries"""
        
        primary_minutes = total_minutes * redistribution['primary_beneficiaries']
        secondary_minutes = total_minutes * redistribution['secondary_beneficiaries']
        
        minute_changes = {}
        
        # Distribute to primary beneficiaries
        if beneficiaries['primary']:
            minutes_per_primary = primary_minutes / len(beneficiaries['primary'])
            for player in beneficiaries['primary']:
                player_name = player['name']
                # Adjust based on current role and capacity
                capacity_factor = self._calculate_capacity_factor(player)
                actual_increase = minutes_per_primary * capacity_factor
                
                minute_changes[player_name] = {
                    'minutes_increase': actual_increase,
                    'new_projected_minutes': player.get('avg_minutes', 0) + actual_increase,
                    'role_change': self._assess_role_change(player, actual_increase),
                    'capacity_factor': capacity_factor
                }
                
        # Distribute to secondary beneficiaries  
        if beneficiaries['secondary']:
            minutes_per_secondary = secondary_minutes / len(beneficiaries['secondary'])
            for player in beneficiaries['secondary']:
                player_name = player['name']
                capacity_factor = self._calculate_capacity_factor(player)
                actual_increase = minutes_per_secondary * capacity_factor
                
                minute_changes[player_name] = {
                    'minutes_increase': actual_increase,
                    'new_projected_minutes': player.get('avg_minutes', 0) + actual_increase,
                    'role_change': self._assess_role_change(player, actual_increase),
                    'capacity_factor': capacity_factor
                }
                
        return minute_changes
        
    def _calculate_capacity_factor(self, player: Dict) -> float:
        """Calculate how much additional minutes a player can realistically handle"""
        current_minutes = player.get('avg_minutes', 0)
        age = player.get('age', 25)
        injury_history = player.get('injury_prone_score', 0)
        
        # Base capacity based on current minutes
        if current_minutes < 15:
            base_capacity = 1.5  # Can significantly increase
        elif current_minutes < 25:
            base_capacity = 1.2  # Can moderately increase
        elif current_minutes < 35:
            base_capacity = 1.0  # Limited increase
        else:
            base_capacity = 0.8  # Already playing heavy minutes
            
        # Age adjustment
        age_factor = 1.0 if age < 30 else 0.9
        
        # Injury history adjustment
        injury_factor = 1.0 - (injury_history * 0.2)
        
        return base_capacity * age_factor * injury_factor
        
    def _assess_role_change(self, player: Dict, minute_increase: float) -> str:
        """Assess how player's role changes with minute increase"""
        current_minutes = player.get('avg_minutes', 0)
        new_minutes = current_minutes + minute_increase
        
        if current_minutes < 15 and new_minutes > 20:
            return 'bench_to_rotation'
        elif current_minutes < 20 and new_minutes > 25:
            return 'rotation_to_starter'
        elif current_minutes < 25 and new_minutes > 30:
            return 'starter_to_featured'
        elif minute_increase > 5:
            return 'expanded_role'
        else:
            return 'minimal_change'
            
    def _calculate_team_impact(self, injured_player: Dict, minute_changes: Dict,
                             roster: List[Dict]) -> Dict:
        """Calculate overall team impact of injury"""
        
        total_minute_change = sum(change['minutes_increase'] for change in minute_changes.values())
        affected_players = len(minute_changes)
        
        # Calculate depth impact
        remaining_depth = len([p for p in roster if p['name'] != injured_player['name']])
        depth_impact = 'high' if remaining_depth < 8 else 'medium' if remaining_depth < 10 else 'low'
        
        # Calculate role disruption
        injured_role = self._classify_player_role(injured_player, roster)
        role_disruption = {
            'star_player': 'severe',
            'starter': 'moderate', 
            'role_player': 'minimal'
        }.get(injured_role, 'minimal')
        
        return {
            'total_minute_redistribution': total_minute_change,
            'players_affected': affected_players,
            'depth_impact': depth_impact,
            'role_disruption': role_disruption,
            'rotation_flexibility': self._assess_rotation_flexibility(minute_changes),
            'fatigue_risk': self._calculate_fatigue_risk(minute_changes, roster)
        }
        
    def _suggest_rotation_changes(self, beneficiaries: Dict, minute_changes: Dict) -> List[str]:
        """Suggest specific rotation adjustments"""
        suggestions = []
        
        for player_name, changes in minute_changes.items():
            role_change = changes['role_change']
            minutes_increase = changes['minutes_increase']
            
            if role_change == 'bench_to_rotation':
                suggestions.append(f"Move {player_name} into regular rotation (+{minutes_increase:.1f} min)")
            elif role_change == 'rotation_to_starter':
                suggestions.append(f"Consider {player_name} for starting lineup (+{minutes_increase:.1f} min)")
            elif role_change == 'expanded_role':
                suggestions.append(f"Expand {player_name}'s role significantly (+{minutes_increase:.1f} min)")
            elif minutes_increase > 3:
                suggestions.append(f"Increase {player_name}'s minutes by {minutes_increase:.1f}")
                
        return suggestions
        
    def _assess_rotation_flexibility(self, minute_changes: Dict) -> str:
        """Assess how flexible the rotation remains"""
        max_increase = max((change['minutes_increase'] for change in minute_changes.values()), default=0)
        avg_increase = np.mean([change['minutes_increase'] for change in minute_changes.values()]) if minute_changes else 0
        
        if max_increase > 10 or avg_increase > 6:
            return 'low'  # Heavy reliance on few players
        elif max_increase > 6 or avg_increase > 4:
            return 'medium'
        else:
            return 'high'
            
    def _calculate_fatigue_risk(self, minute_changes: Dict, roster: List[Dict]) -> str:
        """Calculate increased fatigue risk for team"""
        
        high_minute_players = 0
        for player_name, changes in minute_changes.items():
            new_minutes = changes['new_projected_minutes']
            if new_minutes > 32:
                high_minute_players += 1
                
        total_roster_size = len(roster)
        
        if high_minute_players / total_roster_size > 0.4:
            return 'high'
        elif high_minute_players / total_roster_size > 0.2:
            return 'medium'
        else:
            return 'low'

class ReturnFromInjuryProtocol:
    """Manages return-from-injury protocols and minute restrictions"""
    
    def __init__(self, injury_db: InjuryDatabase):
        self.injury_db = injury_db
        self.return_protocols = {}
        self.logger = self._setup_logger()
        self._initialize_protocols()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('ReturnFromInjuryProtocol')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    def _initialize_protocols(self):
        """Initialize return-from-injury protocols"""
        
        self.return_protocols = {
            InjuryType.KNEE: {
                'initial_restriction': 0.6,  # 60% of normal minutes
                'progression_rate': 0.1,     # 10% increase per game
                'monitoring_games': 10,      # Games of close monitoring
                'reinjury_risk_period': 15,  # High risk period
                'load_management': True
            },
            InjuryType.ANKLE: {
                'initial_restriction': 0.7,
                'progression_rate': 0.15,
                'monitoring_games': 6,
                'reinjury_risk_period': 10,
                'load_management': False
            },
            InjuryType.BACK: {
                'initial_restriction': 0.5,
                'progression_rate': 0.08,
                'monitoring_games': 12,
                'reinjury_risk_period': 20,
                'load_management': True
            },
            InjuryType.CONCUSSION: {
                'initial_restriction': 0.8,
                'progression_rate': 0.05,
                'monitoring_games': 8,
                'reinjury_risk_period': 25,
                'load_management': True
            }
        }
        
    def create_return_plan(self, injury_record: InjuryRecord, 
                          baseline_minutes: float) -> Dict:
        """
        Create comprehensive return-from-injury plan
        
        Args:
            injury_record: The injury being recovered from
            baseline_minutes: Player's typical minutes before injury
            
        Returns:
            Detailed return plan with minute restrictions and monitoring
        """
        
        protocol = self.return_protocols.get(
            injury_record.injury_type, 
            self.return_protocols[InjuryType.ANKLE]  # Default protocol
        )
        
        # Calculate return timeline
        return_plan = {
            'injury_details': {
                'type': injury_record.injury_type.value,
                'severity': injury_record.severity.value,
                'games_missed': injury_record.games_missed,
                'return_date': injury_record.actual_return_date or datetime.now()
            },
            'minute_restrictions': self._calculate_minute_progression(
                baseline_minutes, protocol),
            'monitoring_schedule': self._create_monitoring_schedule(protocol),
            'performance_expectations': self._set_performance_expectations(
                injury_record, baseline_minutes),
            'reinjury_prevention': self._create_prevention_plan(injury_record, protocol),
            'load_management': self._create_load_management_plan(
                injury_record, protocol, baseline_minutes)
        }
        
        return return_plan
        
    def _calculate_minute_progression(self, baseline_minutes: float, 
                                    protocol: Dict) -> List[Dict]:
        """Calculate minute progression schedule"""
        
        initial_restriction = protocol['initial_restriction']
        progression_rate = protocol['progression_rate']
        monitoring_games = protocol['monitoring_games']
        
        progression_schedule = []
        
        current_multiplier = initial_restriction
        
        for game in range(1, monitoring_games + 1):
            restricted_minutes = baseline_minutes * current_multiplier
            
            progression_schedule.append({
                'game_number': game,
                'minute_restriction': current_multiplier,
                'target_minutes': min(restricted_minutes, baseline_minutes),
                'max_minutes': min(restricted_minutes * 1.1, baseline_minutes),  # 10% buffer
                'recommendations': self._get_game_recommendations(game, current_multiplier)
            })
            
            # Increase for next game (but cap at 100%)
            current_multiplier = min(current_multiplier + progression_rate, 1.0)
            
        return progression_schedule
        
    def _get_game_recommendations(self, game_number: int, restriction: float) -> List[str]:
        """Get recommendations for specific game in return"""
        recommendations = []
        
        if game_number <= 3:
            recommendations.extend([
                "Monitor closely for fatigue",
                "Limit consecutive minutes",
                "Extra rest between quarters"
            ])
            
        if restriction < 0.8:
            recommendations.extend([
                "Avoid back-to-back games if possible",
                "Reduce practice intensity"
            ])
            
        if game_number <= 5:
            recommendations.append("Daily check-ins with medical staff")
            
        return recommendations
        
    def _create_monitoring_schedule(self, protocol: Dict) -> Dict:
        """Create monitoring and assessment schedule"""
        
        monitoring_games = protocol['monitoring_games']
        
        return {
            'daily_assessments': {
                'games': monitoring_games,
                'metrics': [
                    'pain_level',
                    'mobility_rating', 
                    'confidence_level',
                    'fatigue_level'
                ]
            },
            'performance_tracking': {
                'games': monitoring_games,
                'metrics': [
                    'minutes_played',
                    'movement_efficiency',
                    'shot_selection',
                    'defensive_intensity'
                ]
            },
            'medical_checkpoints': [
                {'game': 3, 'assessment': 'initial_return_evaluation'},
                {'game': 7, 'assessment': 'mid_return_review'},
                {'game': monitoring_games, 'assessment': 'full_clearance_evaluation'}
            ]
        }
        
    def _set_performance_expectations(self, injury_record: InjuryRecord, 
                                    baseline_minutes: float) -> Dict:
        """Set realistic performance expectations during return"""
        
        # Historical analysis of similar injuries
        similar_injuries = [
            inj for inj in self.injury_db.injuries
            if inj.injury_type == injury_record.injury_type
            and inj.severity == injury_record.severity
            and inj.actual_return_date is not None
        ]
        
        if similar_injuries:
            # Calculate average performance decline in return period
            avg_performance_decline = np.mean([
                inj.performance_impact.get('return_period_decline', 0.1)
                for inj in similar_injuries
                if 'return_period_decline' in inj.performance_impact
            ])
        else:
            # Default expectation based on injury severity
            severity_decline = {
                InjurySeverity.MINOR: 0.05,
                InjurySeverity.MODERATE: 0.15,
                InjurySeverity.MAJOR: 0.25
            }.get(injury_record.severity, 0.15)
            avg_performance_decline = severity_decline
            
        return {
            'initial_performance_level': 1.0 - avg_performance_decline,
            'expected_full_recovery_games': max(10, injury_record.games_missed // 2),
            'performance_metrics': {
                'shooting_accuracy': 1.0 - (avg_performance_decline * 0.5),
                'defensive_intensity': 1.0 - (avg_performance_decline * 0.8),
                'movement_fluidity': 1.0 - avg_performance_decline,
                'confidence_level': 1.0 - (avg_performance_decline * 1.2)
            },
            'milestone_targets': self._create_performance_milestones(
                baseline_minutes, avg_performance_decline)
        }
        
    def _create_performance_milestones(self, baseline_minutes: float, 
                                     performance_decline: float) -> List[Dict]:
        """Create performance milestone targets"""
        
        return [
            {
                'game': 3,
                'target': 'Complete 3 games without reaggravation',
                'minute_target': baseline_minutes * 0.7,
                'performance_target': 1.0 - performance_decline
            },
            {
                'game': 7,
                'target': 'Return to 80% of baseline minutes',
                'minute_target': baseline_minutes * 0.8,
                'performance_target': 1.0 - (performance_decline * 0.5)
            },
            {
                'game': 12,
                'target': 'Full minute load clearance',
                'minute_target': baseline_minutes,
                'performance_target': 1.0 - (performance_decline * 0.2)
            }
        ]
        
    def _create_prevention_plan(self, injury_record: InjuryRecord, 
                              protocol: Dict) -> Dict:
        """Create reinjury prevention plan"""
        
        return {
            'high_risk_period_games': protocol['reinjury_risk_period'],
            'prevention_strategies': [
                'Enhanced warm-up routine',
                'Targeted strengthening exercises', 
                'Movement pattern correction',
                'Load monitoring'
            ],
            'risk_factors_monitoring': [
                'Fatigue accumulation',
                'Movement compensation patterns',
                'Confidence levels',
                'External stressors'
            ],
            'contingency_plans': {
                'early_warning_signs': [
                    'Reduce minutes by 25%',
                    'Additional medical evaluation',
                    'Modified practice participation'
                ],
                'minor_reaggravation': [
                    'Immediate removal from game',
                    'Return to previous restriction level',
                    'Extended monitoring period'
                ]
            }
        }
        
    def _create_load_management_plan(self, injury_record: InjuryRecord,
                                   protocol: Dict, baseline_minutes: float) -> Optional[Dict]:
        """Create load management plan if needed"""
        
        if not protocol.get('load_management', False):
            return None
            
        return {
            'back_to_back_policy': 'rest_second_game',
            'consecutive_game_limit': 3,
            'weekly_minute_cap': baseline_minutes * 5,  # 5 games worth
            'practice_modifications': [
                'Limited contact drills',
                'Reduced scrimmage time',
                'Focus on skill work'
            ],
            'rest_triggers': [
                'Fatigue score > 7/10',
                'Performance drop > 20%',
                'Any pain recurrence'
            ]
        }