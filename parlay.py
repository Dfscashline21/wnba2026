import pandas as pd
import numpy as np
from itertools import combinations
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

class WNBAParlayAnalyzer:
    def __init__(self, data_file: str = None, df: pd.DataFrame = None):
        """
        Initialize the parlay analyzer with either a CSV file or DataFrame
        
        Args:
            data_file: Path to CSV file with prop data
            df: DataFrame with prop data (alternative to file)
        """
        if data_file:
            self.df = pd.read_csv(data_file)
        elif df is not None:
            self.df = df.copy()
        else:
            raise ValueError("Must provide either data_file or df parameter")
            
        # Clean and prepare data
        self._prepare_data()
        
    def _prepare_data(self):
        """Clean and prepare the data for analysis"""
        # Convert odds to numeric, handling string formats
        self.df['dk_odds'] = pd.to_numeric(self.df['dk_odds'], errors='coerce')
        self.df['caesars_over_odds'] = pd.to_numeric(self.df['caesars_over_odds'], errors='coerce')
        self.df['caesars_under_odds'] = pd.to_numeric(self.df['caesars_under_odds'], errors='coerce')
        
        # Convert probabilities to numeric
        prob_columns = ['dk_implied_prob', 'dk_implied_under_prob', 'caesars_over_prob', 
                       'caesars_under_prob', 'over_value', 'under_value']
        for col in prob_columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
            
        # Create unified betting options with clear side identification
        # DK odds are what we're offered, Caesars probs are our true estimates
        self.betting_options = []
        
        for idx, row in self.df.iterrows():
            # Over bets - using DK odds (what we're offered) and Caesars prob (our true estimate)
            if pd.notna(row['dk_odds']) and row['over_value'] > 0:
                self.betting_options.append({
                    'id': f"{idx}_over",
                    'player': row['player'],
                    'team': row['team'],
                    'stat': row['stat'],
                    'line': row['dk_line'],
                    'side': 'over',
                    'matchup': row['matchup'],
                    'game_time': row['game_time'],
                    'odds': row['dk_odds'],  # DK odds - what we're actually offered
                    'true_prob': row['caesars_over_prob'],  # Our true probability estimate
                    'implied_prob': self._american_to_implied_prob(row['dk_odds']),  # Implied prob from DK odds
                    'value': row['over_value'],
                    'value_category': row['over_value_category'],
                    'decimal_odds': self._american_to_decimal(row['dk_odds'])
                })
            
            # Under bets - need to determine if we have under odds in DK data
            # For now, focusing on over bets since that's what your data shows value for
            if pd.notna(row.get('dk_under_odds', np.nan)) and row['under_value'] > 0:
                self.betting_options.append({
                    'id': f"{idx}_under",
                    'player': row['player'],
                    'team': row['team'],
                    'stat': row['stat'],
                    'line': row['dk_line'],
                    'side': 'under',
                    'matchup': row['matchup'],
                    'game_time': row['game_time'],
                    'odds': row.get('dk_under_odds', np.nan),
                    'true_prob': row['caesars_under_prob'],
                    'implied_prob': self._american_to_implied_prob(row.get('dk_under_odds', np.nan)),
                    'value': row['under_value'],
                    'value_category': row['under_value_category'],
                    'decimal_odds': self._american_to_decimal(row.get('dk_under_odds', np.nan))
                })
        
        self.betting_df = pd.DataFrame(self.betting_options)
        print(f"Prepared {len(self.betting_options)} betting options with positive value")
    
    def _american_to_decimal(self, odds: float) -> float:
        """Convert American odds to decimal odds"""
        if pd.isna(odds):
            return np.nan
        if odds > 0:
            return (odds / 100) + 1
        else:
            return (100 / abs(odds)) + 1
    
    def _american_to_implied_prob(self, odds: float) -> float:
        """Convert American odds to implied probability"""
        if pd.isna(odds):
            return np.nan
        if odds > 0:
            return 100 / (odds + 100)
        else:
            return abs(odds) / (abs(odds) + 100)
    
    def calculate_parlay_metrics(self, combination: List[Dict]) -> Dict:
        """Calculate metrics for a parlay combination"""
        if not combination:
            return {}
            
        # Calculate combined probabilities
        true_prob_combined = np.prod([bet['true_prob'] for bet in combination])
        implied_prob_combined = np.prod([bet['implied_prob'] for bet in combination])
        
        # Calculate parlay odds
        parlay_decimal_odds = np.prod([bet['decimal_odds'] for bet in combination])
        parlay_american_odds = self._decimal_to_american(parlay_decimal_odds)
        
        # Calculate expected value (assuming $100 bet)
        bet_amount = 100
        expected_value = (true_prob_combined * (parlay_decimal_odds - 1) * bet_amount) - \
                        ((1 - true_prob_combined) * bet_amount)
        
        # ROI calculation
        roi = (expected_value / bet_amount) * 100
        
        # Edge calculation
        edge = true_prob_combined - implied_prob_combined
        
        # Kelly Criterion
        if parlay_decimal_odds > 1:
            kelly_fraction = ((true_prob_combined * parlay_decimal_odds) - 1) / (parlay_decimal_odds - 1)
        else:
            kelly_fraction = 0
            
        # Risk assessment
        risk_of_ruin = 1 - true_prob_combined
        
        return {
            'combination': combination,
            'legs': len(combination),
            'true_prob_combined': true_prob_combined,
            'implied_prob_combined': implied_prob_combined,
            'parlay_decimal_odds': parlay_decimal_odds,
            'parlay_american_odds': parlay_american_odds,
            'expected_value': expected_value,
            'roi': roi,
            'edge': edge,
            'kelly_fraction': kelly_fraction,
            'risk_of_ruin': risk_of_ruin,
            'potential_payout': bet_amount * parlay_decimal_odds,
            'recommended_bet': max(0, kelly_fraction * 1000)  # Assuming $1000 bankroll
        }
    
    def _decimal_to_american(self, decimal_odds: float) -> float:
        """Convert decimal odds to American odds"""
        if decimal_odds >= 2:
            return (decimal_odds - 1) * 100
        else:
            return -100 / (decimal_odds - 1)
    
    def find_optimal_parlays(self, 
                           min_legs: int = 2,
                           max_legs: int = 6,
                           min_ev: float = 0,
                           min_roi: float = 0,
                           max_risk: float = 0.95,
                           min_value_category: str = None,
                           same_game_only: bool = False,
                           max_combinations: int = 1000) -> List[Dict]:
        """
        Find optimal parlay combinations based on specified criteria
        
        Args:
            min_legs: Minimum number of legs in parlay
            max_legs: Maximum number of legs in parlay
            min_ev: Minimum expected value
            min_roi: Minimum ROI percentage
            max_risk: Maximum risk of ruin (1 - win probability)
            min_value_category: Filter by minimum value category
            same_game_only: Whether to only consider same-game parlays
            max_combinations: Maximum combinations to evaluate (for performance)
        """
        if len(self.betting_options) == 0:
            print("No betting options available")
            return []
        
        # Filter betting options based on criteria
        filtered_options = self.betting_options.copy()
        
        if min_value_category:
            # Handle both "High Value Over" and "High Value" formats
            value_hierarchy = {
                'No Value': 0, 
                'Low Value': 1, 
                'Medium Value': 2, 
                'High Value': 3,
                'Low Value Over': 1,
                'Medium Value Over': 2, 
                'High Value Over': 3
            }
            min_value_level = value_hierarchy.get(min_value_category, 0)
            filtered_options = [opt for opt in filtered_options 
                              if value_hierarchy.get(opt.get('value_category', 'No Value'), 0) >= min_value_level]
        
        print(f"Analyzing {len(filtered_options)} filtered betting options")
        
        all_parlays = []
        combinations_evaluated = 0
        
        # Generate combinations for each leg count
        for leg_count in range(min_legs, min(max_legs + 1, len(filtered_options) + 1)):
            print(f"Generating {leg_count}-leg combinations...")
            
            leg_combinations = list(combinations(filtered_options, leg_count))
            
            # Limit combinations for performance
            if len(leg_combinations) > max_combinations // (max_legs - min_legs + 1):
                leg_combinations = leg_combinations[:max_combinations // (max_legs - min_legs + 1)]
            
            for combo in leg_combinations:
                combinations_evaluated += 1
                
                # Skip if same player/stat combination (avoid conflicting bets)
                players_stats = set()
                valid_combo = True
                games = set()
                
                for bet in combo:
                    player_stat = f"{bet['player']}_{bet['stat']}"
                    if player_stat in players_stats:
                        valid_combo = False
                        break
                    players_stats.add(player_stat)
                    games.add(bet['matchup'])
                
                if not valid_combo:
                    continue
                
                # Same game filter
                if same_game_only and len(games) > 1:
                    continue
                
                # Calculate parlay metrics
                metrics = self.calculate_parlay_metrics(list(combo))
                
                if not metrics:
                    continue
                
                # Apply filters
                if (metrics['expected_value'] >= min_ev and 
                    metrics['roi'] >= min_roi and 
                    metrics['risk_of_ruin'] <= max_risk):
                    
                    all_parlays.append(metrics)
        
        print(f"Evaluated {combinations_evaluated} combinations")
        print(f"Found {len(all_parlays)} profitable parlays")
        
        # Sort by ROI descending
        all_parlays.sort(key=lambda x: x['roi'], reverse=True)
        
        return all_parlays
    
    def generate_parlay_report(self, parlays: List[Dict], top_n: int = 20) -> str:
        """Generate a detailed report of the best parlays"""
        if not parlays:
            return "No profitable parlays found with the given criteria."
        
        report = []
        report.append("=" * 80)
        report.append("WNBA PARLAY ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary statistics
        avg_roi = np.mean([p['roi'] for p in parlays])
        avg_ev = np.mean([p['expected_value'] for p in parlays])
        avg_legs = np.mean([p['legs'] for p in parlays])
        
        report.append("SUMMARY STATISTICS:")
        report.append(f"Total Profitable Parlays Found: {len(parlays)}")
        report.append(f"Average ROI: {avg_roi:.2f}%")
        report.append(f"Average Expected Value: ${avg_ev:.2f}")
        report.append(f"Average Legs: {avg_legs:.1f}")
        report.append("")
        
        # Best parlay
        best = parlays[0]
        report.append("BEST ROI PARLAY:")
        report.append(f"Legs: {best['legs']}")
        report.append(f"ROI: {best['roi']:.2f}%")
        report.append(f"Expected Value: ${best['expected_value']:.2f}")
        report.append(f"Win Probability: {best['true_prob_combined']*100:.2f}%")
        report.append(f"Parlay Odds: {best['parlay_american_odds']:+.0f}")
        report.append(f"Recommended Bet: ${best['recommended_bet']:.2f}")
        report.append("")
        
        # Top parlays
        report.append(f"TOP {min(top_n, len(parlays))} PARLAYS:")
        report.append("-" * 80)
        
        for i, parlay in enumerate(parlays[:top_n], 1):
            report.append(f"#{i} - {parlay['legs']}-Leg Parlay:")
            
            # List the bets in this parlay
            for bet in parlay['combination']:
                report.append(f"  • {bet['player']} ({bet['team']}) {bet['stat']} {bet['side']} {bet['line']} ({bet['odds']:+})")
            
            report.append(f"  ROI: {parlay['roi']:.2f}% | EV: ${parlay['expected_value']:.2f} | " +
                         f"Win%: {parlay['true_prob_combined']*100:.2f}% | " +
                         f"Odds: {parlay['parlay_american_odds']:+.0f}")
            report.append(f"  Recommended Bet: ${parlay['recommended_bet']:.2f} | " +
                         f"Potential Payout: ${parlay['potential_payout']:.2f}")
            report.append("")
        
        return "\n".join(report)
    
    def export_results(self, parlays: List[Dict], filename: str = "wnba_parlay_results.csv"):
        """Export parlay results to CSV"""
        if not parlays:
            print("No parlays to export")
            return
        
        export_data = []
        for i, parlay in enumerate(parlays, 1):
            bet_details = " + ".join([f"{bet['player']} {bet['stat']} {bet['side']} {bet['line']}" 
                                    for bet in parlay['combination']])
            
            export_data.append({
                'rank': i,
                'legs': parlay['legs'],
                'parlay_details': bet_details,
                'roi_percent': round(parlay['roi'], 2),
                'expected_value': round(parlay['expected_value'], 2),
                'win_probability_percent': round(parlay['true_prob_combined'] * 100, 2),
                'parlay_odds': round(parlay['parlay_american_odds']),
                'recommended_bet': round(parlay['recommended_bet'], 2),
                'potential_payout': round(parlay['potential_payout'], 2),
                'edge': round(parlay['edge'], 4),
                'kelly_fraction': round(parlay['kelly_fraction'], 4)
            })
        
        export_df = pd.DataFrame(export_data)
        export_df.to_csv(filename, index=False)
        print(f"Results exported to {filename}")

# Example usage and testing function
def run_analysis_example():
    """Example of how to use the analyzer with sample data"""
    
    # Create sample data matching your column structure
    # DK odds are what you're offered, Caesars probs are your true estimates
    sample_data = {
        'player': ['A\'ja Wilson', 'Breanna Stewart', 'Sabrina Ionescu', 'Alyssa Thomas'],
        'team': ['LVA', 'NYL', 'NYL', 'CON'],
        'stat': ['Rebounds', 'Points', 'Assists', 'Points'],
        'dk_line': [7.5, 18.5, 6.5, 14.5],
        'matchup': ['LVA vs NYL', 'NYL vs LVA', 'NYL vs LVA', 'CON vs PHX'],
        'game_time': ['2025-08-22T02:00:00Z'] * 4,
        'dk_odds': [-110, +120, -150, +110],  # DK odds - what you're offered
        'dk_implied_prob': [0.52, 0.45, 0.60, 0.48],
        'dk_implied_under_prob': [0.48, 0.55, 0.40, 0.52],
        'caesars_threshold': [8, 19, 7, 15],
        'threshold_diff': [0.5, 0.5, 0.5, 0.5],
        'caesars_over_prob': [0.75, 0.70, 0.80, 0.65],  # Your true probability estimates
        'caesars_under_prob': [0.25, 0.30, 0.20, 0.35],
        'caesars_over_odds': [120, 110, 130, 115],  # Not used for betting - just reference
        'caesars_under_odds': [-120, -110, -130, -115],
        'over_value': [0.23, 0.25, 0.20, 0.17],  # Significant positive value
        'under_value': [-0.23, -0.25, -0.20, -0.17],
        'over_value_category': ['High Value'] * 4,
        'under_value_category': ['No Value'] * 4,
        'best_value_side': ['Over'] * 4
    }
    
    df = pd.DataFrame(sample_data)
    
    # Initialize analyzer
    analyzer = WNBAParlayAnalyzer(df=df)
    
    # Find optimal parlays
    parlays = analyzer.find_optimal_parlays(
        min_legs=2,
        max_legs=4,
        min_ev=0,
        min_roi=5,
        max_risk=0.85,
        min_value_category='Medium Value'
    )
    
    # Generate report
    report = analyzer.generate_parlay_report(parlays, top_n=10)
    print(report)
    
    # Export results
    if parlays:
        analyzer.export_results(parlays, 'sample_parlay_results.csv')
    
    return analyzer, parlays

if __name__ == "__main__":
    # Run example
    print("Running WNBA Parlay Analysis Example...")
    analyzer, results = run_analysis_example()
    print(f"\nAnalysis complete! Found {len(results)} profitable parlay combinations.")