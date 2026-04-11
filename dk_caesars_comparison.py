# -*- coding: utf-8 -*-
"""
Script to compare DraftKings WNBA prop lines with Caesars simulation probabilities
to identify value betting opportunities.

@author: trent
"""
import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_draftkings_data(filename):
    """Load DraftKings alternate lines data"""
    print(f"📊 Loading DraftKings data from {filename}...")
    
    try:
        dk_df = pd.read_csv(filename)
        print(f"✅ Loaded {len(dk_df)} DraftKings prop lines")
        
        # Clean and standardize column names
        dk_df.columns = dk_df.columns.str.lower()
        
        # Extract stat type from prop_type
        dk_df['stat_type'] = dk_df['prop_type'].str.replace('player_', '').str.replace('_alternate', '')
        
        # Map DraftKings stat types to Caesars stat types
        stat_mapping = {
            'assists': 'Assists',
            'points': 'Points', 
            'rebounds': 'Rebounds',
            'threes': '3-Pointers'
        }
        
        # Apply mapping
        dk_df['stat_type'] = dk_df['stat_type'].map(stat_mapping)
        
        # Filter for Over bets only (since we're comparing to over probabilities)
        dk_df = dk_df[dk_df['bet_type'] == 'Over'].copy()
        
        # Convert odds to implied probability
        dk_df['implied_probability'] = dk_df['odds_price'].apply(odds_to_probability)
        
        print(f"📊 Filtered to {len(dk_df)} Over prop lines")
        return dk_df
        
    except Exception as e:
        print(f"❌ Error loading DraftKings data: {e}")
        return None

def load_caesars_data(filename):
    """Load Caesars simulation probabilities data"""
    print(f"📊 Loading Caesars data from {filename}...")
    
    try:
        caesars_df = pd.read_csv(filename)
        print(f"✅ Loaded {len(caesars_df)} Caesars simulation records")
        
        # Clean column names
        caesars_df.columns = caesars_df.columns.str.strip('"')
        
        # Convert probability columns to numeric
        caesars_df['probability'] = pd.to_numeric(caesars_df['probability'], errors='coerce')
        caesars_df['under_probability'] = pd.to_numeric(caesars_df['under_probability'], errors='coerce')
        
        # Filter out extreme probabilities (100% or 0%)
        caesars_df = caesars_df[
            (caesars_df['probability'] < 0.9999) & 
            (caesars_df['probability'] > 0.0001) &
            (caesars_df['under_probability'] < 0.9999) & 
            (caesars_df['under_probability'] > 0.0001)
        ].copy()
        
        print(f"📊 Filtered to {len(caesars_df)} valid simulation records")
        return caesars_df
        
    except Exception as e:
        print(f"❌ Error loading Caesars data: {e}")
        return None

def odds_to_probability(odds):
    """Convert American odds to implied probability"""
    try:
        odds = float(odds)
        if odds > 0:
            return 100 / (odds + 100)
        else:
            return abs(odds) / (abs(odds) + 100)
    except:
        return np.nan

def find_matching_props(dk_df, caesars_df):
    """Find matching prop lines between DraftKings and Caesars data"""
    print("🔍 Finding matching prop lines...")
    
    matches = []
    
    for _, dk_row in dk_df.iterrows():
        player = dk_row['player_name']
        stat = dk_row['stat_type']
        line = dk_row['line_value']
        
        # Find matching Caesars data where threshold exceeds the line by 0.5 or more
        # For example: DK line 5.5 should match Caesars threshold 6+
        caesars_match = caesars_df[
            (caesars_df['player'] == player) & 
            (caesars_df['stat'] == stat) &
            (caesars_df['threshold'] >= line + 0.5)  # Threshold must exceed line by at least 0.5
        ].copy()
        
        if not caesars_match.empty:
            # Get the closest threshold that exceeds the line
            caesars_match['threshold_diff'] = caesars_match['threshold'] - line
            closest_match = caesars_match.loc[caesars_match['threshold_diff'].idxmin()]
            
            matches.append({
                'player': player,
                'team': closest_match['team'],
                'stat': stat,
                'dk_line': line,
                'caesars_threshold': closest_match['threshold'],
                'threshold_diff': closest_match['threshold_diff'],
                'dk_odds': dk_row['odds_price'],
                'dk_implied_prob': dk_row['implied_probability'],
                'caesars_over_prob': closest_match['probability'],
                'caesars_under_prob': closest_match['under_probability'],
                'caesars_over_odds': closest_match['american_odds'],
                'caesars_under_odds': closest_match['under_american_odds'],
                'over_value': closest_match['probability'] - dk_row['implied_probability'],
                'under_value': closest_match['under_probability'] - (1 - dk_row['implied_probability']),
                'matchup': dk_row['commence_time'],
                'game_time': dk_row['commence_time']
            })
    
    print(f"✅ Found {len(matches)} matching prop lines")
    return pd.DataFrame(matches)

def calculate_value_opportunities(matches_df):
    """Calculate and categorize value opportunities"""
    print("💰 Calculating value opportunities...")
    
    # Define value thresholds
    HIGH_VALUE_THRESHOLD = 0.10  # 10% edge
    MEDIUM_VALUE_THRESHOLD = 0.05  # 5% edge
    
    # Add value categories
    matches_df['over_value_category'] = 'No Value'
    matches_df['under_value_category'] = 'No Value'
    
    # Categorize over value
    matches_df.loc[matches_df['over_value'] >= HIGH_VALUE_THRESHOLD, 'over_value_category'] = 'High Value Over'
    matches_df.loc[(matches_df['over_value'] >= MEDIUM_VALUE_THRESHOLD) & 
                   (matches_df['over_value'] < HIGH_VALUE_THRESHOLD), 'over_value_category'] = 'Medium Value Over'
    
    # Categorize under value (note: for under, we need to consider the implied probability of under)
    # DraftKings implied probability of under = 1 - implied probability of over
    matches_df['dk_implied_under_prob'] = 1 - matches_df['dk_implied_prob']
    matches_df['under_value'] = matches_df['caesars_under_prob'] - matches_df['dk_implied_under_prob']
    
    matches_df.loc[matches_df['under_value'] >= HIGH_VALUE_THRESHOLD, 'under_value_category'] = 'High Value Under'
    matches_df.loc[(matches_df['under_value'] >= MEDIUM_VALUE_THRESHOLD) & 
                   (matches_df['under_value'] < HIGH_VALUE_THRESHOLD), 'under_value_category'] = 'Medium Value Under'
    
    # Overall value category
    matches_df['best_value_side'] = 'No Value'
    
    for idx, row in matches_df.iterrows():
        over_value = row['over_value']
        under_value = row['under_value']
        
        if over_value >= HIGH_VALUE_THRESHOLD and under_value >= HIGH_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'Both Sides High Value'
        elif over_value >= HIGH_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'High Value Over'
        elif under_value >= HIGH_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'High Value Under'
        elif over_value >= MEDIUM_VALUE_THRESHOLD and under_value >= MEDIUM_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'Both Sides Medium Value'
        elif over_value >= MEDIUM_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'Medium Value Over'
        elif under_value >= MEDIUM_VALUE_THRESHOLD:
            matches_df.loc[idx, 'best_value_side'] = 'Medium Value Under'
    
    return matches_df

def generate_reports(matches_df):
    """Generate comprehensive value analysis reports"""
    print("📊 Generating analysis reports...")
    
    # Summary statistics
    total_props = len(matches_df)
    high_value_over = len(matches_df[matches_df['over_value_category'] == 'High Value Over'])
    high_value_under = len(matches_df[matches_df['under_value_category'] == 'High Value Under'])
    medium_value_over = len(matches_df[matches_df['over_value_category'] == 'Medium Value Over'])
    medium_value_under = len(matches_df[matches_df['under_value_category'] == 'Medium Value Under'])
    
    print(f"\n📈 VALUE OPPORTUNITY SUMMARY:")
    print(f"Total Props Analyzed: {total_props}")
    print(f"High Value Over: {high_value_over} ({high_value_over/total_props*100:.1f}%)")
    print(f"High Value Under: {high_value_under} ({high_value_under/total_props*100:.1f}%)")
    print(f"Medium Value Over: {medium_value_over} ({medium_value_over/total_props*100:.1f}%)")
    print(f"Medium Value Under: {medium_value_under} ({medium_value_under/total_props*100:.1f}%)")
    
    # Top value opportunities
    print(f"\n🏆 TOP HIGH VALUE OPPORTUNITIES:")
    high_value = matches_df[
        (matches_df['over_value_category'] == 'High Value Over') | 
        (matches_df['under_value_category'] == 'High Value Under')
    ].copy()
    
    if not high_value.empty:
        high_value['max_value'] = high_value[['over_value', 'under_value']].max(axis=1)
        high_value = high_value.sort_values('max_value', ascending=False).head(10)
        
        for _, row in high_value.iterrows():
            if row['over_value'] > row['under_value']:
                print(f"  {row['player']} ({row['team']}) - {row['stat']} {row['dk_line']}+ OVER")
                print(f"    Caesars: {row['caesars_over_prob']:.1%}, DK: {row['dk_implied_prob']:.1%}")
                print(f"    Edge: {row['over_value']:.1%} | DK Odds: {row['dk_odds']}")
            else:
                print(f"  {row['player']} ({row['team']}) - {row['stat']} {row['dk_line']}+ UNDER")
                print(f"    Caesars: {row['caesars_under_prob']:.1%}, DK: {row['dk_implied_under_prob']:.1%}")
                print(f"    Edge: {row['under_value']:.1%} | DK Odds: {row['dk_odds']}")
            print()
    
    return matches_df

def save_results(matches_df, output_filename=None):
    """Save comparison results to CSV"""
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"dk_caesars_comparison_{timestamp}.csv"
    
    try:
        # Select relevant columns for output
        output_columns = [
            'player', 'team', 'stat', 'dk_line', 'matchup', 'game_time',
            'dk_odds', 'dk_implied_prob', 'dk_implied_under_prob',
            'caesars_threshold', 'threshold_diff',
            'caesars_over_prob', 'caesars_under_prob',
            'caesars_over_odds', 'caesars_under_odds',
            'over_value', 'under_value',
            'over_value_category', 'under_value_category', 'best_value_side'
        ]
        
        output_df = matches_df[output_columns].copy()
        
        # Round numeric columns
        numeric_columns = ['dk_implied_prob', 'dk_implied_under_prob', 'caesars_over_prob', 
                          'caesars_under_prob', 'over_value', 'under_value']
        for col in numeric_columns:
            if col in output_df.columns:
                output_df[col] = output_df[col].round(4)
        
        output_df.to_csv(output_filename, index=False)
        print(f"💾 Results saved to {output_filename}")
        
        return output_filename
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")
        return None

def main():
    """Main execution function"""
    print("🎯 DRAFTKINGS vs CAESARS WNBA PROP COMPARISON")
    print("=" * 50)
    
    # File paths
    dk_file = "draftkings_wnba_alternates_20250826_152252.csv"
    caesars_file = r"C:\Users\trent\postgres caesars_range_outcomes 20250826.csv"
    
    # Check if files exist
    if not os.path.exists(dk_file):
        print(f"❌ DraftKings file not found: {dk_file}")
        return
    
    if not os.path.exists(caesars_file):
        print(f"❌ Caesars file not found: {caesars_file}")
        return
    
    # Load data
    dk_df = load_draftkings_data(dk_file)
    if dk_df is None:
        return
    
    caesars_df = load_caesars_data(caesars_file)
    if caesars_df is None:
        return
    
    # Find matching props
    matches_df = find_matching_props(dk_df, caesars_df)
    if matches_df.empty:
        print("❌ No matching prop lines found")
        return
    
    # Calculate value opportunities
    matches_df = calculate_value_opportunities(matches_df)
    
    # Generate reports
    matches_df = generate_reports(matches_df)
    
    # Save results
    output_file = save_results(matches_df)
    
    print(f"\n✅ Analysis complete! Found {len(matches_df)} matching prop lines")
    print(f"📁 Results saved to: {output_file}")
    
    return matches_df

if __name__ == "__main__":
    main()
