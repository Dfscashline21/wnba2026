# -*- coding: utf-8 -*-
"""
Created on [Current Date]
Script to pull WNBA player prop over/under lines and odds from BetOnline sportbook

@author: trent
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

def pull_betonline():
    """
    Pull WNBA player prop over/under lines and odds from BetOnline sportbook using Oddsshopper API
    
    Returns:
        pd.DataFrame: DataFrame containing player props with lines and odds
    """
    print("📊 Pulling BetOnline WNBA player props from Oddsshopper...")
    
    try:
        # Get available offers from Oddsshopper
        offers_url = 'https://www.oddsshopper.com/api/liveOdds/offers?league=WNBA'
        response = requests.get(offers_url, timeout=15)
        offers_data = response.json()
        
        # Extract player props IDs
        player_props_ids = []
        if 'offerCategories' in offers_data:
            for category in offers_data['offerCategories']:
                if category.get('name') == 'PlayerProps':
                    for offer in category.get('offers', []):
                        player_props_ids.append(offer.get('id'))
        
        if not player_props_ids:
            print("⚠️ No player props found in Oddsshopper")
            return pd.DataFrame()
        
        print(f"📊 Found {len(player_props_ids)} player prop offers")
        
        rows = []
        
        # Get current date range for WNBA season
        current_date = datetime.now()
        start_date = current_date - timedelta(days=7)
        end_date = current_date + timedelta(days=7)
        
        for prop_id in player_props_ids:
            try:
                # Build URL for specific prop
                prop_url = f'https://api.oddsshopper.com/api/offers/{prop_id}/outcomes/live'
                params = {
                    'state': 'NV',  # Nevada for testing
                    'seasonStartYear': '2024',
                    'startDate': start_date.isoformat() + 'Z',
                    'endDate': end_date.isoformat() + 'Z',
                    'selectedSportsbooks': '',
                    'sortBy': 'Time',
                    'oddsExplore': 'false'
                }
                
                response = requests.get(prop_url, params=params, timeout=15)
                prop_data = response.json()
                
                # Process prop data
                for game_data in prop_data:
                    if 'participants' in game_data and 'sides' in game_data:
                        player_name = game_data['participants'][0].get('name', 'Unknown')
                        offer_name = game_data.get('offerName', 'Unknown')
                        game_date = game_data.get('startDate', '')
                        
                        # Extract BetOnline odds
                        for side in game_data.get('sides', []):
                            for outcome in side.get('outcomes', []):
                                if outcome.get('sportsbookCode') == 'BetOnline':
                                    rows.append({
                                        'date': datetime.fromisoformat(game_date).strftime('%Y-%m-%d') if game_date else 'Unknown',
                                        'player': player_name,
                                        'offerName': offer_name,
                                        'over_under': outcome.get('label', ''),
                                        'line': outcome.get('line', ''),
                                        'americanOdds': outcome.get('americanOdds', ''),
                                        'sportsbook': 'BetOnline',
                                        'game_time': game_date
                                    })
                
                # Rate limiting to be respectful to API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"⚠️ Error processing prop {prop_id}: {e}")
                continue
        
        if not rows:
            print("⚠️ No BetOnline data found in Oddsshopper")
            return pd.DataFrame()
        
        # Create DataFrame
        propdf2 = pd.DataFrame(rows)
        
        # Clean and structure data
        propdf2['line'] = pd.to_numeric(propdf2['line'], errors='coerce')
        propdf2['americanOdds'] = pd.to_numeric(propdf2['americanOdds'], errors='coerce')
        
        print(f"✅ Successfully retrieved {len(propdf2)} BetOnline records")
        return propdf2
        
    except Exception as e:
        print(f"❌ Error in pull_betonline: {e}")
        return pd.DataFrame()

def save_betonline_data(df, filename='betonline_wnba_props.csv'):
    """
    Save BetOnline data to CSV file
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Output filename
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not df.empty:
            # Create timestamp for filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename_with_timestamp = f"betonline_wnba_props_{timestamp}.csv"
            
            # Save to CSV
            df.to_csv(filename_with_timestamp, index=False)
            print(f"💾 Saved BetOnline data to {filename_with_timestamp}")
            
            # Also save without timestamp for easy access
            df.to_csv(filename, index=False)
            print(f"💾 Saved BetOnline data to {filename}")
            
            return True
        else:
            print("⚠️ No data to save")
            return False
    except Exception as e:
        print(f"❌ Error saving data: {e}")
        return False

def create_pivot_table(df):
    """
    Create pivot table for over lines only
    
    Args:
        df (pd.DataFrame): Raw data DataFrame
    
    Returns:
        pd.DataFrame: Pivot table with over lines
    """
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Filter for over lines only
        overdf = df[df['over_under'] != 'Under'].copy()
        
        if not overdf.empty:
            # Create pivot table
            overodds = overdf.pivot_table(
                index=['player', 'date'], 
                columns='offerName', 
                values='line'
            ).reset_index()
            
            # Add additional metadata
            overodds['sportsbook'] = 'BetOnline'
            overodds['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"📊 Created pivot table with {len(overodds)} over lines")
            return overodds
        else:
            print("⚠️ No over lines found for pivot table")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error creating pivot table: {e}")
        return pd.DataFrame()

def main():
    """
    Main function to run BetOnline data pull and export
    """
    print("🚀 Starting BetOnline WNBA player props pull...")
    print("=" * 60)
    
    # Pull data
    betonline_data = pull_betonline()
    
    if betonline_data is not None and not betonline_data.empty:
        print(f"✅ Successfully pulled {len(betonline_data)} records from BetOnline")
        
        # Display sample of raw data
        print("\n📋 Sample of raw BetOnline data:")
        print(betonline_data.head())
        
        # Create pivot table
        print("\n🔄 Creating pivot table...")
        pivot_data = create_pivot_table(betonline_data)
        
        if not pivot_data.empty:
            print("\n📊 Sample of pivot table:")
            print(pivot_data.head())
        
        # Save raw data to CSV
        print("\n💾 Saving data to CSV files...")
        save_success = save_betonline_data(betonline_data)
        
        if save_success:
            print("\n🎉 BetOnline data pull completed successfully!")
            print(f"📁 Files saved in current directory")
            print(f"📊 Total records: {len(betonline_data)}")
            
            # Display summary statistics
            if not betonline_data.empty:
                print(f"\n📈 Summary Statistics:")
                print(f"   - Unique players: {betonline_data['player'].nunique()}")
                print(f"   - Prop types: {betonline_data['offerName'].unique()}")
                print(f"   - Date range: {betonline_data['date'].min()} to {betonline_data['date'].max()}")
        else:
            print("❌ Failed to save data to CSV")
            
        return betonline_data
    else:
        print("❌ Failed to pull BetOnline data")
        return None

if __name__ == "__main__":
    main()


