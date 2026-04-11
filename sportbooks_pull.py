# -*- coding: utf-8 -*-
"""
Created on Sat Apr 19 13:41:05 2025

@author: trent
"""
import requests
import pandas as pd
from datetime import date as dt, datetime, timedelta
import numpy as np
import time


def pull_mgm():
    cate = 'https://www.oddsshopper.com/api/liveOdds/offers?league=WNBA'
    gories = requests.get(cate).json()
    print(gories)
    
    player_props_ids = [
        offer["id"]
        for category in gories["offerCategories"]
        if category["name"] == "PlayerProps"
        for offer in category["offers"]
    ]
    
    rows = []
    
    
    url ='https://api.oddsshopper.com/api/offers/f535d358-c0b6-431a-a595-4980317a3c09/outcomes/live?state=NV&seasonStartYear=2024&startDate=2025-01-31T08%3A00%3A00.000Z&endDate=2025-12-31T07%3A59%3A59.999Z&selectedSportsbooks=&sortBy=Time&oddsExplore=false&edgeSportsbooks=Circa%2CFanDuel%2CPinnacle'
    
    test = requests.get(url)
    tester = test.json()
    
    for propid in player_props_ids:
        try:
            url ='https://api.oddsshopper.com/api/offers/'+propid+'/outcomes/live?state=NV&seasonStartYear=2024&startDate=2025-01-01T08%3A00%3A00.000Z&endDate=2025-12-28T07%3A59%3A59.999Z&selectedSportsbooks=&sortBy=Time&oddsExplore=false&edgeSportsbooks=Circa%2CFanDuel%2CPinnacle'
            
            test = requests.get(url)
            tester = test.json()
        
            
            
            for i in range(len(tester)):
                data =tester[i]
                    
                for side in data['sides']:
                    for outcome in side['outcomes']:
                        if outcome['sportsbookCode'] == 'BetMGM':  # Filter by BetMGM
                            rows.append({
                                'date': datetime.fromisoformat(data['startDate']).strftime('%Y-%m-%d'),
                                'player': data['participants'][0]['name'],
                                'offerName': data['offerName'],
                                'over/under': outcome['label'],
                                'line': outcome['line'],
                                'americanOdds': outcome['americanOdds'],
                                'sportsbook': outcome['sportsbookCode']
                            })
        except:
            pass
    
    # Create DataFrame
    propdf2 = pd.DataFrame(rows)
    
    
    
    overdf = propdf2[propdf2['over/under']!='Under']
    
    overdf['line'] = overdf['line'].astype(float)
    
        
    overodds = overdf.pivot_table(index=['player','date'],columns = 'offerName', values='line').reset_index()   
    
    return overodds




def pull_caesars():
    """
    Pull WNBA player prop over/under lines and odds from Caesars sportbook using Oddsshopper API
    
    Returns:
        pd.DataFrame: DataFrame containing player props with lines and odds
    """
    print("📊 Pulling Caesars WNBA player props from Oddsshopper...")
    
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
                        
                        # Extract Caesars odds
                        for side in game_data.get('sides', []):
                            for outcome in side.get('outcomes', []):
                                if outcome.get('sportsbookCode') == 'Caesars':
                                    rows.append({
                                        'date': datetime.fromisoformat(game_date).strftime('%Y-%m-%d') if game_date else 'Unknown',
                                        'player': player_name,
                                        'offerName': offer_name,
                                        'over/under': outcome.get('label', ''),
                                        'line': outcome.get('line', ''),
                                        'americanOdds': outcome.get('americanOdds', ''),
                                        'sportsbook': 'Caesars',
                                        'game_time': game_date
                                    })
                
                # Rate limiting to be respectful to API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"⚠️ Error processing prop {prop_id}: {e}")
                continue
        
        if not rows:
            print("⚠️ No Caesars data found in Oddsshopper")
            return pd.DataFrame()
        
        # Create DataFrame
        propdf2 = pd.DataFrame(rows)
        
        # Clean and structure data
        propdf2['line'] = pd.to_numeric(propdf2['line'], errors='coerce')
        propdf2['americanOdds'] = pd.to_numeric(propdf2['americanOdds'], errors='coerce')
        
        # Create pivot table for over lines
        overdf = propdf2[propdf2['over/under'] != 'Under'].copy()
        if not overdf.empty:
            overodds = overdf.pivot_table(
                index=['player', 'date'], 
                columns='offerName', 
                values='line'
            ).reset_index()
            
            # Add additional metadata
            overodds['sportsbook'] = 'Caesars'
            overodds['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return overodds
        
        return propdf2
        
    except Exception as e:
        print(f"❌ Error in pull_caesars: {e}")
        return pd.DataFrame()


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
                                        'over/under': outcome.get('label', ''),
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
        
        # Create pivot table for over lines
        overdf = propdf2[propdf2['over/under'] != 'Under'].copy()
        if not overdf.empty:
            overodds = overdf.pivot_table(
                index=['player', 'date'], 
                columns='offerName', 
                values='line'
            ).reset_index()
            
            # Add additional metadata
            overodds['sportsbook'] = 'BetOnline'
            overodds['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return overodds
        
        return propdf2
        
    except Exception as e:
        print(f"❌ Error in pull_betonline: {e}")
        return pd.DataFrame()
