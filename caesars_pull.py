# -*- coding: utf-8 -*-
"""
Created on [Current Date]
Script to pull WNBA player prop over/under lines and odds from Caesars sportbook

@author: trent
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import os
import re
from dotenv import load_dotenv

load_dotenv()  # Load .env variables into environment

def pull_caesars():
    """
    Pull WNBA player prop over/under lines and odds from Caesars sportbook
    
    Returns:
        pd.DataFrame: DataFrame containing player props with lines and odds
    """
    print("📊 Pulling Caesars WNBA player props...")
    
    # Initialize variables
    rows = []
    current_date = datetime.now()
    
    try:
        # Method 1: Try direct Caesars API with enhanced endpoints
        caesars_data = pull_caesars_direct()
        if caesars_data is not None and not caesars_data.empty:
            print("✅ Successfully pulled data from Caesars direct API")
            return caesars_data
            
    except Exception as e:
        print(f"⚠️ Direct Caesars API failed: {e}")
    
    try:
        # Method 2: Try Caesars sportsbook page scraping
        print("🔄 Trying Caesars sportsbook page scraping...")
        caesars_data = pull_caesars_sportsbook()
        if caesars_data is not None and not caesars_data.empty:
            print("✅ Successfully pulled data from Caesars sportsbook pages")
            return caesars_data
            
    except Exception as e:
        print(f"⚠️ Caesars sportsbook scraping failed: {e}")
    
    try:
        # Method 3: Use Oddsshopper API as fallback
        print("🔄 Trying Oddsshopper API as fallback...")
        caesars_data = pull_caesars_oddsshopper()
        if caesars_data is not None and not caesars_data.empty:
            print("✅ Successfully pulled data from Oddsshopper API")
            return caesars_data
            
    except Exception as e:
        print(f"⚠️ Oddsshopper API failed: {e}")
    
    try:
        # Method 4: Try Caesars website scraping as last resort
        print("🔄 Trying Caesars website scraping as last resort...")
        caesars_data = pull_caesars_scraping()
        if caesars_data is not None and not caesars_data.empty:
            print("✅ Successfully pulled data from Caesars website")
            return caesars_data
            
    except Exception as e:
        print(f"⚠️ Caesars website scraping failed: {e}")
    
    print("❌ All methods failed to pull Caesars data")
    return pd.DataFrame()

def pull_caesars_direct():
    """
    Attempt to pull data directly from Caesars API with enhanced endpoints
    """
    try:
        # Enhanced Caesars API endpoints based on URL structure analysis
        base_urls = [
            "https://api.caesars.com/sports/v1/odds",
            "https://api.caesars.com/sports/v1/props",
            "https://api.caesars.com/sports/v1/wnba/props",
            "https://api.caesars.com/sports/v1/basketball/props",
            "https://api.caesars.com/sports/v1/games",
            "https://api.caesars.com/sports/v1/events"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://sportsbook.caesars.com/',
            'Origin': 'https://sportsbook.caesars.com',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        # Try different geographic locations
        locations = ['nj', 'ny', 'pa', 'mi', 'in', 'il', 'co', 'nv', 'az']
        
        for base_url in base_urls:
            try:
                # Try different parameter combinations
                params_list = [
                    {'sport': 'wnba', 'league': 'wnba', 'state': 'nj'},
                    {'sport': 'basketball', 'league': 'wnba', 'state': 'nj'},
                    {'sport': 'wnba', 'state': 'nj'},
                    {'sport': 'basketball', 'state': 'nj'},
                    {'sport': 'wnba'},
                    {'sport': 'basketball'},
                    {'league': 'wnba'},
                    {}
                ]
                
                for params in params_list:
                    try:
                        response = requests.get(base_url, headers=headers, params=params, timeout=15)
                        if response.status_code == 200:
                            data = response.json()
                            print(f"✅ Found working Caesars endpoint: {base_url}")
                            return parse_caesars_response(data)
                    except:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Error with endpoint {base_url}: {e}")
                continue
                
    except Exception as e:
        print(f"⚠️ Direct Caesars API error: {e}")
    
    return None

def pull_caesars_sportsbook():
    """
    Try to pull data from Caesars sportsbook pages directly
    """
    try:
        # Based on the URL structure: https://sportsbook.caesars.com/us/nj/bet/basketball?id=...
        base_url = "https://sportsbook.caesars.com"
        
        # Try different geographic locations and sport combinations
        locations = ['nj', 'ny', 'pa', 'mi', 'in', 'il', 'co', 'nv', 'az']
        sports = ['basketball', 'wnba']
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        for location in locations:
            for sport in sports:
                try:
                    # Try to access the main sportsbook page
                    url = f"{base_url}/us/{location}/bet/{sport}"
                    print(f"🔄 Trying Caesars sportsbook: {url}")
                    
                    response = requests.get(url, headers=headers, timeout=15)
                    if response.status_code == 200:
                        print(f"✅ Successfully accessed Caesars sportsbook: {url}")
                        
                        # Look for WNBA content in the HTML
                        if 'wnba' in response.text.lower() or 'women' in response.text.lower():
                            print("🎯 Found WNBA content on Caesars page")
                            
                            # Try to extract game IDs and access specific game pages
                            game_ids = extract_game_ids(response.text)
                            if game_ids:
                                return pull_game_props(base_url, location, sport, game_ids, headers)
                        
                        # Try to find API endpoints in the page source
                        api_endpoints = extract_api_endpoints(response.text)
                        if api_endpoints:
                            print(f"🔗 Found potential API endpoints: {api_endpoints}")
                            for endpoint in api_endpoints:
                                try:
                                    api_data = requests.get(endpoint, headers=headers, timeout=15).json()
                                    parsed_data = parse_caesars_response(api_data)
                                    if parsed_data is not None and not parsed_data.empty:
                                        return parsed_data
                                except:
                                    continue
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    print(f"⚠️ Error accessing {location}/{sport}: {e}")
                    continue
        
        print("⚠️ No accessible Caesars sportsbook pages found")
        return None
        
    except Exception as e:
        print(f"⚠️ Caesars sportsbook scraping error: {e}")
        return None

def extract_game_ids(html_content):
    """
    Extract game IDs from Caesars HTML content
    """
    try:
        # Look for patterns like: id=fa3dd530-9699-4731-8ff2-6b3df29ae403
        game_id_pattern = r'id=([a-f0-9\-]{36})'
        game_ids = re.findall(game_id_pattern, html_content)
        
        # Also look for other ID patterns
        alt_pattern = r'gameId["\']?\s*:\s*["\']([^"\']+)["\']'
        alt_ids = re.findall(alt_pattern, html_content)
        
        all_ids = list(set(game_ids + alt_ids))
        print(f"🎮 Found {len(all_ids)} potential game IDs")
        return all_ids
        
    except Exception as e:
        print(f"⚠️ Error extracting game IDs: {e}")
        return []

def extract_api_endpoints(html_content):
    """
    Extract potential API endpoints from Caesars HTML content
    """
    try:
        # Look for API endpoints in JavaScript or data attributes
        api_patterns = [
            r'["\'](https?://[^"\']*api[^"\']*)["\']',
            r'["\'](https?://[^"\']*odds[^"\']*)["\']',
            r'["\'](https?://[^"\']*props[^"\']*)["\']',
            r'["\'](https?://[^"\']*games[^"\']*)["\']'
        ]
        
        endpoints = []
        for pattern in api_patterns:
            found = re.findall(pattern, html_content)
            endpoints.extend(found)
        
        # Remove duplicates and filter for Caesars domains
        unique_endpoints = list(set(endpoints))
        caesars_endpoints = [ep for ep in unique_endpoints if 'caesars' in ep.lower()]
        
        print(f"🔗 Found {len(caesars_endpoints)} Caesars API endpoints")
        return caesars_endpoints
        
    except Exception as e:
        print(f"⚠️ Error extracting API endpoints: {e}")
        return []

def pull_game_props(base_url, location, sport, game_ids, headers):
    """
    Pull props from specific game pages
    """
    try:
        rows = []
        
        for game_id in game_ids[:5]:  # Limit to first 5 games
            try:
                game_url = f"{base_url}/us/{location}/bet/{sport}?id={game_id}"
                print(f"🎮 Accessing game: {game_url}")
                
                response = requests.get(game_url, headers=headers, timeout=15)
                if response.status_code == 200:
                    # Look for player props in the game page
                    game_props = extract_player_props(response.text, game_id)
                    if game_props:
                        rows.extend(game_props)
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"⚠️ Error accessing game {game_id}: {e}")
                continue
        
        if rows:
            df = pd.DataFrame(rows)
            print(f"✅ Extracted {len(rows)} props from game pages")
            return df
        
        return None
        
    except Exception as e:
        print(f"⚠️ Error pulling game props: {e}")
        return None

def extract_player_props(html_content, game_id):
    """
    Extract player props from game page HTML
    """
    try:
        rows = []
        
        # Look for player prop patterns in the HTML
        # This would need to be customized based on actual Caesars HTML structure
        
        # Example patterns to look for:
        # - Player names
        # - Prop types (points, rebounds, assists, etc.)
        # - Over/under lines
        # - Odds
        
        # For now, return empty list as placeholder
        print(f"⚠️ Player prop extraction not fully implemented for game {game_id}")
        return []
        
    except Exception as e:
        print(f"⚠️ Error extracting player props: {e}")
        return []

def pull_caesars_oddsshopper():
    """
    Pull Caesars data using Oddsshopper API as fallback
    """
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
            return None
        
        print(f"📊 Found {len(player_props_ids)} player prop offers")
        
        rows = []
        
        # Get current date range
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now() + timedelta(days=7)
        
        for prop_id in player_props_ids:
            try:
                # Build URL for specific prop
                prop_url = f'https://api.oddsshopper.com/api/offers/{prop_id}/outcomes/live'
                params = {
                    'state': 'NJ',  # Changed to New Jersey based on URL
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
                                        'over_under': outcome.get('label', ''),
                                        'line': outcome.get('line', ''),
                                        'americanOdds': outcome.get('americanOdds', ''),
                                        'sportsbook': 'Caesars',
                                        'game_time': game_date
                                    })
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"⚠️ Error processing prop {prop_id}: {e}")
                continue
        
        if not rows:
            print("⚠️ No Caesars data found in Oddsshopper")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Clean and structure data
        df['line'] = pd.to_numeric(df['line'], errors='coerce')
        df['americanOdds'] = pd.to_numeric(df['americanOdds'], errors='coerce')
        
        # Create pivot table for over lines
        over_df = df[df['over_under'] != 'Under'].copy()
        if not over_df.empty:
            over_odds = over_df.pivot_table(
                index=['player', 'date'], 
                columns='offerName', 
                values='line'
            ).reset_index()
            
            # Add additional columns
            over_odds['sportsbook'] = 'Caesars'
            over_odds['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return over_odds
        
        return df
        
    except Exception as e:
        print(f"⚠️ Oddsshopper API error: {e}")
        return None

def pull_caesars_scraping():
    """
    Attempt to scrape Caesars website as last resort
    """
    try:
        print("⚠️ Website scraping not implemented - would require additional dependencies")
        print("💡 Consider using Selenium or Playwright for website scraping")
        return None
        
    except Exception as e:
        print(f"⚠️ Scraping error: {e}")
        return None

def parse_caesars_response(data):
    """
    Parse response from Caesars API
    """
    try:
        rows = []
        
        # This function would need to be customized based on actual Caesars API response structure
        # For now, return empty DataFrame as placeholder
        
        print("⚠️ Caesars API response structure unknown - needs customization")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"⚠️ Error parsing Caesars response: {e}")
        return None

def save_caesars_data(df, filename='caesars_wnba_props.csv'):
    """
    Save Caesars data to CSV file
    """
    try:
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"💾 Saved Caesars data to {filename}")
            return True
        else:
            print("⚠️ No data to save")
            return False
    except Exception as e:
        print(f"❌ Error saving data: {e}")
        return False

def main():
    """
    Main function to run Caesars data pull
    """
    print("🚀 Starting Caesars WNBA player props pull...")
    
    # Pull data
    caesars_data = pull_caesars()
    
    if caesars_data is not None and not caesars_data.empty:
        print(f"✅ Successfully pulled {len(caesars_data)} records from Caesars")
        
        # Save data
        save_caesars_data(caesars_data)
        
        # Display sample
        print("\n📊 Sample of Caesars data:")
        print(caesars_data.head())
        
        return caesars_data
    else:
        print("❌ Failed to pull Caesars data")
        return None

if __name__ == "__main__":
    main()
