#!/usr/bin/env python3
"""
WNBA Player Props Alternates API Script
Pulls WNBA player prop alternate lines using The Odds API

Requirements:
- requests library: pip install requests
- Valid API key from The Odds API (https://the-odds-api.com/)
"""

import requests
import json
import csv
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

class WNBAPlayerPropsAPI:
    def __init__(self, api_key: str):
        """
        Initialize the WNBA Player Props API client
        
        Args:
            api_key (str): Your API key from The Odds API
        """
        self.api_key = api_key
        self.base_url = "https://api.the-odds-api.com/v4"
        self.sport = "basketball_wnba"
        self.session = requests.Session()
        
        # Common headers for all requests
        self.session.headers.update({
            'User-Agent': 'WNBA-Props-Script/1.0'
        })
    
    def get_events(self) -> List[Dict[str, Any]]:
        """
        Get all current WNBA events/games
        
        Returns:
            List of WNBA events with event IDs
        """
        url = f"{self.base_url}/sports/{self.sport}/events"
        params = {
            'apiKey': self.api_key
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            events = response.json()
            
            print(f"Found {len(events)} WNBA events")
            return events
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching events: {e}")
            return []
    
    def get_player_props_markets(self) -> List[str]:
        """
        Get available player props markets for WNBA focusing on alternates
        Uses the exact alternate market names available in The Odds API
        
        Returns:
            List of market keys for player props alternates
        """
        # Exact alternate market names from The Odds API
        alternate_markets = [
            'player_points_alternate',
            'player_rebounds_alternate',
            'player_assists_alternate'
        ]
        
        # Also include standard markets to catch any additional alternates
        standard_markets = [
            'player_points',
            'player_rebounds', 
            'player_assists',
            'player_threes',
            'player_steals',
            'player_blocks',
            'player_turnovers'
        ]
        
        # Additional alternate variations that might exist
        other_alternates = [
            'player_threes_alternate',
            'player_steals_alternate',
            'player_blocks_alternate',
            'player_turnovers_alternate'
        ]
        
        # Combination markets
        combo_markets = [
            'player_points_rebounds_assists',
            'player_points_rebounds',
            'player_points_assists',
            'player_rebounds_assists'
        ]
        
        # Prioritize the known alternates first
        return alternate_markets + standard_markets + other_alternates + combo_markets
    
    def get_event_odds(self, event_id: str, markets: List[str] = None, 
                      regions: str = "us", odds_format: str = "american", 
                      bookmakers: str = "draftkings") -> Dict[str, Any]:
        """
        Get odds for a specific WNBA event including player props alternates from DraftKings
        
        Args:
            event_id (str): The event ID from get_events()
            markets (List[str]): List of markets to fetch (defaults to player props)
            regions (str): Bookmaker regions (us, uk, eu, au)
            odds_format (str): Odds format (american, decimal)
            bookmakers (str): Specific bookmaker to target (default: draftkings)
            
        Returns:
            Event odds data including player props alternates from DraftKings
        """
        if markets is None:
            markets = self.get_player_props_markets()
        
        url = f"{self.base_url}/sports/{self.sport}/events/{event_id}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': regions,
            'markets': ','.join(markets),
            'oddsFormat': odds_format,
            'bookmakers': bookmakers
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching odds for event {event_id}: {e}")
            return {}
    
    def extract_draftkings_alternate_props(self, event_data: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Extract and organize alternate player props specifically from DraftKings
        
        Args:
            event_data: Raw event odds data from API
            
        Returns:
            Dictionary organized by player and prop type with DraftKings alternate lines only
        """
        alternates = {}
        
        if not event_data or 'bookmakers' not in event_data:
            return alternates
        
        for bookmaker in event_data['bookmakers']:
            # Only process DraftKings bookmaker
            if bookmaker['key'].lower() != 'draftkings':
                continue
                
            bookmaker_name = bookmaker['title']
            print(f"  Processing {bookmaker_name} markets...")
            
            for market in bookmaker.get('markets', []):
                market_key = market['key']
                print(f"    Found market: {market_key}")
                
                # Focus on the specific alternate markets and any player props
                if self._is_target_alternate_market(market_key):
                    print(f"    ✓ Processing alternate market: {market_key}")
                    
                    for outcome in market.get('outcomes', []):
                        player_name = outcome.get('description', 'Unknown Player')
                        prop_type = market_key
                        
                        # Initialize player structure if not exists
                        if player_name not in alternates:
                            alternates[player_name] = {}
                        
                        if prop_type not in alternates[player_name]:
                            alternates[player_name][prop_type] = []
                        
                        # Add the alternate line from DraftKings
                        alternates[player_name][prop_type].append({
                            'bookmaker': bookmaker_name,
                            'line': outcome.get('point'),
                            'over_price': outcome.get('price') if outcome.get('name') == 'Over' else None,
                            'under_price': outcome.get('price') if outcome.get('name') == 'Under' else None,
                            'name': outcome.get('name'),
                            'last_update': market.get('last_update')
                        })
                else:
                    print(f"    - Skipping non-alternate market: {market_key}")
        
        return alternates
    
    def _is_target_alternate_market(self, market_key: str) -> bool:
        """
        Check if market key matches our target alternate markets
        
        Args:
            market_key: Market key from API response
            
        Returns:
            True if this is one of our target alternate markets
        """
        target_alternates = [
            'player_points_alternate',
            'player_rebounds_alternate', 
            'player_assists_alternate',
            'player_threes_alternate',
            'player_steals_alternate',
            'player_blocks_alternate',
            'player_turnovers_alternate'
        ]
        
        # Also include any market with 'alternate' in the name
        return market_key in target_alternates or 'alternate' in market_key.lower()
    
    def debug_available_markets(self, event_id: str) -> None:
        """
        Debug function to see what markets are actually available for an event
        
        Args:
            event_id: Event ID to check
        """
        print(f"\n=== DEBUGGING MARKETS FOR EVENT {event_id} ===")
        
        # Get all available markets without filtering
        url = f"{self.base_url}/sports/{self.sport}/events/{event_id}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': 'us',
            'bookmakers': 'draftkings'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            event_data = response.json()
            
            if 'bookmakers' in event_data:
                for bookmaker in event_data['bookmakers']:
                    if bookmaker['key'].lower() == 'draftkings':
                        print(f"DraftKings markets available:")
                        for market in bookmaker.get('markets', []):
                            market_key = market['key']
                            outcome_count = len(market.get('outcomes', []))
                            print(f"  - {market_key} ({outcome_count} outcomes)")
                            
                            # Show sample outcomes for alternate markets
                            if 'alternate' in market_key.lower():
                                for outcome in market.get('outcomes', [])[:3]:  # First 3
                                    player = outcome.get('description', 'N/A')
                                    line = outcome.get('point', 'N/A')
                                    name = outcome.get('name', 'N/A')
                                    print(f"    {name} {player} {line}")
            else:
                print("No bookmaker data found")
                
        except requests.exceptions.RequestException as e:
            print(f"Error in debug: {e}")
    
    def get_all_wnba_alternates(self, save_to_file: bool = True, output_format: str = "csv") -> Dict[str, Any]:
        """
        Get alternate player props for all current WNBA games from DraftKings only
        
        Args:
            save_to_file (bool): Whether to save results to file
            output_format (str): Output format - "csv" or "json"
            
        Returns:
            Complete dataset of WNBA alternate player props from DraftKings
        """
        print("Starting WNBA alternate player props collection from DraftKings...")
        
        # Get all events
        events = self.get_events()
        if not events:
            print("No WNBA events found")
            return {}
        
        all_alternates = {}
        
        for i, event in enumerate(events, 1):
            event_id = event['id']
            matchup = f"{event['away_team']} @ {event['home_team']}"
            commence_time = event['commence_time']
            
            print(f"\n[{i}/{len(events)}] Processing: {matchup}")
            print(f"Event ID: {event_id}")
            print(f"Start Time: {commence_time}")
            
            # Get odds for this event from DraftKings only
            event_odds = self.get_event_odds(event_id, bookmakers="draftkings")
            
            if event_odds:
                # Extract DraftKings alternates
                alternates = self.extract_draftkings_alternate_props(event_odds)
                
                if alternates:
                    all_alternates[event_id] = {
                        'matchup': matchup,
                        'commence_time': commence_time,
                        'home_team': event['home_team'],
                        'away_team': event['away_team'],
                        'alternates': alternates,
                        'retrieved_at': datetime.now().isoformat()
                    }
                    
                    player_count = len(alternates)
                    total_props = sum(len(props) for props in alternates.values())
                    print(f"✓ Found DraftKings alternates for {player_count} players ({total_props} total prop types)")
                else:
                    print("✗ No DraftKings alternate props found for this event")
            else:
                print("✗ Failed to get DraftKings odds data")
            
            # Rate limiting - be respectful to the API
            if i < len(events):
                time.sleep(1)
        
        print(f"\n=== DRAFTKINGS COLLECTION COMPLETE ===")
        print(f"Total events processed: {len(events)}")
        print(f"Events with DraftKings alternates: {len(all_alternates)}")
        
        # Save to file if requested
        if save_to_file:
            if output_format.lower() == "csv":
                self.save_to_csv(all_alternates)
            else:
                filename = f"draftkings_wnba_alternates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(all_alternates, f, indent=2)
                print(f"Results saved to: {filename}")
        
        return all_alternates
    
    def search_player_alternates(self, all_data: Dict[str, Any], 
                               player_name: str = None, 
                               prop_type: str = None) -> List[Dict]:
        """
        Search for specific player alternate props across all games
        
        Args:
            all_data: Complete dataset from get_all_wnba_alternates()
            player_name: Player name to search for (partial match)
            prop_type: Prop type to filter by (e.g., 'player_points')
            
        Returns:
            List of matching alternate props
        """
        results = []
        
        for event_id, event_data in all_data.items():
            matchup = event_data['matchup']
            
            for player, props in event_data['alternates'].items():
                # Filter by player name if specified
                if player_name and player_name.lower() not in player.lower():
                    continue
                
                for prop_key, prop_lines in props.items():
                    # Filter by prop type if specified
                    if prop_type and prop_type not in prop_key:
                        continue
                    
                    for line in prop_lines:
                        results.append({
                            'event_id': event_id,
                            'matchup': matchup,
                            'player': player,
                            'prop_type': prop_key,
                            'line': line['line'],
                            'bookmaker': line['bookmaker'],
                            'over_price': line['over_price'],
                            'under_price': line['under_price'],
                            'last_update': line['last_update']
                        })
        
        return results
    
    def save_to_csv(self, all_data: Dict[str, Any]) -> str:
        """
        Save alternate props data to CSV file
        
        Args:
            all_data: Complete dataset from get_all_wnba_alternates()
            
        Returns:
            Filename of saved CSV
        """
        filename = f"draftkings_wnba_alternates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Define CSV headers
        headers = [
            'event_id',
            'matchup', 
            'home_team',
            'away_team',
            'commence_time',
            'player_name',
            'prop_type',
            'bookmaker',
            'bet_type',  # Over/Under
            'line_value',
            'odds_price',
            'last_update',
            'retrieved_at'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            # Write data rows
            for event_id, event_data in all_data.items():
                matchup = event_data['matchup']
                home_team = event_data['home_team']
                away_team = event_data['away_team']
                commence_time = event_data['commence_time']
                retrieved_at = event_data['retrieved_at']
                
                for player_name, props in event_data['alternates'].items():
                    for prop_type, prop_lines in props.items():
                        for line in prop_lines:
                            # Create row for Over bet if exists
                            if line['over_price'] is not None:
                                writer.writerow([
                                    event_id,
                                    matchup,
                                    home_team,
                                    away_team,
                                    commence_time,
                                    player_name,
                                    prop_type,
                                    line['bookmaker'],
                                    'Over',
                                    line['line'],
                                    line['over_price'],
                                    line['last_update'],
                                    retrieved_at
                                ])
                            
                            # Create row for Under bet if exists
                            if line['under_price'] is not None:
                                writer.writerow([
                                    event_id,
                                    matchup,
                                    home_team,
                                    away_team,
                                    commence_time,
                                    player_name,
                                    prop_type,
                                    line['bookmaker'],
                                    'Under',
                                    line['line'],
                                    line['under_price'],
                                    line['last_update'],
                                    retrieved_at
                                ])
        
        print(f"CSV results saved to: {filename}")
        return filename
    
    def save_search_results_to_csv(self, search_results: List[Dict], filename: str = None) -> str:
        """
        Save search results to CSV file
        
        Args:
            search_results: Results from search_player_alternates()
            filename: Optional custom filename
            
        Returns:
            Filename of saved CSV
        """
        if filename is None:
            filename = f"wnba_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if not search_results:
            print("No search results to save")
            return filename
        
        # Define CSV headers based on search results structure
        headers = [
            'event_id',
            'matchup',
            'player',
            'prop_type', 
            'line',
            'bookmaker',
            'over_price',
            'under_price',
            'last_update'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            for result in search_results:
                writer.writerow([
                    result['event_id'],
                    result['matchup'],
                    result['player'],
                    result['prop_type'],
                    result['line'],
                    result['bookmaker'],
                    result['over_price'],
                    result['under_price'],
                    result['last_update']
                ])
        
        print(f"Search results saved to: {filename}")
        return filename

def main():
    """
    Example usage of the WNBA Player Props API script
    """
    # Replace with your actual API key
    API_KEY = "7fa01dfe4fac4d1a6f8888f513d79d0f"
    
    if API_KEY == "YOUR_API_KEY_HERE":
        print("Error: Please set your API key from The Odds API")
        print("Get one free at: https://the-odds-api.com/")
        return
    
    # Initialize the API client
    api = WNBAPlayerPropsAPI(API_KEY)
    
    # Debug: Check what markets are available for the first event
    print("=== DEBUGGING: Checking Available Markets ===")
    events = api.get_events()
    if events:
        first_event_id = events[0]['id']
        api.debug_available_markets(first_event_id)
    
    # Example 1: Get all WNBA alternate props from Caesars and save as CSV
    print("\n=== Getting All WNBA Alternate Player Props from Caesars ===")
    all_alternates = api.get_all_wnba_alternates(save_to_file=True, output_format="csv")
    
    # Example 2: Search for specific player in Caesars data
    if all_alternates:
        print("\n=== Searching for A'ja Wilson Props from Caesars ===")
        aja_props = api.search_player_alternates(all_alternates, player_name="A'ja Wilson")
        
        if aja_props:
            # Save search results to CSV
            search_filename = api.save_search_results_to_csv(aja_props, "caesars_aja_wilson_props.csv")
            
            # Display first 5 results
            for prop in aja_props[:5]:
                print(f"Player: {prop['player']}")
                print(f"Matchup: {prop['matchup']}")
                print(f"Prop: {prop['prop_type']}")
                print(f"Line: {prop['line']}")
                print(f"Bookmaker: {prop['bookmaker']} (Caesars)")
                print(f"Over: {prop['over_price']}, Under: {prop['under_price']}")
                print("-" * 50)
        else:
            print("No Caesars props found for A'ja Wilson")
    
    # Example 3: Search for all points props from Caesars
    if all_alternates:
        print("\n=== Getting All Points Props from Caesars ===")
        points_props = api.search_player_alternates(all_alternates, prop_type="player_points")
        
        if points_props:
            points_filename = api.save_search_results_to_csv(points_props, "caesars_points_props.csv")
            print(f"Found {len(points_props)} Caesars points prop lines")
            print(f"Saved to: {points_filename}")
    
    # Example 4: Summary statistics for Caesars data
    if all_alternates:
        print("\n=== Caesars Data Summary Statistics ===")
        total_events = len(all_alternates)
        total_players = sum(len(event['alternates']) for event in all_alternates.values())
        total_prop_lines = 0
        
        for event in all_alternates.values():
            for player_props in event['alternates'].values():
                for prop_lines in player_props.values():
                    total_prop_lines += len(prop_lines)
        
        print(f"Total Events with Caesars Data: {total_events}")
        print(f"Total Players with Caesars Props: {total_players}")
        print(f"Total Caesars Prop Lines: {total_prop_lines}")
        print("All Caesars alternate props data saved to CSV format!")
    else:
        print("\n=== TROUBLESHOOTING ===")
        print("No alternates found. This could be because:")
        print("1. Caesars doesn't have alternate lines for current WNBA games")
        print("2. The alternate markets aren't available yet")
        print("3. WNBA season timing - alternates may not be offered during off-season")
        print("4. API key limitations or rate limiting")
        print("\nTry running the debug function above to see what markets are actually available.")


if __name__ == "__main__":
    main()