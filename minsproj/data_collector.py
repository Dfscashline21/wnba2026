"""
WNBA Data Collection Module
Handles real-time data collection from multiple sources for minutes prediction model
"""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple
import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json

class WNBADataCollector:
    """Main data collection class for WNBA minutes prediction model"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = self._setup_logger()
        self.base_urls = {
            'wnba_api': 'https://data.wnba.com',
            'injury_reports': 'https://www.espn.com/wnba/injuries',
            'rotowire': 'https://www.rotowire.com/basketball/wnba/',
            'twitter_api': 'https://api.twitter.com/2'
        }
        self.session = requests.Session()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('WNBA_DataCollector')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
        
    async def collect_real_time_game_data(self, game_date: str = None) -> Dict:
        """
        Collect real-time game data including injury reports, lineups, and game context
        
        Args:
            game_date: Date in YYYY-MM-DD format, defaults to today
            
        Returns:
            Dictionary containing all real-time game data
        """
        if not game_date:
            game_date = datetime.now().strftime('%Y-%m-%d')
            
        self.logger.info(f"Collecting real-time data for {game_date}")
        
        # Collect data from multiple sources concurrently
        tasks = [
            self._get_injury_reports(game_date),
            self._get_starting_lineups(game_date),
            self._get_game_context(game_date),
            self._get_recent_performance_data(game_date),
            self._get_betting_lines(game_date)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            'injury_reports': results[0] if not isinstance(results[0], Exception) else {},
            'starting_lineups': results[1] if not isinstance(results[1], Exception) else {},
            'game_context': results[2] if not isinstance(results[2], Exception) else {},
            'recent_performance': results[3] if not isinstance(results[3], Exception) else {},
            'betting_lines': results[4] if not isinstance(results[4], Exception) else {},
            'collection_timestamp': datetime.now().isoformat()
        }
        
    async def _get_injury_reports(self, game_date: str) -> Dict:
        """Collect injury reports from multiple sources"""
        injury_data = {}
        
        try:
            # ESPN injury reports
            espn_url = f"{self.base_urls['injury_reports']}/_/date/{game_date}"
            async with aiohttp.ClientSession() as session:
                async with session.get(espn_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        injury_data['espn'] = self._parse_espn_injuries(soup)
                        
            # RotoWire injury reports
            roto_url = f"{self.base_urls['rotowire']}news/injuries"
            async with aiohttp.ClientSession() as session:
                async with session.get(roto_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        injury_data['rotowire'] = self._parse_rotowire_injuries(soup)
                        
        except Exception as e:
            self.logger.error(f"Error collecting injury reports: {e}")
            
        return injury_data
        
    def _parse_espn_injuries(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse ESPN injury report HTML"""
        injuries = []
        try:
            injury_rows = soup.find_all('tr', class_='Table__TR')
            for row in injury_rows:
                player_cell = row.find('td', class_='Table__TD')
                if player_cell:
                    player_name = player_cell.get_text(strip=True)
                    status_cell = row.find_all('td', class_='Table__TD')[2] if len(row.find_all('td', class_='Table__TD')) > 2 else None
                    injury_type_cell = row.find_all('td', class_='Table__TD')[1] if len(row.find_all('td', class_='Table__TD')) > 1 else None
                    
                    injuries.append({
                        'player': player_name,
                        'injury_type': injury_type_cell.get_text(strip=True) if injury_type_cell else 'Unknown',
                        'status': status_cell.get_text(strip=True) if status_cell else 'Unknown',
                        'source': 'ESPN'
                    })
        except Exception as e:
            self.logger.error(f"Error parsing ESPN injuries: {e}")
            
        return injuries
        
    def _parse_rotowire_injuries(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse RotoWire injury report HTML"""
        injuries = []
        try:
            news_items = soup.find_all('div', class_='news-update')
            for item in news_items:
                player_elem = item.find('a', class_='news-update__player-link')
                status_elem = item.find('span', class_='news-update__injury')
                
                if player_elem and status_elem:
                    injuries.append({
                        'player': player_elem.get_text(strip=True),
                        'status': status_elem.get_text(strip=True),
                        'source': 'RotoWire'
                    })
        except Exception as e:
            self.logger.error(f"Error parsing RotoWire injuries: {e}")
            
        return injuries
        
    async def _get_starting_lineups(self, game_date: str) -> Dict:
        """Get confirmed starting lineups for games"""
        lineups = {}
        
        try:
            # This would connect to WNBA API or other lineup sources
            api_url = f"{self.base_urls['wnba_api']}/lineups/{game_date}"
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        lineups = self._process_lineup_data(data)
        except Exception as e:
            self.logger.error(f"Error collecting starting lineups: {e}")
            
        return lineups
        
    def _process_lineup_data(self, data: Dict) -> Dict:
        """Process raw lineup data into structured format"""
        processed = {}
        
        try:
            for game in data.get('games', []):
                game_id = game.get('gameId')
                home_team = game.get('homeTeam', {}).get('teamTricode')
                away_team = game.get('awayTeam', {}).get('teamTricode')
                
                processed[game_id] = {
                    'home_team': home_team,
                    'away_team': away_team,
                    'home_starters': game.get('homeTeam', {}).get('starters', []),
                    'away_starters': game.get('awayTeam', {}).get('starters', []),
                    'confirmed': game.get('lineupConfirmed', False),
                    'last_updated': datetime.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Error processing lineup data: {e}")
            
        return processed
        
    async def _get_game_context(self, game_date: str) -> Dict:
        """Collect game context data (home/away, rest days, opponent strength)"""
        context_data = {}
        
        try:
            # Get schedule data
            schedule_url = f"{self.base_urls['wnba_api']}/schedule/{game_date}"
            async with aiohttp.ClientSession() as session:
                async with session.get(schedule_url) as response:
                    if response.status == 200:
                        schedule_data = await response.json()
                        context_data = self._process_game_context(schedule_data)
        except Exception as e:
            self.logger.error(f"Error collecting game context: {e}")
            
        return context_data
        
    def _process_game_context(self, schedule_data: Dict) -> Dict:
        """Process game context information"""
        context = {}
        
        try:
            for game in schedule_data.get('games', []):
                game_id = game.get('gameId')
                context[game_id] = {
                    'is_back_to_back': self._check_back_to_back(game),
                    'rest_days': self._calculate_rest_days(game),
                    'home_team': game.get('homeTeam', {}).get('teamTricode'),
                    'away_team': game.get('awayTeam', {}).get('teamTricode'),
                    'playoff_implications': self._assess_playoff_implications(game),
                    'rivalry_game': self._check_rivalry(game),
                    'season_stage': self._determine_season_stage(game.get('gameDate'))
                }
        except Exception as e:
            self.logger.error(f"Error processing game context: {e}")
            
        return context
        
    def _check_back_to_back(self, game: Dict) -> bool:
        """Check if game is part of back-to-back for either team"""
        # Implementation would check previous game dates
        return False  # Placeholder
        
    def _calculate_rest_days(self, game: Dict) -> Dict:
        """Calculate rest days for each team"""
        # Implementation would calculate rest days
        return {'home': 1, 'away': 2}  # Placeholder
        
    def _assess_playoff_implications(self, game: Dict) -> bool:
        """Assess if game has playoff seeding implications"""
        # Implementation would check standings and remaining games
        return False  # Placeholder
        
    def _check_rivalry(self, game: Dict) -> bool:
        """Check if game is a rivalry matchup"""
        rivalry_pairs = [
            ('LAS', 'SEA'), ('NYL', 'CON'), ('CHI', 'IND')
        ]
        home_team = game.get('homeTeam', {}).get('teamTricode')
        away_team = game.get('awayTeam', {}).get('teamTricode')
        
        return (home_team, away_team) in rivalry_pairs or (away_team, home_team) in rivalry_pairs
        
    def _determine_season_stage(self, game_date: str) -> str:
        """Determine what stage of season the game is in"""
        try:
            game_datetime = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
            # WNBA season typically runs May-October
            month = game_datetime.month
            
            if month in [5, 6]:
                return 'early'
            elif month in [7, 8]:
                return 'mid'
            else:
                return 'late'
        except:
            return 'mid'
            
    async def _get_recent_performance_data(self, game_date: str) -> Dict:
        """Get recent performance data for all players"""
        performance_data = {}
        
        try:
            # Get last 10 games for all active players
            api_url = f"{self.base_urls['wnba_api']}/players/stats/last10"
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        performance_data = self._process_performance_data(data)
        except Exception as e:
            self.logger.error(f"Error collecting performance data: {e}")
            
        return performance_data
        
    def _process_performance_data(self, data: Dict) -> Dict:
        """Process recent performance data"""
        processed = {}
        
        try:
            for player_data in data.get('players', []):
                player_id = player_data.get('playerId')
                processed[player_id] = {
                    'last_10_games': player_data.get('last10Games', []),
                    'avg_minutes': np.mean([g.get('minutes', 0) for g in player_data.get('last10Games', [])]),
                    'minute_trend': self._calculate_minute_trend(player_data.get('last10Games', [])),
                    'performance_metrics': {
                        'points_per_game': np.mean([g.get('points', 0) for g in player_data.get('last10Games', [])]),
                        'rebounds_per_game': np.mean([g.get('rebounds', 0) for g in player_data.get('last10Games', [])]),
                        'assists_per_game': np.mean([g.get('assists', 0) for g in player_data.get('last10Games', [])])
                    }
                }
        except Exception as e:
            self.logger.error(f"Error processing performance data: {e}")
            
        return processed
        
    def _calculate_minute_trend(self, games: List[Dict]) -> str:
        """Calculate if player's minutes are trending up, down, or stable"""
        if len(games) < 3:
            return 'stable'
            
        minutes = [g.get('minutes', 0) for g in games[-5:]]  # Last 5 games
        if len(minutes) < 3:
            return 'stable'
            
        # Simple trend calculation
        early_avg = np.mean(minutes[:2])
        late_avg = np.mean(minutes[-2:])
        
        if late_avg > early_avg + 2:
            return 'increasing'
        elif late_avg < early_avg - 2:
            return 'decreasing'
        else:
            return 'stable'
            
    async def _get_betting_lines(self, game_date: str) -> Dict:
        """Get betting lines and movements for insight into expected player availability"""
        betting_data = {}
        
        try:
            # This would connect to betting APIs or scrape sportsbook data
            # Placeholder implementation
            betting_data = {
                'lines_available': False,
                'message': 'Betting data collection not implemented in demo'
            }
        except Exception as e:
            self.logger.error(f"Error collecting betting data: {e}")
            
        return betting_data
        
    def collect_historical_data(self, seasons: List[str] = None) -> pd.DataFrame:
        """
        Collect historical player data for model training
        
        Args:
            seasons: List of seasons to collect (e.g., ['2022', '2023', '2024'])
            
        Returns:
            DataFrame with historical game-by-game data
        """
        if not seasons:
            current_year = datetime.now().year
            seasons = [str(year) for year in range(current_year - 3, current_year + 1)]
            
        self.logger.info(f"Collecting historical data for seasons: {seasons}")
        
        all_data = []
        
        for season in seasons:
            try:
                season_data = self._collect_season_data(season)
                all_data.extend(season_data)
            except Exception as e:
                self.logger.error(f"Error collecting data for season {season}: {e}")
                
        df = pd.DataFrame(all_data)
        return self._clean_historical_data(df)
        
    def _collect_season_data(self, season: str) -> List[Dict]:
        """Collect all games data for a specific season"""
        season_data = []
        
        try:
            # This would make API calls to get all games for the season
            api_url = f"{self.base_urls['wnba_api']}/seasons/{season}/games"
            response = self.session.get(api_url)
            
            if response.status_code == 200:
                data = response.json()
                season_data = self._process_season_games(data, season)
        except Exception as e:
            self.logger.error(f"Error collecting season {season} data: {e}")
            
        return season_data
        
    def _process_season_games(self, data: Dict, season: str) -> List[Dict]:
        """Process season games data into training format"""
        games_data = []
        
        try:
            for game in data.get('games', []):
                game_date = game.get('gameDate')
                home_team = game.get('homeTeam', {})
                away_team = game.get('awayTeam', {})
                
                # Process home team players
                for player in home_team.get('players', []):
                    player_game_data = {
                        'season': season,
                        'game_date': game_date,
                        'game_id': game.get('gameId'),
                        'player_id': player.get('playerId'),
                        'player_name': player.get('playerName'),
                        'team': home_team.get('teamTricode'),
                        'opponent': away_team.get('teamTricode'),
                        'is_home': True,
                        'minutes_played': player.get('statistics', {}).get('minutes', 0),
                        'points': player.get('statistics', {}).get('points', 0),
                        'rebounds': player.get('statistics', {}).get('rebounds', 0),
                        'assists': player.get('statistics', {}).get('assists', 0),
                        'is_starter': player.get('starter', False)
                    }
                    games_data.append(player_game_data)
                    
                # Process away team players
                for player in away_team.get('players', []):
                    player_game_data = {
                        'season': season,
                        'game_date': game_date,
                        'game_id': game.get('gameId'),
                        'player_id': player.get('playerId'),
                        'player_name': player.get('playerName'),
                        'team': away_team.get('teamTricode'),
                        'opponent': home_team.get('teamTricode'),
                        'is_home': False,
                        'minutes_played': player.get('statistics', {}).get('minutes', 0),
                        'points': player.get('statistics', {}).get('points', 0),
                        'rebounds': player.get('statistics', {}).get('rebounds', 0),
                        'assists': player.get('statistics', {}).get('assists', 0),
                        'is_starter': player.get('starter', False)
                    }
                    games_data.append(player_game_data)
                    
        except Exception as e:
            self.logger.error(f"Error processing season games: {e}")
            
        return games_data
        
    def _clean_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate historical data"""
        if df.empty:
            return df
            
        # Remove invalid entries
        df = df[df['minutes_played'].notna()]
        df = df[df['minutes_played'] >= 0]
        df = df[df['minutes_played'] <= 48]  # Maximum possible minutes in WNBA game
        
        # Convert data types
        df['game_date'] = pd.to_datetime(df['game_date'])
        df['minutes_played'] = df['minutes_played'].astype(float)
        
        # Sort by date and player
        df = df.sort_values(['player_id', 'game_date'])
        
        self.logger.info(f"Cleaned historical data: {len(df)} player-game records")
        
        return df
        
    def get_team_data(self) -> pd.DataFrame:
        """Collect team-level data and coaching information"""
        try:
            api_url = f"{self.base_urls['wnba_api']}/teams"
            response = self.session.get(api_url)
            
            if response.status_code == 200:
                data = response.json()
                return self._process_team_data(data)
        except Exception as e:
            self.logger.error(f"Error collecting team data: {e}")
            
        return pd.DataFrame()
        
    def _process_team_data(self, data: Dict) -> pd.DataFrame:
        """Process team data into structured format"""
        team_records = []
        
        try:
            for team in data.get('teams', []):
                team_record = {
                    'team_id': team.get('teamId'),
                    'team_code': team.get('teamTricode'),
                    'team_name': team.get('teamName'),
                    'coach': team.get('headCoach', {}).get('name'),
                    'division': team.get('division'),
                    'conference': team.get('conference')
                }
                team_records.append(team_record)
        except Exception as e:
            self.logger.error(f"Error processing team data: {e}")
            
        return pd.DataFrame(team_records)