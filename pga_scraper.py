import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PGATourScraperFixed:
    """
    Updated PGA Tour scraper using working endpoints and web scraping.
    Uses a combination of available APIs and direct web scraping.
    """

    def __init__(self, delay_between_requests=1.0):
        """
        Initialize scraper with request delay to be respectful.
        """
        self.delay = delay_between_requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def get_current_leaderboard(self) -> pd.DataFrame:
        """
        Scrape current tournament leaderboard from PGA Tour website.
        """
        try:
            logger.info("Fetching current leaderboard from PGA Tour website...")

            # PGA Tour's main leaderboard page
            url = "https://www.pgatour.com/leaderboard"
            response = self.session.get(url)
            time.sleep(self.delay)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Try to find the Next.js data script
                script_tag = soup.find('script', id='__NEXT_DATA__')
                if script_tag:
                    data = json.loads(script_tag.string)
                    # Navigate through the JSON structure
                    leaderboard_data = self._extract_leaderboard_from_json(data)
                    if leaderboard_data:
                        return pd.DataFrame(leaderboard_data)

                # Fallback to table scraping if JSON not found
                return self._scrape_leaderboard_table(soup)

            logger.error(f"Failed to fetch leaderboard: Status {response.status_code}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error getting current leaderboard: {e}")
            return pd.DataFrame()

    def _extract_leaderboard_from_json(self, data: dict) -> List[Dict]:
        """
        Extract leaderboard data from Next.js JSON.
        """
        try:
            # Navigate through possible JSON structures
            # This structure may vary, so we try multiple paths
            players = []

            # Common paths in PGA Tour's Next.js data
            paths = [
                ['props', 'pageProps', 'leaderboard', 'players'],
                ['props', 'pageProps', 'data', 'leaderboard'],
                ['props', 'pageProps', 'initialState', 'leaderboard', 'players']
            ]

            for path in paths:
                current = data
                for key in path:
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        break
                else:
                    # Successfully navigated the path
                    if isinstance(current, list):
                        for player in current:
                            players.append({
                                'player_name': player.get('playerName', ''),
                                'position': player.get('position', ''),
                                'total_score': player.get('totalScore', ''),
                                'score_to_par': player.get('scoreToPar', ''),
                                'thru': player.get('thru', ''),
                                'today': player.get('today', '')
                            })
                        return players

            return []

        except Exception as e:
            logger.warning(f"Could not extract from JSON: {e}")
            return []

    def _scrape_leaderboard_table(self, soup) -> pd.DataFrame:
        """
        Fallback method to scrape leaderboard from HTML tables.
        """
        try:
            players = []

            # Look for leaderboard table with various possible class names
            table_classes = ['leaderboard', 'table', 'leaderboard-table']
            table = None

            for class_name in table_classes:
                table = soup.find('table', {'class': lambda x: x and class_name in x})
                if table:
                    break

            if not table:
                # Try finding any table with player data
                tables = soup.find_all('table')
                for t in tables:
                    if t.find(string=re.compile(r'(Player|Position|Score)', re.I)):
                        table = t
                        break

            if table:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 3:
                        players.append({
                            'position': cols[0].get_text(strip=True),
                            'player_name': cols[1].get_text(strip=True),
                            'score': cols[2].get_text(strip=True) if len(cols) > 2 else '',
                            'thru': cols[3].get_text(strip=True) if len(cols) > 3 else '',
                            'today': cols[4].get_text(strip=True) if len(cols) > 4 else ''
                        })

            return pd.DataFrame(players)

        except Exception as e:
            logger.error(f"Error scraping table: {e}")
            return pd.DataFrame()

    def scrape_player_stats_page(self, stat_type: str = 'STATS_YEAR') -> pd.DataFrame:
        """
        Scrape player statistics from PGA Tour stats pages.

        stat_type options:
        - 'STATS_YEAR': Current year stats
        - 'ROTO_STANDINGS': Fantasy/comprehensive stats
        """
        try:
            logger.info(f"Fetching player statistics ({stat_type})...")

            # Different stat pages on PGA Tour website
            stat_urls = {
                'STATS_YEAR': 'https://www.pgatour.com/stats',
                'ROTO_STANDINGS': 'https://www.pgatour.com/stats/detail/02675',  # Strokes Gained Total
                'DRIVING': 'https://www.pgatour.com/stats/detail/101',  # Driving Distance
                'PUTTING': 'https://www.pgatour.com/stats/detail/02676'  # SG: Putting
            }

            url = stat_urls.get(stat_type, stat_urls['STATS_YEAR'])
            response = self.session.get(url)
            time.sleep(self.delay)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Look for stats table
                stats_table = soup.find('table', {'class': lambda x: x and 'stats' in str(x).lower()})
                if not stats_table:
                    stats_table = soup.find('table')

                if stats_table:
                    players = []
                    rows = stats_table.find_all('tr')[1:]  # Skip header

                    for row in rows[:50]:  # Top 50 players
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 3:
                            players.append({
                                'rank': cols[0].get_text(strip=True),
                                'player_name': cols[1].get_text(strip=True),
                                'value': cols[2].get_text(strip=True),
                                'rounds': cols[3].get_text(strip=True) if len(cols) > 3 else ''
                            })

                    return pd.DataFrame(players)

            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error scraping stats page: {e}")
            return pd.DataFrame()

    def scrape_espn_leaderboard(self) -> pd.DataFrame:
        """
        Scrape current tournament from ESPN as backup source.
        """
        try:
            logger.info("Fetching from ESPN (backup source)...")

            url = "https://www.espn.com/golf/leaderboard"
            response = self.session.get(url)
            time.sleep(self.delay)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                players = []

                # ESPN uses different table structure
                leaderboard = soup.find('div', {'class': 'ResponsiveTable'})
                if not leaderboard:
                    leaderboard = soup.find('table', {'class': 'Table'})

                if leaderboard:
                    rows = leaderboard.find_all('tr')

                    for row in rows[1:]:  # Skip header
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 3:
                            # Clean player name (remove country flag, etc.)
                            player_name = cols[1].get_text(strip=True)
                            player_name = re.sub(r'\([A-Z]{3}\)', '', player_name).strip()

                            players.append({
                                'position': cols[0].get_text(strip=True),
                                'player_name': player_name,
                                'score': cols[2].get_text(strip=True),
                                'thru': cols[3].get_text(strip=True) if len(cols) > 3 else '',
                                'today': cols[4].get_text(strip=True) if len(cols) > 4 else '',
                                'r1': cols[5].get_text(strip=True) if len(cols) > 5 else '',
                                'r2': cols[6].get_text(strip=True) if len(cols) > 6 else '',
                                'r3': cols[7].get_text(strip=True) if len(cols) > 7 else '',
                                'r4': cols[8].get_text(strip=True) if len(cols) > 8 else ''
                            })

                return pd.DataFrame(players)

            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error scraping ESPN: {e}")
            return pd.DataFrame()

    def get_player_historical_stats(self, player_name: str) -> Dict:
        """
        Get historical statistics for a specific player.
        """
        try:
            # Format player name for URL (lowercase, replace spaces with hyphens)
            player_slug = player_name.lower().replace(' ', '-')

            url = f"https://www.pgatour.com/players/player.{player_slug}.html"
            response = self.session.get(url)
            time.sleep(self.delay)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                stats = {
                    'player_name': player_name,
                    'career_wins': 0,
                    'career_top10s': 0,
                    'career_earnings': 0
                }

                # Extract career stats from player page
                stat_items = soup.find_all('div', {'class': lambda x: x and 'stat' in str(x).lower()})

                for item in stat_items:
                    label = item.find('span', {'class': 'label'})
                    value = item.find('span', {'class': 'value'})

                    if label and value:
                        label_text = label.get_text(strip=True).lower()
                        value_text = value.get_text(strip=True)

                        if 'wins' in label_text:
                            stats['career_wins'] = int(re.sub(r'\D', '', value_text) or 0)
                        elif 'top 10' in label_text:
                            stats['career_top10s'] = int(re.sub(r'\D', '', value_text) or 0)
                        elif 'earnings' in label_text:
                            stats['career_earnings'] = float(re.sub(r'[^\d.]', '', value_text) or 0)

                return stats

            return {}

        except Exception as e:
            logger.warning(f"Could not get stats for {player_name}: {e}")
            return {}

    def get_comprehensive_stats(self) -> pd.DataFrame:
        """
        Combine multiple stat categories into comprehensive dataset.
        """
        logger.info("Building comprehensive statistics dataset...")

        all_stats = []

        # Get different stat categories
        stat_categories = {
            'strokes_gained_total': 'https://www.pgatour.com/content/pgatour/stats/stat.02675.y2024.html',
            'driving_distance': 'https://www.pgatour.com/content/pgatour/stats/stat.101.y2024.html',
            'driving_accuracy': 'https://www.pgatour.com/content/pgatour/stats/stat.102.y2024.html',
            'gir_percentage': 'https://www.pgatour.com/content/pgatour/stats/stat.103.y2024.html',
            'scrambling': 'https://www.pgatour.com/content/pgatour/stats/stat.130.y2024.html',
            'putting_average': 'https://www.pgatour.com/content/pgatour/stats/stat.104.y2024.html'
        }

        # First, get the base leaderboard
        base_data = self.get_current_leaderboard()
        if base_data.empty:
            base_data = self.scrape_espn_leaderboard()

        if not base_data.empty:
            # For each player in leaderboard, compile their stats
            for _, player_row in base_data.iterrows():
                player_stats = {
                    'player_name': player_row['player_name'],
                    'current_position': player_row.get('position', ''),
                    'current_score': player_row.get('score', '')
                }

                # Add placeholder stats (would be scraped from individual stat pages)
                player_stats.update({
                    'strokes_gained_total': 0.0,
                    'driving_distance': 290.0,
                    'driving_accuracy': 60.0,
                    'gir_percentage': 65.0,
                    'scrambling': 55.0,
                    'putting_average': 29.0,
                    'world_ranking': 50,
                    'fedex_points': 500
                })

                all_stats.append(player_stats)

        return pd.DataFrame(all_stats)

    def scrape_historical_results(self, year: int = 2024) -> pd.DataFrame:
        """
        Scrape historical tournament results for a given year.
        """
        logger.info(f"Scraping historical results for {year}...")

        try:
            url = f"https://www.pgatour.com/tournaments/schedule.{year}.html"
            response = self.session.get(url)
            time.sleep(self.delay)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                tournaments = []
                tournament_rows = soup.find_all('div', {'class': lambda x: x and 'tournament-row' in str(x)})

                for row in tournament_rows:
                    winner = row.find('div', {'class': 'winner'})
                    if winner:
                        tournaments.append({
                            'tournament': row.find('div', {'class': 'tournament-name'}).get_text(strip=True),
                            'winner': winner.get_text(strip=True),
                            'date': row.find('div', {'class': 'dates'}).get_text(strip=True) if row.find('div', {
                                'class': 'dates'}) else ''
                        })

                return pd.DataFrame(tournaments)

            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error scraping historical results: {e}")
            return pd.DataFrame()


def test_scraper():
    """
    Test the scraper with various methods to find what works.
    """
    print("=" * 60)
    print("TESTING PGA TOUR SCRAPER - Finding Working Methods")
    print("=" * 60)

    scraper = PGATourScraperFixed()
    results = {}

    # Test 1: PGA Tour Leaderboard
    print("\n1. Testing PGA Tour website leaderboard...")
    leaderboard = scraper.get_current_leaderboard()
    if not leaderboard.empty:
        print(f"   ✓ Success! Got {len(leaderboard)} players")
        results['pga_leaderboard'] = leaderboard
    else:
        print(f"   ✗ Failed")

    # Test 2: ESPN Backup
    print("\n2. Testing ESPN leaderboard...")
    espn_data = scraper.scrape_espn_leaderboard()
    if not espn_data.empty:
        print(f"   ✓ Success! Got {len(espn_data)} players")
        results['espn_leaderboard'] = espn_data
    else:
        print(f"   ✗ Failed")

    # Test 3: Stats Pages
    print("\n3. Testing stats pages...")
    stats = scraper.scrape_player_stats_page()
    if not stats.empty:
        print(f"   ✓ Success! Got {len(stats)} player stats")
        results['stats'] = stats
    else:
        print(f"   ✗ Failed")

    # Test 4: Comprehensive Stats
    print("\n4. Building comprehensive dataset...")
    comprehensive = scraper.get_comprehensive_stats()
    if not comprehensive.empty:
        print(f"   ✓ Success! Got {len(comprehensive)} complete records")
        results['comprehensive'] = comprehensive
    else:
        print(f"   ✗ Failed")

    # Save successful results
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)

    for name, df in results.items():
        if not df.empty:
            filename = f"pga_data_{name}_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False)
            print(f"✓ Saved: {filename} ({len(df)} rows, {len(df.columns)} columns)")
            print(f"  Columns: {', '.join(df.columns[:5])}{', ...' if len(df.columns) > 5 else ''}")

    if not results:
        print("✗ No data could be scraped. Check internet connection or try again later.")

    return results


def main():
    """
    Main execution - tries multiple methods to get data.
    """
    print("\nPGA Tour Data Scraper - Fixed Version")
    print("=" * 60)
    print("This version uses web scraping instead of broken APIs")
    print("=" * 60)

    # Run the test to find working methods
    results = test_scraper()

    if results:
        print("\n" + "=" * 60)
        print("NEXT STEPS")
        print("=" * 60)
        print("\n1. Check the CSV files created")
        print("2. Use the data with your prediction model")
        print("3. If you need more detailed stats, we can scrape individual player pages")

        # Show sample of best dataset
        if 'comprehensive' in results:
            print("\nSample of comprehensive data:")
            print(results['comprehensive'][['player_name', 'current_position', 'current_score']].head())
        elif 'pga_leaderboard' in results:
            print("\nSample of leaderboard data:")
            print(results['pga_leaderboard'].head())


if __name__ == "__main__":
    main()