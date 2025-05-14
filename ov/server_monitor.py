import os
import time
import logging
import requests
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
import signal
import sys
import codecs
from functools import wraps
from typing import Dict, List, Optional, Any, Tuple, Union, Callable

from database import init_database, migrate_database
from server_manager import get_or_create_server
from session_manager import get_active_session, update_player_playtimes, end_session
from player_manager import update_player_record, update_death_statistics
from status_manager import save_server_status, save_team_status
from redemption_stats import report_recent_redeems, report_recent_deaths
from wave_manager import save_wave_end_snapshot

API_URL = "https://csc.sunrust.org/public/servers"
DATA_DIR = Path("./data")
LOG_DIR = Path("./logs")
DB_FILE = "gameservers.db"
INTERVAL_SECONDS = 10
SESSION_TIMEOUT_MINUTES = 15
MIN_PLAYERS_FOR_SESSION = 1
LOG_RETENTION_DAYS = 7
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY = 2

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

def retry_request(func: Callable) -> Callable:
    """Decorator for retrying HTTP requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logging.warning(f"Request failed: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Request failed after {MAX_RETRIES} attempts: {str(e)}")
                    return None
    return wrapper

class ServerMonitor:
    def __init__(self):
        self.stop_event = threading.Event()
        self.last_minute = None
        self.thread_pool = []
        self.task_queue = queue.Queue()
        self.active_servers_cache = {}
        self.last_status_report = datetime.now()
        self.setup_log_rotation()
        
    def setup_log_rotation(self) -> None:
        """Set up log rotation to remove old log files"""
        today = datetime.now().date()
        for log_file in LOG_DIR.glob("server_monitor_*.log"):
            try:
                filename = log_file.name
                date_str = filename.replace("server_monitor_", "").replace(".log", "")
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                if (today - file_date).days > LOG_RETENTION_DAYS:
                    os.remove(log_file)
                    logging.debug(f"Deleted old log file: {log_file}")
            except Exception as e:
                logging.error(f"Error during log rotation for file {log_file}: {str(e)}")

    @retry_request
    def fetch_server_list(self) -> Optional[Dict]:
        """Fetch the list of servers from the API"""
        response = requests.get(API_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    @retry_request
    def fetch_server_details(self, server_code: str) -> Optional[Dict]:
        """Fetch details for a specific server"""
        url = f"{API_URL}/{server_code}"
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def process_server(self, server_code: str, server_data: Dict, timestamp: str) -> Tuple[int, int]:
        """Process data for a single server"""
        try:
            player_count = server_data.get('PlayerCount', 0)
            current_wave = server_data.get('ExtraInfo')
            map_name = server_data.get('Map', 'Unknown')
            max_players = server_data.get('MaxPlayers', 0)
            server_name = server_data.get('Name', server_code)
            
            server_id = get_or_create_server(server_code, server_name)
            
            team_data = None
            
            if player_count >= MIN_PLAYERS_FOR_SESSION:
                details = self.fetch_server_details(server_code)
                
                if details and 'TeamList' in details and 'PlayerList' in details:
                    team_counts = {}
                    team_scores = {}
                    
                    for player in details['PlayerList']:
                        team_id = player.get('Details', {}).get('Team')
                        if team_id:
                            if team_id not in team_counts:
                                team_counts[team_id] = 0
                                team_scores[team_id] = 0
                            
                            team_counts[team_id] += 1
                            
                            score = player.get('Details', {}).get('Frags', 0)
                            team_scores[team_id] += score
                    
                    team_data = team_counts
            
            session_id = get_active_session(server_id, map_name, current_wave, player_count, max_players, team_data)
            save_server_status(session_id, player_count, current_wave, timestamp)
            
            # If there are players, save detailed infos
            if player_count >= MIN_PLAYERS_FOR_SESSION and details:
                if 'TeamList' in details and 'PlayerList' in details:
                    for team_id, count in team_counts.items():
                        team_name = details.get('TeamList', {}).get(str(team_id), {}).get('Name', f'Team {team_id}')
                        save_team_status(session_id, team_id, team_name, count, team_scores[team_id], timestamp)
                
                if 'PlayerList' in details:
                    self.process_players(session_id, details['PlayerList'], timestamp)
                    
            return (1 if player_count > 0 else 0), player_count
        except Exception as e:
            logging.error(f"Error processing server {server_code}: {str(e)}")
            return 0, 0

    def process_players(self, session_id: int, player_list: List[Dict], timestamp: str) -> None:
        """Process player data for a session"""
        for player in player_list:
            try:
                is_bot = player.get('SteamID64') == "0"
                
                if is_bot:
                    bot_name = player.get('Details', {}).get('BotInfo', {}).get('Name', 'Unknown Bot')
                    bot_team = player.get('Details', {}).get('Team', 0)
                    
                    steam_id = f"bot_{bot_name}_{bot_team}"
                    player_name = bot_name
                else:
                    steam_id = player.get('SteamID64', 'Unknown')
                    player_name = player.get('SteamPlayerDetails', {}).get('Name', 'Unknown Player')
                
                team_id = player.get('Details', {}).get('Team')
                score = player.get('Details', {}).get('Frags', 0) or 0
                
                update_player_record(session_id, steam_id, player_name, team_id, score, is_bot, timestamp)
            except Exception as e:
                logging.error(f"Error processing player {player.get('SteamID64', 'unknown')}: {str(e)}")

    def process_servers(self) -> None:
        """Process all servers data"""
        timestamp = datetime.now().isoformat()
        logging.debug("Starting server data collection cycle")
        
        servers = self.fetch_server_list()
        if not servers:
            return
        
        for server_code, server_data in servers.items():
            self.task_queue.put((self.process_server, (server_code, server_data, timestamp)))
        
        self.task_queue.join()
        self.active_servers_cache = servers
        
        now = datetime.now()
        if not self.last_minute or self.last_minute != now.minute:
            self.last_minute = now.minute
            
            active_servers = sum(1 for s in servers.values() if s.get('PlayerCount', 0) > 0)
            total_players = sum(s.get('PlayerCount', 0) for s in servers.values())
            
            if active_servers > 0:
                logging.info(f"Active servers: {active_servers} with {total_players} players")

    def worker(self) -> None:
        """Worker thread function to process tasks from the queue"""
        while not self.stop_event.is_set():
            try:
                func, args = self.task_queue.get(timeout=1)
                func(*args)
                self.task_queue.task_done()
            except queue.Empty:
                pass
            except Exception as e:
                logging.error(f"Error in worker thread: {str(e)}")
                self.task_queue.task_done()

    def monitor_thread(self) -> None:
        """Main monitoring thread"""
        while not self.stop_event.is_set():
            try:
                self.process_servers()
                report_recent_deaths()
                report_recent_redeems()
            except Exception as e:
                logging.error(f"Unexpected error in monitor thread: {str(e)}")
            
            self.stop_event.wait(INTERVAL_SECONDS)

    def cleanup_previous_sessions(self) -> None:
        """Clean up any active sessions on startup"""
        from database import query_all, execute_query
        
        now = datetime.now().isoformat()
        
        active_sessions = query_all(
            '''
            SELECT id, server_id, map_name, wave_number
            FROM game_sessions
            WHERE is_active = 1
            '''
        )
        
        processed_sessions = set()
        
        for session in active_sessions:
            session_id = session['id']
            
            if session_id in processed_sessions:
                continue
            
            processed_sessions.add(session_id)
            
            wave_number = session['wave_number'] if session['wave_number'] is not None else 0
            
            logging.info(f"Found previously active session {session_id} on startup, marking as ended")
            
            execute_query(
                '''
                UPDATE game_sessions
                SET is_active = 0, end_time = ?, session_result = 'Incomplete - Server Restart'
                WHERE id = ?
                ''', 
                (now, session_id)
            )
            
            try:
                save_wave_end_snapshot(session_id, wave_number, "Server Restart")
                update_player_playtimes(session_id, now)
                update_death_statistics(session_id)
            except Exception as e:
                logging.error(f"Error finalizing session {session_id} on startup: {str(e)}")

    def start(self) -> threading.Thread:
        """Start the server monitor"""
        logging.info("Server Monitor starting...")
        init_database()
        migrate_database()
        
        self.cleanup_previous_sessions()
        
        # Start worker threads
        for _ in range(min(4, os.cpu_count() or 4)):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()
            self.thread_pool.append(t)
        
        monitor = threading.Thread(target=self.monitor_thread)
        monitor.daemon = True
        monitor.start()
        self.thread_pool.append(monitor)
        
        return monitor

    def stop(self) -> None:
        """Stop the server monitor"""
        logging.info("Stopping server monitor...")
        self.stop_event.set()
        
        for thread in self.thread_pool:
            thread.join(timeout=5)
        
        logging.info("Server monitor stopped")

monitor = None

def signal_handler(sig, frame):
    """Handle termination signals"""
    global monitor
    logging.info("Shutdown signal received. Stopping monitor...")
    if monitor:
        monitor.stop()
    sys.exit(0)

def main():
    global monitor
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    monitor = ServerMonitor()
    monitor_thread = monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down...")
        monitor.stop()
        monitor_thread.join(timeout=5)

if __name__ == "__main__":
    main()