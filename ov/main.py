import time
import logging
import requests
import threading
from datetime import datetime
from pathlib import Path
import signal
import sys
import codecs

from database import init_database, migrate_database
from server_manager import get_or_create_server
from session_manager import get_active_session
from player_manager import update_player_record
from status_manager import save_server_status, save_team_status

API_URL = "https://csc.sunrust.org/public/servers"
DATA_DIR = Path("./data")
LOG_DIR = Path("./logs")
DB_FILE = "gameservers.db"
INTERVAL_SECONDS = 10

SESSION_TIMEOUT_MINUTES = 15
MIN_PLAYERS_FOR_SESSION = 1

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"server_monitor_{datetime.now().strftime('%Y-%m-%d')}.log"
file_handler = logging.FileHandler(log_file, encoding='utf-8')
console_handler = logging.StreamHandler(codecs.getwriter('utf-8')(sys.stdout.buffer))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[file_handler, console_handler]
)

def fetch_server_list():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch server list: {str(e)}")
        return None

def fetch_server_details(server_code):
    try:
        url = f"{API_URL}/{server_code}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch details for server {server_code}: {str(e)}")
        return None

def process_servers():
    timestamp = datetime.now().isoformat()
    logging.info("Starting server data collection cycle")
    
    servers = fetch_server_list()
    if not servers:
        return
    
    active_servers = 0
    total_players = 0
    
    for server_code, server_data in servers.items():
        player_count = server_data.get('PlayerCount', 0)
        total_players += player_count
        
        if player_count > 0:
            active_servers += 1
        
        current_wave = server_data.get('ExtraInfo')
        map_name = server_data.get('Map', 'Unknown')
        max_players = server_data.get('MaxPlayers', 0)
        server_name = server_data.get('Name', server_code)
        
        server_id = get_or_create_server(server_code, server_name)
        
        team_data = None
        
        if player_count >= MIN_PLAYERS_FOR_SESSION:
            details = fetch_server_details(server_code)
            
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
        
        if player_count >= MIN_PLAYERS_FOR_SESSION and details:
            if 'TeamList' in details and 'PlayerList' in details:
                for team_id, count in team_counts.items():
                    team_name = details.get('TeamList', {}).get(str(team_id), {}).get('Name', f'Team {team_id}')
                    save_team_status(session_id, team_id, team_name, count, team_scores[team_id], timestamp)
            
            if 'PlayerList' in details:
                for player in details['PlayerList']:
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
    
    logging.info(f"Completed server data collection cycle. Processed {len(servers)} servers, {active_servers} active with {total_players} players.")

stop_event = threading.Event()

def monitor_thread():
    while not stop_event.is_set():
        try:
            process_servers()
        except Exception as e:
            logging.error(f"Unexpected error in monitor thread: {str(e)}")
        
        stop_event.wait(INTERVAL_SECONDS)

def signal_handler(sig, frame):
    logging.info("Shutdown signal received. Stopping monitor...")
    stop_event.set()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("Server Monitor starting...")
    init_database()
    migrate_database()
    
    monitor = threading.Thread(target=monitor_thread)
    monitor.daemon = True
    monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down...")
        stop_event.set()
        monitor.join(timeout=5)

if __name__ == "__main__":
    main()