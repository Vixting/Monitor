#!/usr/bin/env python3

import os
import json
import time
import logging
import requests
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
import signal
import sys
import hashlib
import codecs
import re

from database import init_database, get_db_connection, migrate_database, execute_with_retry
from server_manager import get_or_create_server
from session_manager import get_active_session, determine_session_result, update_player_playtimes, get_session_deaths, save_wave_end_snapshot
from player_manager import update_player_record, update_death_statistics
from status_manager import save_server_status, save_team_status
from util import extract_wave_number
from redemption_stats import report_recent_redeems

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

def setup_log_rotation():
    today = datetime.now().date()
    for log_file in LOG_DIR.glob("server_monitor_*.log"):
        try:
            filename = log_file.name
            date_str = filename.replace("server_monitor_", "").replace(".log", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            if (today - file_date).days > 7:
                os.remove(log_file)
                logging.debug(f"Deleted old log file: {log_file}")
        except Exception as e:
            logging.error(f"Error during log rotation for file {log_file}: {str(e)}")

setup_log_rotation()

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
    logging.debug("Starting server data collection cycle")
    
    servers = fetch_server_list()
    if not servers:
        return
    
    active_servers = 0
    total_players = 0
    
    for server_code, server_data in servers.items():
        try:
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
        except Exception as e:
            logging.error(f"Error processing server {server_code}: {str(e)}")
            continue
    
    logging.debug(f"Completed server data collection cycle. Processed {len(servers)} servers, {active_servers} active with {total_players} players.")
    
    current_minute = datetime.now().minute
    if not hasattr(process_servers, 'last_minute') or process_servers.last_minute != current_minute:
        process_servers.last_minute = current_minute
        if active_servers > 0:
            logging.info(f"Active servers: {active_servers} with {total_players} players")

process_servers.last_minute = None

def report_recent_deaths():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, server_id, map_name, wave_number, wave_text
        FROM game_sessions
        WHERE is_active = 1 AND team1_player_count > 0
        ''')
        
        active_sessions = cursor.fetchall()
        
        if not active_sessions:
            conn.close()
            return
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching active sessions for death reporting: {str(e)}")
        return
    
    for session in active_sessions:
        try:
            session_id = session['id']
            
            one_minute_ago = (datetime.now() - timedelta(minutes=1)).isoformat()
            
            conn2 = get_db_connection()
            cursor2 = conn2.cursor()
            
            execute_with_retry(cursor2, '''
            SELECT pr.player_name, ptc.timestamp, ptc.wave_number, ptc.wave_text
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_death = 1 AND ptc.timestamp > ?
            ORDER BY ptc.timestamp DESC
            ''', (session_id, one_minute_ago))
            
            recent_deaths = cursor2.fetchall()
            conn2.close()
            
            if recent_deaths:
                for death in recent_deaths:
                    try:
                        player_name = death['player_name']
                        wave_number = death['wave_number'] or 0
                        wave_text = death['wave_text'] or "Unknown Wave"
                        
                        conn3 = get_db_connection()
                        cursor3 = conn3.cursor()
                        
                        execute_with_retry(cursor3, 'SELECT name FROM servers WHERE id = ?', (session['server_id'],))
                        server = cursor3.fetchone()
                        conn3.close()
                        
                        server_name = server['name'] if server else "Unknown Server"
                        
                        logging.info(f"DEATH: {player_name} died on {server_name} / {session['map_name']} at {wave_text} (Wave {wave_number})")
                    except Exception as e:
                        logging.error(f"Error processing death record: {str(e)}")
        except Exception as e:
            logging.error(f"Error processing session {session.get('id', 'Unknown')} for deaths: {str(e)}")
    
    conn.close()

stop_event = threading.Event()

def monitor_thread():
    while not stop_event.is_set():
        try:
            process_servers()
            report_recent_deaths()
            report_recent_redeems()
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
    
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, server_id, map_name, wave_number
        FROM game_sessions
        WHERE is_active = 1
        ''')
        
        active_sessions = cursor.fetchall()
        
        for session in active_sessions:
            session_id = session['id']
            wave_number = session.get('wave_number') or 0
            
            logging.info(f"Found previously active session {session_id} on startup, marking as ended")
            
            execute_with_retry(cursor, '''
            UPDATE game_sessions
            SET is_active = 0, end_time = ?, session_result = 'Incomplete - Server Restart'
            WHERE id = ?
            ''', (now, session_id))
            
            conn.commit()
            conn.close()
            
            try:
                save_wave_end_snapshot(session_id, wave_number, "Server Restart")
                update_player_playtimes(session_id, now)
                update_death_statistics(session_id)
            except Exception as e:
                logging.error(f"Error finalizing session {session_id} on startup: {str(e)}")
            
            conn = get_db_connection()
            cursor = conn.cursor()
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error cleaning up previous sessions on startup: {str(e)}")
    finally:
        conn.close()
    
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