import logging
from datetime import datetime, timedelta
from database import get_db_connection, execute_with_retry
from util import extract_wave_number
from player_manager import update_player_playtimes, update_death_statistics

SESSION_TIMEOUT_MINUTES = 15

def save_wave_end_snapshot(session_id, wave_number, reason):
    if not session_id:
        logging.warning(f"Attempted to save wave end snapshot with invalid session_id={session_id}")
        return
        
    if wave_number is None:
        wave_number = 0
        if reason != "Timeout" and reason != "Server Restart":
            reason = "Game Start"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id FROM wave_end_records 
        WHERE session_id = ? AND wave_number = ? AND reason = ?
        ''', (session_id, wave_number, reason))
        
        existing = cursor.fetchone()
        if existing:
            logging.info(f"Wave end record already exists for session {session_id}, wave {wave_number} ({reason})")
            conn.close()
            return
        
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(f"Error checking for existing wave end record: {str(e)}")
        return
    
    # Create the wave_end_record
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    
    try:
        logging.info(f"Starting wave end snapshot for session {session_id}, wave {wave_number} ({reason})")
        
        execute_with_retry(cursor, '''
        CREATE TABLE IF NOT EXISTS wave_end_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            wave_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            reason TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES game_sessions (id)
        )
        ''')
        
        execute_with_retry(cursor, '''
        CREATE TABLE IF NOT EXISTS player_wave_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wave_end_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            steam_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            team_id INTEGER,
            score INTEGER NOT NULL,
            is_bot BOOLEAN NOT NULL,
            FOREIGN KEY (wave_end_id) REFERENCES wave_end_records (id),
            FOREIGN KEY (player_id) REFERENCES player_records (id)
        )
        ''')
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        logging.error(f"Error creating tables for wave end snapshot: {str(e)}")
        return
    
    # Create the wave end record
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        INSERT INTO wave_end_records (session_id, wave_number, timestamp, reason)
        VALUES (?, ?, ?, ?)
        ''', (session_id, wave_number, now, reason))
        
        wave_end_id = cursor.lastrowid
        conn.commit()
        
        if not wave_end_id:
            conn.close()
            logging.error(f"Failed to create wave_end_record for session {session_id}, wave {wave_number}")
            return
    except Exception as e:
        conn.rollback()
        conn.close()
        logging.error(f"Error creating wave end record: {str(e)}")
        return
    
    # Get player records
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, steam_id, player_name, team_id, current_score, is_bot
        FROM player_records
        WHERE session_id = ?
        ''', (session_id,))
        
        players = cursor.fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching player records: {str(e)}")
        return
    
    # Insert player wave scores in batches
    player_count = 0
    batch_size = 10
    player_batches = [players[i:i + batch_size] for i in range(0, len(players), batch_size)]
    
    for batch in player_batches:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            for player in batch:
                player_id = player['id']
                steam_id = player['steam_id']
                player_name = player['player_name']
                team_id = player['team_id']
                score = player['current_score']
                is_bot = player['is_bot']
                
                execute_with_retry(cursor, '''
                INSERT INTO player_wave_scores (
                    wave_end_id, player_id, steam_id, player_name, team_id, score, is_bot
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (wave_end_id, player_id, steam_id, player_name, team_id, score, is_bot))
                
                player_count += 1
            
            conn.commit()
            conn.close()
        except Exception as e:
            conn.rollback()
            conn.close()
            logging.error(f"Error saving wave scores for a batch: {str(e)}")
            continue
    
    if player_count > 0:
        logging.info(f"Saved wave {wave_number} end snapshot for session {session_id} ({reason}) with {player_count} player records")
    else:
        logging.warning(f"No player records saved for wave {wave_number} end snapshot in session {session_id}")
        
def get_active_session(server_id, map_name, current_wave, player_count, max_players, team_data=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    session_id = None

    current_wave_number = extract_wave_number(current_wave)
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, map_name, wave_number, wave_text, peak_player_count, is_active, end_time
        FROM game_sessions 
        WHERE server_id = ? AND is_active = 1
        ORDER BY start_time DESC LIMIT 1
        ''', (server_id,))
        
        active_server_session = cursor.fetchone()
        
        if active_server_session and active_server_session['map_name'] != map_name:
            old_session_id = active_server_session['id']
            previous_wave_number = active_server_session['wave_number']
            old_map_name = active_server_session['map_name']
            
            session_result = determine_session_result(old_session_id, previous_wave_number)
            
            execute_with_retry(cursor, '''
            UPDATE game_sessions 
            SET is_active = 0, end_time = ?, session_result = ?
            WHERE id = ?
            ''', (now, session_result + " - Map Changed", old_session_id))
            
            conn.commit()
            conn.close()
            
            save_wave_end_snapshot(old_session_id, previous_wave_number or 0, "Map Changed")
            update_player_playtimes(old_session_id, now)
            update_death_statistics(old_session_id)
            
            logging.info(f"Map change detected for server_id {server_id}: {old_map_name} -> {map_name}")
            
            new_session_needed = True
            conn = get_db_connection()
            cursor = conn.cursor()
        else:
            execute_with_retry(cursor, '''
            SELECT id, map_name, wave_number, wave_text, peak_player_count, is_active, end_time
            FROM game_sessions 
            WHERE server_id = ? AND map_name = ? AND is_active = 1
            ORDER BY start_time DESC LIMIT 1
            ''', (server_id, map_name))
            
            session = cursor.fetchone()
            
            new_session_needed = True
            
            if session:
                session_id = session['id']
                previous_wave_number = session['wave_number']
                
                if session['end_time'] and session['end_time'] != '[NULL]':
                    new_session_needed = True
                    logging.info(f"Session {session_id} already has an end time set, creating new session")
                elif current_wave_number is not None and previous_wave_number is not None:
                    if current_wave_number < previous_wave_number:
                        execute_with_retry(cursor, '''
                        SELECT COUNT(*) as count FROM server_status
                        WHERE session_id = ? AND wave_number = ?
                        ''', (session_id, current_wave_number))
                        
                        same_wave_count = cursor.fetchone()['count']
                        
                        if same_wave_count > 0 or current_wave_number == 1:
                            logging.info(f"Round restart detected for server_id {server_id} on {map_name}: {previous_wave_number} -> {current_wave_number}")
                            
                            session_result = determine_session_result(session_id, previous_wave_number)
                            
                            execute_with_retry(cursor, '''
                            UPDATE game_sessions 
                            SET is_active = 0, end_time = ?, session_result = ?
                            WHERE id = ?
                            ''', (now, session_result + " - Round Restarted", session_id))
                            
                            conn.commit()
                            conn.close()
                            
                            save_wave_end_snapshot(session_id, previous_wave_number, "Round Restarted")
                            update_player_playtimes(session_id, now)
                            update_death_statistics(session_id)
                            
                            new_session_needed = True
                            conn = get_db_connection()
                            cursor = conn.cursor()
                        else:
                            logging.info(f"Wave reset without restart detected for session {session_id}: {previous_wave_number} -> {current_wave_number}")
                            new_session_needed = False
                    elif current_wave_number > previous_wave_number:
                        conn.commit()
                        conn.close()
                        
                        if previous_wave_number > 0:
                            save_wave_end_snapshot(session_id, previous_wave_number, "Wave Completed")
                        
                        logging.info(f"Wave progression detected for session {session_id}: {previous_wave_number} -> {current_wave_number}")
                        
                        new_session_needed = False
                        conn = get_db_connection()
                        cursor = conn.cursor()
                    else:
                        new_session_needed = False
                else:
                    execute_with_retry(cursor, '''
                    SELECT timestamp FROM server_status 
                    WHERE session_id = ? AND player_count > 0 
                    ORDER BY timestamp DESC LIMIT 1
                    ''', (session_id,))
                    
                    last_active = cursor.fetchone()
                    
                    if last_active:
                        last_active_time = datetime.fromisoformat(last_active['timestamp'])
                        now_time = datetime.fromisoformat(now)
                        
                        if (now_time - last_active_time) > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                            new_session_needed = True
                            logging.info(f"Server ID {server_id} was inactive for over {SESSION_TIMEOUT_MINUTES} minutes")
                            
                            execute_with_retry(cursor, '''
                            UPDATE game_sessions 
                            SET is_active = 0, end_time = ?, session_result = 'Loss - Timeout' 
                            WHERE id = ?
                            ''', (now, session_id))
                            
                            conn.commit()
                            conn.close()
                            
                            save_wave_end_snapshot(session_id, previous_wave_number or 0, "Timeout")
                            
                            conn = get_db_connection()
                            cursor = conn.cursor()
                        else:
                            new_session_needed = False
                    else:
                        new_session_needed = False
        
        if new_session_needed:
            if session_id:
                execute_with_retry(cursor, '''
                UPDATE game_sessions SET is_active = 0, end_time = ? 
                WHERE id = ?
                ''', (now, session_id))
                
                conn.commit()
                conn.close()
                
                update_player_playtimes(session_id, now)
                update_death_statistics(session_id)
                
                conn = get_db_connection()
                cursor = conn.cursor()
            
            team1_count = 0
            team2_count = 0
            team3_count = 0
            team4_count = 0
            team1002_count = 0
            
            if team_data:
                team1_count = team_data.get(1, 0)
                team2_count = team_data.get(2, 0)
                team3_count = team_data.get(3, 0)
                team4_count = team_data.get(4, 0)
                team1002_count = team_data.get(1002, 0)
            
            execute_with_retry(cursor, '''
            INSERT INTO game_sessions 
            (server_id, map_name, start_time, max_players, peak_player_count, wave_text, wave_number, is_active, 
             team1_player_count, team2_player_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ''', (server_id, map_name, now, max_players, player_count, current_wave, current_wave_number, 
                 team4_count, team3_count))
            
            session_id = cursor.lastrowid
            logging.info(f"Created new game session {session_id} for server ID {server_id} on map {map_name}")
        else:
            peak_players = max(session['peak_player_count'], player_count)
            
            if team_data:
                team4_count = team_data.get(4, 0)
                team3_count = team_data.get(3, 0)
                
                execute_with_retry(cursor, '''
                UPDATE game_sessions 
                SET wave_text = ?, wave_number = ?, peak_player_count = ?,
                    team1_player_count = ?, team2_player_count = ?
                WHERE id = ?
                ''', (current_wave, current_wave_number, peak_players, team4_count, team3_count, session_id))
            else:
                execute_with_retry(cursor, '''
                UPDATE game_sessions 
                SET wave_text = ?, wave_number = ?, peak_player_count = ? 
                WHERE id = ?
                ''', (current_wave, current_wave_number, peak_players, session_id))
            
            logging.debug(f"Updated existing session {session_id} for server ID {server_id} on map {map_name}")
        
        conn.commit()
        return session_id
    
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Database error managing session for server ID {server_id}: {str(e)}")
        return None
    finally:
        if conn:
            conn.close()

def determine_session_result(session_id, wave_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT team1_player_count
        FROM game_sessions
        WHERE id = ?
        ''', (session_id,))
        
        session_data = cursor.fetchone()
        
        if session_data:
            survivors = session_data['team1_player_count']
            
            if wave_number is None:
                return "Unknown Result - Missing Wave Data"
            elif wave_number == 6 and survivors > 0:
                return "Win - Completed Wave 6"
            elif wave_number >= 6:
                return "Loss - No Survivors"
            else:
                return f"Loss - Ended on Wave {wave_number}"
                
        return "Unknown Result"
    
    except Exception as e:
        logging.error(f"Error determining session result for session {session_id}: {str(e)}")
        return "Error Determining Result"
    finally:
        conn.close()

def get_session_deaths(session_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT pr.player_name, ptc.timestamp, ptc.wave_number, ptc.wave_text
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_death = 1
        ORDER BY ptc.timestamp
        ''', (session_id,))
        
        deaths = cursor.fetchall()
        return deaths
    except Exception as e:
        logging.error(f"Error getting session deaths for session {session_id}: {str(e)}")
        return []
    finally:
        conn.close()