import logging
from database import get_db_connection, execute_with_retry
from util import extract_wave_number

def save_server_status(session_id, player_count, wave, timestamp):
    if not session_id:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        wave_number = extract_wave_number(wave)
        
        execute_with_retry(cursor, '''
        INSERT INTO server_status (session_id, timestamp, player_count, wave_text, wave_number)
        VALUES (?, ?, ?, ?, ?)
        ''', (session_id, timestamp, player_count, wave, wave_number))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.debug(f"Database error saving server status for session {session_id}: {str(e)}")
    finally:
        conn.close()

def save_team_status(session_id, team_id, team_name, player_count, total_score, timestamp):
    if not session_id:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        INSERT INTO team_status (session_id, timestamp, team_id, team_name, player_count, total_score)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (session_id, timestamp, team_id, team_name, player_count, total_score))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.debug(f"Database error saving team status for session {session_id}: {str(e)}")
    finally:
        conn.close()