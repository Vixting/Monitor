import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry, query_one, execute_query
from utils import extract_wave_number
from typing import Optional, Dict, Any

def save_server_status(session_id: Optional[int], player_count: int, wave: Optional[str], timestamp: str) -> None:
    if not session_id:
        return
    
    wave_number = extract_wave_number(wave)
    
    execute_query(
        '''
        INSERT INTO server_status (session_id, timestamp, player_count, wave_text, wave_number)
        VALUES (?, ?, ?, ?, ?)
        ''', 
        (session_id, timestamp, player_count, wave, wave_number)
    )

def save_team_status(session_id: Optional[int], team_id: int, team_name: str, player_count: int, total_score: int, timestamp: str) -> None:
    if not session_id:
        return
    
    execute_query(
        '''
        INSERT INTO team_status (session_id, timestamp, team_id, team_name, player_count, total_score)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', 
        (session_id, timestamp, team_id, team_name, player_count, total_score)
    )

def get_latest_server_status(session_id: int) -> Optional[Dict[str, Any]]:
    return query_one(
        '''
        SELECT timestamp, player_count, wave_text, wave_number 
        FROM server_status 
        WHERE session_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 1
        ''', 
        (session_id,)
    )

def get_latest_team_status(session_id: int, team_id: Optional[int] = None) -> list:
    if team_id is not None:
        return query_one(
            '''
            SELECT team_id, team_name, player_count, total_score 
            FROM team_status 
            WHERE session_id = ? AND team_id = ? 
            ORDER BY timestamp DESC 
            LIMIT 1
            ''', 
            (session_id, team_id)
        )
    else:
        from database import query_all
        return query_all(
            '''
            SELECT ts.* 
            FROM team_status ts
            JOIN (
                SELECT team_id, MAX(timestamp) as latest_time
                FROM team_status
                WHERE session_id = ?
                GROUP BY team_id
            ) latest 
            ON ts.team_id = latest.team_id AND ts.timestamp = latest.latest_time
            WHERE ts.session_id = ?
            ''', 
            (session_id, session_id)
        )