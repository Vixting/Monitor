import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry, query_one, query_all, execute_query
from utils import extract_wave_number
from typing import Optional, Dict, Any, List, Union

def save_wave_end_snapshot(session_id: int, wave_number: int, reason: str) -> Optional[int]:
    timestamp = datetime.now().isoformat()
    
    try:
        wave_end_id = execute_query(
            '''
            INSERT INTO wave_end_records (session_id, wave_number, timestamp, reason)
            VALUES (?, ?, ?, ?)
            ''', 
            (session_id, wave_number, timestamp, reason)
        )
        
        if not wave_end_id:
            return None
        
        player_records = query_all(
            '''
            SELECT id, steam_id, player_name, team_id, current_score, is_bot
            FROM player_records
            WHERE session_id = ?
            ''', 
            (session_id,)
        )
        
        for player in player_records:
            execute_query(
                '''
                INSERT INTO player_wave_scores 
                (wave_end_id, player_id, steam_id, player_name, team_id, score, is_bot)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', 
                (
                    wave_end_id, 
                    player['id'], 
                    player['steam_id'], 
                    player['player_name'], 
                    player['team_id'], 
                    player['current_score'], 
                    player['is_bot']
                )
            )
        
        logging.info(f"Saved wave end snapshot for session {session_id}, wave {wave_number}, reason: {reason}")
        return wave_end_id
    
    except Exception as e:
        logging.error(f"Error saving wave end snapshot for session {session_id}: {str(e)}")
        return None

def get_wave_end_records(session_id: int) -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT id, wave_number, timestamp, reason
        FROM wave_end_records
        WHERE session_id = ?
        ORDER BY wave_number, timestamp
        ''', 
        (session_id,)
    )

def get_player_wave_scores(wave_end_id: int) -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT player_id, steam_id, player_name, team_id, score
        FROM player_wave_scores
        WHERE wave_end_id = ?
        ORDER BY score DESC
        ''', 
        (wave_end_id,)
    )

def get_wave_winners(session_id: int, wave_number: int) -> List[Dict[str, Any]]:
    wave_record = query_one(
        '''
        SELECT id FROM wave_end_records
        WHERE session_id = ? AND wave_number = ?
        ORDER BY timestamp DESC LIMIT 1
        ''', 
        (session_id, wave_number)
    )
    
    if not wave_record:
        return []
    
    return query_all(
        '''
        SELECT player_name, steam_id, score
        FROM player_wave_scores
        WHERE wave_end_id = ? AND is_bot = 0 AND team_id = 4
        ORDER BY score DESC LIMIT 5
        ''', 
        (wave_record['id'],)
    )

def get_wave_summary(session_id: int, wave_number: int) -> Optional[Dict[str, Any]]:
    wave_record = query_one(
        '''
        SELECT id, timestamp, reason FROM wave_end_records
        WHERE session_id = ? AND wave_number = ?
        ORDER BY timestamp DESC LIMIT 1
        ''', 
        (session_id, wave_number)
    )
    
    if not wave_record:
        return None
    
    wave_end_id = wave_record['id']
    
    team_stats = query_all(
        '''
        SELECT team_id, COUNT(*) as player_count, SUM(score) as total_score
        FROM player_wave_scores
        WHERE wave_end_id = ?
        GROUP BY team_id
        ''', 
        (wave_end_id,)
    )
    
    top_players = query_all(
        '''
        SELECT player_name, steam_id, team_id, score
        FROM player_wave_scores
        WHERE wave_end_id = ? AND is_bot = 0
        ORDER BY score DESC LIMIT 10
        ''', 
        (wave_end_id,)
    )
    
    return {
        'wave_number': wave_number,
        'timestamp': wave_record['timestamp'],
        'reason': wave_record['reason'],
        'team_stats': team_stats,
        'top_players': top_players
    }

def get_all_wave_summaries(session_id: int) -> List[Dict[str, Any]]:
    wave_records = query_all(
        '''
        SELECT id, wave_number, timestamp, reason
        FROM wave_end_records
        WHERE session_id = ?
        ORDER BY wave_number
        ''', 
        (session_id,)
    )
    
    summaries = []
    
    for record in wave_records:
        wave_end_id = record['id']
        wave_number = record['wave_number']
        
        team_stats = query_all(
            '''
            SELECT team_id, COUNT(*) as player_count, SUM(score) as total_score
            FROM player_wave_scores
            WHERE wave_end_id = ?
            GROUP BY team_id
            ''', 
            (wave_end_id,)
        )
        
        summary = {
            'wave_number': wave_number,
            'timestamp': record['timestamp'],
            'reason': record['reason'],
            'team_stats': team_stats
        }
        
        summaries.append(summary)
    
    return summaries

def get_player_wave_history(session_id: int, steam_id: str) -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT wer.wave_number, wer.timestamp, wer.reason, pws.score, pws.team_id
        FROM player_wave_scores pws
        JOIN wave_end_records wer ON pws.wave_end_id = wer.id
        WHERE wer.session_id = ? AND pws.steam_id = ?
        ORDER BY wer.wave_number
        ''', 
        (session_id, steam_id)
    )

def get_latest_wave(session_id: int) -> Optional[Dict[str, Any]]:
    return query_one(
        '''
        SELECT wave_number, wave_text 
        FROM game_sessions 
        WHERE id = ?
        ''', 
        (session_id,)
    )