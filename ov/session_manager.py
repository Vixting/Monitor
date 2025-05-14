import logging
from datetime import datetime, timedelta
from database import get_db_connection, execute_with_retry, query_one, query_all, execute_query
from utils import extract_wave_number
from player_manager import update_player_playtimes, update_death_statistics
from wave_manager import save_wave_end_snapshot
from typing import Optional, Dict, Any, List, Union, Tuple

SESSION_TIMEOUT_MINUTES = 15

TEAM_NAMES = {
    1: "Team 1",
    2: "Team 2", 
    3: "Undead",
    4: "Humans",
    1002: "Spectators"
}

def get_active_session(
    server_id: int, 
    map_name: str, 
    current_wave: Optional[str], 
    player_count: int, 
    max_players: int, 
    team_data: Optional[Dict[int, int]] = None
) -> Optional[int]:
    now = datetime.now().isoformat()
    current_wave_number = extract_wave_number(current_wave)
    
    active_server_session = query_one(
        '''
        SELECT id, map_name, wave_number, wave_text, peak_player_count, is_active, end_time
        FROM game_sessions 
        WHERE server_id = ? AND is_active = 1
        ORDER BY start_time DESC LIMIT 1
        ''', 
        (server_id,)
    )
    
    if active_server_session and active_server_session['map_name'] != map_name:
        old_session_id = active_server_session['id']
        previous_wave_number = active_server_session['wave_number']
        old_map_name = active_server_session['map_name']
        
        session_result = determine_session_result(old_session_id, previous_wave_number)
        
        execute_query(
            '''
            UPDATE game_sessions 
            SET is_active = 0, end_time = ?, session_result = ?
            WHERE id = ?
            ''', 
            (now, session_result + " - Map Changed", old_session_id)
        )
        
        save_wave_end_snapshot(old_session_id, previous_wave_number or 0, "Map Changed")
        update_player_playtimes(old_session_id, now)
        update_death_statistics(old_session_id)
        
        logging.info(f"MAP: Change detected for server_id {server_id}: {old_map_name} -> {map_name}")
        
        return create_new_session(server_id, map_name, current_wave, current_wave_number, player_count, max_players, team_data, now)
    
    session = query_one(
        '''
        SELECT id, map_name, wave_number, wave_text, peak_player_count, is_active, end_time
        FROM game_sessions 
        WHERE server_id = ? AND map_name = ? AND is_active = 1
        ORDER BY start_time DESC LIMIT 1
        ''', 
        (server_id, map_name)
    )
    
    if not session:
        return create_new_session(server_id, map_name, current_wave, current_wave_number, player_count, max_players, team_data, now)
    
    session_id = session['id']
    previous_wave_number = session['wave_number']
    
    if session['end_time'] and session['end_time'] != '[NULL]':
        logging.info(f"Session {session_id} already has an end time set, creating new session")
        return create_new_session(server_id, map_name, current_wave, current_wave_number, player_count, max_players, team_data, now)
    
    if current_wave_number is not None and previous_wave_number is not None:
        if current_wave_number < previous_wave_number:
            same_wave_count = query_one(
                '''
                SELECT COUNT(*) as count FROM server_status
                WHERE session_id = ? AND wave_number = ?
                ''', 
                (session_id, current_wave_number)
            )['count']
            
            if same_wave_count > 0 or current_wave_number == 1:
                logging.info(f"WAVE: Round restart detected for server_id {server_id} on {map_name}: {previous_wave_number} -> {current_wave_number}")
                
                session_result = determine_session_result(session_id, previous_wave_number)
                
                execute_query(
                    '''
                    UPDATE game_sessions 
                    SET is_active = 0, end_time = ?, session_result = ?
                    WHERE id = ?
                    ''', 
                    (now, session_result + " - Round Restarted", session_id)
                )
                
                save_wave_end_snapshot(session_id, previous_wave_number, "Round Restarted")
                update_player_playtimes(session_id, now)
                update_death_statistics(session_id)
                
                return create_new_session(server_id, map_name, current_wave, current_wave_number, player_count, max_players, team_data, now)
            else:
                logging.info(f"WAVE: Reset without restart detected for session {session_id}: {previous_wave_number} -> {current_wave_number}")
        elif current_wave_number > previous_wave_number:
            if previous_wave_number > 0:
                save_wave_end_snapshot(session_id, previous_wave_number, "Wave Completed")
            
            logging.info(f"WAVE: Progression detected for session {session_id}: {previous_wave_number} -> {current_wave_number}")
    else:
        last_active = query_one(
            '''
            SELECT timestamp FROM server_status 
            WHERE session_id = ? AND player_count > 0 
            ORDER BY timestamp DESC LIMIT 1
            ''', 
            (session_id,)
        )
        
        if last_active:
            last_active_time = datetime.fromisoformat(last_active['timestamp'])
            now_time = datetime.fromisoformat(now)
            
            if (now_time - last_active_time) > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                logging.info(f"SERVER: ID {server_id} was inactive for over {SESSION_TIMEOUT_MINUTES} minutes")
                
                execute_query(
                    '''
                    UPDATE game_sessions 
                    SET is_active = 0, end_time = ?, session_result = 'Loss - Timeout' 
                    WHERE id = ?
                    ''', 
                    (now, session_id)
                )
                
                save_wave_end_snapshot(session_id, previous_wave_number or 0, "Timeout")
                
                return create_new_session(server_id, map_name, current_wave, current_wave_number, player_count, max_players, team_data, now)
    
    update_session(session_id, current_wave, current_wave_number, player_count, team_data)
    return session_id

def create_new_session(
    server_id: int, 
    map_name: str, 
    current_wave: Optional[str], 
    current_wave_number: Optional[int], 
    player_count: int, 
    max_players: int, 
    team_data: Optional[Dict[int, int]], 
    now: str
) -> int:

    team1_count = team2_count = 0
    team_json_data = {}
    
    if team_data:
        team1_count = team_data.get(4, 0)  
        team2_count = team_data.get(3, 0)  
        
        for team_id, count in team_data.items():
            team_name = TEAM_NAMES.get(team_id, f"Team {team_id}")
            team_json_data[team_id] = {
                "name": team_name,
                "count": count
            }
    
    
    session_id = execute_query(
        '''
        INSERT INTO game_sessions 
        (server_id, map_name, start_time, max_players, peak_player_count, wave_text, wave_number, is_active, 
         team1_player_count, team2_player_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        ''', 
        (server_id, map_name, now, max_players, player_count, current_wave, current_wave_number, 
         team1_count, team2_count)
    )
    
    logging.info(f"MAP: Created new game session {session_id} for server ID {server_id} on map {map_name}")
    
    if team_data:
        team_info = ", ".join([f"{TEAM_NAMES.get(team_id, f'Team {team_id}')}: {count}" for team_id, count in team_data.items()])
        logging.debug(f"TEAMS: New session {session_id} teams - {team_info}")
    
    return session_id

def update_session(
    session_id: int, 
    current_wave: Optional[str], 
    current_wave_number: Optional[int], 
    player_count: int, 
    team_data: Optional[Dict[int, int]]
) -> None:
    session = query_one("SELECT peak_player_count FROM game_sessions WHERE id = ?", (session_id,))
    peak_players = max(session['peak_player_count'], player_count)
    
    if team_data:
        team1_count = team_data.get(4, 0)  # Humans
        team2_count = team_data.get(3, 0)  # Undead
        
        if team1_count > 0 or team2_count > 0:
            logging.debug(f"TEAMS: Session {session_id} - Humans: {team1_count}, Undead: {team2_count}")
        
        execute_query(
            '''
            UPDATE game_sessions 
            SET wave_text = ?, wave_number = ?, peak_player_count = ?,
                team1_player_count = ?, team2_player_count = ?
            WHERE id = ?
            ''', 
            (current_wave, current_wave_number, peak_players, team1_count, team2_count, session_id)
        )
    else:
        execute_query(
            '''
            UPDATE game_sessions 
            SET wave_text = ?, wave_number = ?, peak_player_count = ? 
            WHERE id = ?
            ''', 
            (current_wave, current_wave_number, peak_players, session_id)
        )
    
    logging.debug(f"Updated existing session {session_id}")

def determine_session_result(session_id: int, wave_number: Optional[int]) -> str:
    session_data = query_one(
        '''
        SELECT team1_player_count
        FROM game_sessions
        WHERE id = ?
        ''', 
        (session_id,)
    )
    
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

def get_session_deaths(session_id: int) -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT pr.player_name, ptc.timestamp, ptc.wave_number, ptc.wave_text
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_death = 1
        ORDER BY ptc.timestamp
        ''', 
        (session_id,)
    )

def get_active_sessions() -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT gs.id, gs.server_id, s.name as server_name, gs.map_name, 
               gs.start_time, gs.wave_number, gs.wave_text,
               gs.team1_player_count, gs.team2_player_count
        FROM game_sessions gs
        JOIN servers s ON gs.server_id = s.id
        WHERE gs.is_active = 1
        ORDER BY gs.start_time DESC
        ''')

def end_session(session_id: int, result: str) -> None:
    now = datetime.now().isoformat()
    session = query_one(
        '''
        SELECT wave_number 
        FROM game_sessions 
        WHERE id = ?
        ''', 
        (session_id,)
    )
    
    if not session:
        return
    
    wave_number = session['wave_number'] or 0
    
    execute_query(
        '''
        UPDATE game_sessions 
        SET is_active = 0, end_time = ?, session_result = ?
        WHERE id = ?
        ''', 
        (now, result, session_id)
    )
    
    save_wave_end_snapshot(session_id, wave_number, f"Manual End: {result}")
    update_player_playtimes(session_id, now)
    update_death_statistics(session_id)
    
    logging.info(f"SERVER: Manually ended session {session_id} with result: {result}")

def get_session_stats(session_id: int) -> Dict[str, Any]:
    session = query_one(
        '''
        SELECT gs.*, s.name as server_name
        FROM game_sessions gs
        JOIN servers s ON gs.server_id = s.id
        WHERE gs.id = ?
        ''', 
        (session_id,)
    )
    
    if not session:
        return {}
    
    player_count = query_one(
        '''
        SELECT COUNT(*) as count
        FROM player_records
        WHERE session_id = ? AND is_bot = 0
        ''', 
        (session_id,)
    )['count']
    
    deaths = query_one(
        '''
        SELECT COUNT(*) as count
        FROM player_team_changes
        WHERE session_id = ? AND is_death = 1
        ''', 
        (session_id,)
    )['count']
    
    redeems = query_one(
        '''
        SELECT COUNT(*) as count
        FROM player_team_changes
        WHERE session_id = ? AND is_redeem = 1
        ''', 
        (session_id,)
    )['count']
    
    # Get all team data for this session
    team_status = query_all(
        '''
        SELECT ts.team_id, ts.team_name, ts.player_count, ts.total_score
        FROM team_status ts
        JOIN (
            SELECT team_id, MAX(timestamp) as latest_time
            FROM team_status
            WHERE session_id = ?
            GROUP BY team_id
        ) latest ON ts.team_id = latest.team_id AND ts.timestamp = latest.latest_time
        WHERE ts.session_id = ?
        ''', 
        (session_id, session_id)
    )
    
    teams = {}
    if team_status:
        for team in team_status:
            team_id = team['team_id']
            teams[team_id] = {
                'name': team['team_name'],
                'count': team['player_count'],
                'score': team['total_score']
            }
    
    return {
        'id': session['id'],
        'server_name': session['server_name'],
        'map_name': session['map_name'],
        'start_time': session['start_time'],
        'end_time': session['end_time'],
        'is_active': session['is_active'],
        'wave_number': session['wave_number'],
        'max_players': session['max_players'],
        'peak_player_count': session['peak_player_count'],
        'team1_count': session['team1_player_count'],  # Humans
        'team2_count': session['team2_player_count'],  # Undead
        'teams': teams,  # All teams with details
        'player_count': player_count,
        'deaths': deaths,
        'redeems': redeems,
        'session_result': session['session_result']
    }