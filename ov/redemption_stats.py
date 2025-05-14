import logging
from datetime import datetime, timedelta
from database import query_one, query_all, execute_query
from typing import Optional, Dict, Any, List, Union, Set

_reported_death_ids = set()
_reported_redeem_ids = set()

try:
    from main import death_logger_instance as death_logger
    from main import redeem_logger_instance as redeem_logger
except ImportError:
    death_logger = logging.getLogger('death_events')
    redeem_logger = logging.getLogger('redeem_events')
    if not death_logger.handlers:
        death_logger = logging.getLogger()
    if not redeem_logger.handlers:
        redeem_logger = logging.getLogger()

def get_player_redemption_stats(steam_id: Optional[str] = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    if steam_id:
        return query_one(
            '''
            SELECT prs.*, 
                   COUNT(ptc.id) as recent_redeems,
                   MAX(ptc.timestamp) as last_redeem_time
            FROM player_redeem_stats prs
            LEFT JOIN player_team_changes ptc ON prs.steam_id = (
                SELECT pr.steam_id 
                FROM player_records pr 
                WHERE pr.id = ptc.player_id
            )
            WHERE prs.steam_id = ? AND ptc.is_redeem = 1
            GROUP BY prs.id
            ''', 
            (steam_id,)
        )
    else:
        return query_all(
            '''
            SELECT * FROM player_redeem_stats
            ORDER BY total_redeems DESC
            LIMIT 100
            '''
        )

def get_session_redeems(session_id: int, wave_number: Optional[int] = None) -> List[Dict[str, Any]]:
    if wave_number is not None:
        return query_all(
            '''
            SELECT pr.player_name, pr.steam_id, pr.is_bot, ptc.timestamp, 
                   ptc.wave_number, ptc.wave_text, ptc.old_team_id, ptc.new_team_id
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND ptc.wave_number = ?
            ORDER BY ptc.timestamp
            ''', 
            (session_id, wave_number)
        )
    else:
        return query_all(
            '''
            SELECT pr.player_name, pr.steam_id, pr.is_bot, ptc.timestamp, 
                   ptc.wave_number, ptc.wave_text, ptc.old_team_id, ptc.new_team_id
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1
            ORDER BY ptc.wave_number, ptc.timestamp
            ''', 
            (session_id,)
        )

def get_session_team_composition(session_id: int, wave_number: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if wave_number is not None:
        wave_record = query_one(
            '''
            SELECT id FROM wave_end_records
            WHERE session_id = ? AND wave_number = ?
            ORDER BY timestamp DESC LIMIT 1
            ''', 
            (session_id, wave_number)
        )
        
        if wave_record:
            wave_end_id = wave_record['id']
            
            results = query_all(
                '''
                SELECT team_id, COUNT(*) as player_count, SUM(score) as total_score
                FROM player_wave_scores
                WHERE wave_end_id = ?
                GROUP BY team_id
                ''', 
                (wave_end_id,)
            )
            
            if results:
                composition = {
                    'wave_number': wave_number,
                    'teams': {}
                }
                
                for result in results:
                    team_id = result['team_id']
                    if team_id is not None:
                        team_name = get_team_name(team_id)
                        composition['teams'][team_id] = {
                            'name': team_name,
                            'player_count': result['player_count'],
                            'total_score': result['total_score']
                        }
                
                return composition
    
    results = query_all(
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
    
    if results:
        composition = {
            'teams': {}
        }
        
        session_data = query_one(
            '''
            SELECT wave_number, wave_text FROM game_sessions
            WHERE id = ?
            ''', 
            (session_id,)
        )
        
        if session_data:
            composition['wave_number'] = session_data['wave_number']
            composition['wave_text'] = session_data['wave_text']
        
        for result in results:
            team_id = result['team_id']
            composition['teams'][team_id] = {
                'name': result['team_name'],
                'player_count': result['player_count'],
                'total_score': result['total_score']
            }
        
        return composition
    
    return None

def get_team_name(team_id: int) -> str:
    team_names = {
        1: "Team 1",
        2: "Team 2",
        3: "Undead",
        4: "Humans",
        1002: "Spectators"
    }
    return team_names.get(team_id, f"Team {team_id}")

def report_recent_redeems() -> None:
    global _reported_redeem_ids
    
    active_sessions = query_all(
        '''
        SELECT id, server_id, map_name, wave_number, wave_text
        FROM game_sessions
        WHERE is_active = 1
        '''
    )
    
    if not active_sessions:
        return
    
    one_minute_ago = (datetime.now() - timedelta(minutes=1)).isoformat()
    
    for session in active_sessions:
        session_id = session['id']
        
        recent_redeems = query_all(
            '''
            SELECT ptc.id, pr.player_name, ptc.timestamp, ptc.wave_number, ptc.wave_text
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND ptc.timestamp > ?
            ORDER BY ptc.timestamp DESC
            ''', 
            (session_id, one_minute_ago)
        )
        
        if recent_redeems:
            for redeem in recent_redeems:
                redeem_id = redeem['id']
                
                if redeem_id in _reported_redeem_ids:
                    continue
                
                _reported_redeem_ids.add(redeem_id)
                
                player_name = redeem['player_name']
                wave_number = redeem['wave_number'] or 0
                wave_text = redeem['wave_text'] or "Unknown Wave"
                
                server_name = get_server_name(session['server_id'])
                
                redeem_logger.info(f"REDEEM: {player_name} redeemed on {server_name} / {session['map_name']} at {wave_text} (Wave {wave_number})")
    
    if len(_reported_redeem_ids) > 1000:
        _reported_redeem_ids = set(list(_reported_redeem_ids)[-1000:])

def get_server_name(server_id: int) -> str:
    server = query_one('SELECT name FROM servers WHERE id = ?', (server_id,))
    return server['name'] if server else "Unknown Server"

def report_recent_deaths() -> None:
    global _reported_death_ids
    
    active_sessions = query_all(
        '''
        SELECT id, server_id, map_name, wave_number, wave_text
        FROM game_sessions
        WHERE is_active = 1 AND team1_player_count > 0
        '''
    )
    
    if not active_sessions:
        return
    
    one_minute_ago = (datetime.now() - timedelta(minutes=1)).isoformat()
    
    for session in active_sessions:
        session_id = session['id']
        
        recent_deaths = query_all(
            '''
            SELECT ptc.id, pr.player_name, ptc.timestamp, ptc.wave_number, ptc.wave_text
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_death = 1 AND ptc.timestamp > ?
            ORDER BY ptc.timestamp DESC
            ''', 
            (session_id, one_minute_ago)
        )
        
        if recent_deaths:
            server_name = get_server_name(session['server_id'])
            
            for death in recent_deaths:
                death_id = death['id']
                
                if death_id in _reported_death_ids:
                    continue
                
                _reported_death_ids.add(death_id)
                
                player_name = death['player_name']
                wave_number = death['wave_number'] or 0
                wave_text = death['wave_text'] or "Unknown Wave"
                
                death_logger.info(f"DEATH: {player_name} died on {server_name} / {session['map_name']} at {wave_text} (Wave {wave_number})")
    
    if len(_reported_death_ids) > 1000:
        _reported_death_ids = set(list(_reported_death_ids)[-1000:])

def get_top_redeemers(limit: int = 10) -> List[Dict[str, Any]]:
    return query_all(
        '''
        SELECT steam_id, player_name, total_redeems, last_updated
        FROM player_redeem_stats
        ORDER BY total_redeems DESC
        LIMIT ?
        ''', 
        (limit,)
    )