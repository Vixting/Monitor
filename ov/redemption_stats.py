import logging
from datetime import datetime, timedelta
from database import get_db_connection, execute_with_retry

def get_player_redemption_stats(steam_id=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if steam_id:
            execute_with_retry(cursor, '''
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
            ''', (steam_id,))
            
            player_stats = cursor.fetchone()
            return player_stats
        else:
            execute_with_retry(cursor, '''
            SELECT * FROM player_redeem_stats
            ORDER BY total_redeems DESC
            LIMIT 100
            ''')
            
            return cursor.fetchall()
    
    except Exception as e:
        logging.error(f"Error getting player redemption stats: {str(e)}")
        return []
    finally:
        conn.close()

def get_session_redeems(session_id, wave_number=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if wave_number is not None:
            execute_with_retry(cursor, '''
            SELECT pr.player_name, pr.steam_id, pr.is_bot, ptc.timestamp, 
                   ptc.wave_number, ptc.wave_text, ptc.old_team_id, ptc.new_team_id
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND ptc.wave_number = ?
            ORDER BY ptc.timestamp
            ''', (session_id, wave_number))
        else:
            execute_with_retry(cursor, '''
            SELECT pr.player_name, pr.steam_id, pr.is_bot, ptc.timestamp, 
                   ptc.wave_number, ptc.wave_text, ptc.old_team_id, ptc.new_team_id
            FROM player_team_changes ptc
            JOIN player_records pr ON ptc.player_id = pr.id
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1
            ORDER BY ptc.wave_number, ptc.timestamp
            ''', (session_id,))
        
        return cursor.fetchall()
    
    except Exception as e:
        logging.error(f"Error getting session redemptions for session {session_id}: {str(e)}")
        return []
    finally:
        conn.close()

def get_session_team_composition(session_id, wave_number=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if wave_number is not None:
            execute_with_retry(cursor, '''
            SELECT id FROM wave_end_records
            WHERE session_id = ? AND wave_number = ?
            ORDER BY timestamp DESC LIMIT 1
            ''', (session_id, wave_number))
            
            wave_record = cursor.fetchone()
            
            if wave_record:
                wave_end_id = wave_record['id']
                
                execute_with_retry(cursor, '''
                SELECT team_id, COUNT(*) as player_count, SUM(score) as total_score
                FROM player_wave_scores
                WHERE wave_end_id = ?
                GROUP BY team_id
                ''', (wave_end_id,))
                
                results = cursor.fetchall()
                
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
        
        execute_with_retry(cursor, '''
        SELECT ts.team_id, ts.team_name, ts.player_count, ts.total_score
        FROM team_status ts
        JOIN (
            SELECT team_id, MAX(timestamp) as latest_time
            FROM team_status
            WHERE session_id = ?
            GROUP BY team_id
        ) latest ON ts.team_id = latest.team_id AND ts.timestamp = latest.latest_time
        WHERE ts.session_id = ?
        ''', (session_id, session_id))
        
        results = cursor.fetchall()
        
        if results:
            composition = {
                'teams': {}
            }
            
            execute_with_retry(cursor, '''
            SELECT wave_number, wave_text FROM game_sessions
            WHERE id = ?
            ''', (session_id,))
            
            session_data = cursor.fetchone()
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
    
    except Exception as e:
        logging.error(f"Error getting team composition for session {session_id}: {str(e)}")
        return None
    finally:
        conn.close()

def get_team_name(team_id):
    team_names = {
        1: "Team 1",
        2: "Team 2",
        3: "Undead",
        4: "Humans",
        1002: "Spectators"
    }
    return team_names.get(team_id, f"Team {team_id}")

def report_recent_redeems():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, server_id, map_name, wave_number, wave_text
        FROM game_sessions
        WHERE is_active = 1
        ''')
        
        active_sessions = cursor.fetchall()
        
        if not active_sessions:
            conn.close()
            return
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching active sessions for redeem reporting: {str(e)}")
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
            WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND ptc.timestamp > ?
            ORDER BY ptc.timestamp DESC
            ''', (session_id, one_minute_ago))
            
            recent_redeems = cursor2.fetchall()
            conn2.close()
            
            if recent_redeems:
                for redeem in recent_redeems:
                    try:
                        player_name = redeem['player_name']
                        wave_number = redeem['wave_number'] or 0
                        wave_text = redeem['wave_text'] or "Unknown Wave"
                        
                        conn3 = get_db_connection()
                        cursor3 = conn3.cursor()
                        
                        execute_with_retry(cursor3, 'SELECT name FROM servers WHERE id = ?', (session['server_id'],))
                        server = cursor3.fetchone()
                        conn3.close()
                        
                        server_name = server['name'] if server else "Unknown Server"
                        
                        logging.info(f"REDEEM: {player_name} redeemed on {server_name} / {session['map_name']} at {wave_text} (Wave {wave_number})")
                    except Exception as e:
                        logging.error(f"Error processing redeem record: {str(e)}")
        except Exception as e:
            logging.error(f"Error processing session {session.get('id', 'Unknown')} for redeems: {str(e)}")
    
    conn.close()