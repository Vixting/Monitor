import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry

def update_player_record(session_id, steam_id, player_name, team_id, score, is_bot, timestamp):
    if not session_id:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, initial_score, highest_score, current_score, team_id
        FROM player_records 
        WHERE session_id = ? AND steam_id = ?
        ''', (session_id, steam_id))
        
        player = cursor.fetchone()
        
        if player:
            player_id = player['id']
            highest_score = max(player['highest_score'], score)
            current_score = player['current_score']
            previous_team_id = player['team_id']
            
            if previous_team_id != team_id and previous_team_id is not None and team_id is not None:
                execute_with_retry(cursor, '''
                SELECT id FROM player_team_scores 
                WHERE player_id = ? AND team_id = ? AND session_id = ?
                ''', (player_id, previous_team_id, session_id))
                
                team_score_record = cursor.fetchone()
                
                if team_score_record:
                    execute_with_retry(cursor, '''
                    UPDATE player_team_scores 
                    SET final_score = ?, last_updated = ?
                    WHERE id = ?
                    ''', (current_score, timestamp, team_score_record['id']))
                else:
                    execute_with_retry(cursor, '''
                    INSERT INTO player_team_scores 
                    (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (player_id, session_id, previous_team_id, player['initial_score'], current_score, player['first_seen'], timestamp))
                
                conn.commit()
                conn.close()
                log_player_team_change(session_id, player_id, player_name, previous_team_id, team_id, timestamp, current_score)
                
                conn = get_db_connection()
                cursor = conn.cursor()
                
                execute_with_retry(cursor, '''
                INSERT OR IGNORE INTO player_team_scores 
                (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (player_id, session_id, team_id, score, score, timestamp, timestamp))
            
            if score != current_score:
                execute_with_retry(cursor, '''
                UPDATE player_records 
                SET current_score = ?, highest_score = ?, last_seen = ?, player_name = ?, team_id = ?
                WHERE id = ?
                ''', (score, highest_score, timestamp, player_name, team_id, player_id))
                
                if team_id is not None:
                    execute_with_retry(cursor, '''
                    SELECT id FROM player_team_scores 
                    WHERE player_id = ? AND team_id = ? AND session_id = ?
                    ''', (player_id, team_id, session_id))
                    
                    team_score_record = cursor.fetchone()
                    
                    if team_score_record:
                        execute_with_retry(cursor, '''
                        UPDATE player_team_scores 
                        SET final_score = ?, last_updated = ?
                        WHERE id = ?
                        ''', (score, timestamp, team_score_record['id']))
                    else:
                        execute_with_retry(cursor, '''
                        INSERT INTO player_team_scores 
                        (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (player_id, session_id, team_id, score, score, timestamp, timestamp))
                
                if abs(score - current_score) > 5:
                    execute_with_retry(cursor, '''
                    INSERT INTO score_history (player_id, timestamp, score)
                    VALUES (?, ?, ?)
                    ''', (player_id, timestamp, score))
                    
                    logging.debug(f"Updated score for player {player_name} in session {session_id}: {current_score} -> {score}")
            else:
                execute_with_retry(cursor, '''
                UPDATE player_records 
                SET last_seen = ?, team_id = ?
                WHERE id = ?
                ''', (timestamp, team_id, player_id))
        else:
            execute_with_retry(cursor, '''
            INSERT INTO player_records 
            (session_id, steam_id, player_name, team_id, current_score, initial_score, highest_score, first_seen, last_seen, is_bot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (session_id, steam_id, player_name, team_id, score, score, score, timestamp, timestamp, is_bot))
            
            player_id = cursor.lastrowid
            
            if team_id is not None:
                execute_with_retry(cursor, '''
                INSERT INTO player_team_scores 
                (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (player_id, session_id, team_id, score, score, timestamp, timestamp))
            
            execute_with_retry(cursor, '''
            INSERT INTO score_history (player_id, timestamp, score)
            VALUES (?, ?, ?)
            ''', (player_id, timestamp, score))
            
            logging.debug(f"Added new player {player_name} to session {session_id} with initial score {score}")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Database error updating player record for {steam_id} in session {session_id}: {str(e)}")
    finally:
        conn.close()

def log_player_team_change(session_id, player_id, player_name, old_team_id, new_team_id, timestamp, current_score=None):
    if not session_id:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT wave_text, wave_number FROM game_sessions WHERE id = ?
        ''', (session_id,))
        
        session_data = cursor.fetchone()
        wave_text = session_data['wave_text'] if session_data else "Unknown"
        wave_number = session_data['wave_number'] if session_data else None
        
        is_death = 0
        is_redeem = 0
        
        if old_team_id == 4 and (new_team_id == 3 or new_team_id == 1002):
            is_death = 1
            logging.info(f"Player {player_name} died at wave {wave_number} - {wave_text}")
        elif old_team_id == 3 and new_team_id == 4:
            is_redeem = 1
            logging.info(f"Player {player_name} redeemed at wave {wave_number} - {wave_text}")
        
        execute_with_retry(cursor, '''
        INSERT INTO player_team_changes
        (player_id, session_id, old_team_id, new_team_id, timestamp, wave_text, wave_number, is_death, is_redeem, score_at_change)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (player_id, session_id, old_team_id, new_team_id, timestamp, wave_text, wave_number, is_death, is_redeem, current_score))
        
        conn.commit()
        
        if is_death:
            logging.info(f"Recorded death for player {player_name}: team {old_team_id} -> team {new_team_id}")
        elif is_redeem:
            logging.info(f"Recorded redemption for player {player_name}: team {old_team_id} -> team {new_team_id}")
        else:
            logging.info(f"Recorded team change for player {player_name}: team {old_team_id} -> team {new_team_id}")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error logging player team change for player {player_name}: {str(e)}")
    finally:
        conn.close()

def update_player_playtimes(session_id, end_time):
    if not session_id:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        CREATE TABLE IF NOT EXISTS player_team_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            initial_score INTEGER NOT NULL DEFAULT 0,
            final_score INTEGER NOT NULL DEFAULT 0,
            first_seen TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            UNIQUE(player_id, session_id, team_id),
            FOREIGN KEY (player_id) REFERENCES player_records (id),
            FOREIGN KEY (session_id) REFERENCES game_sessions (id)
        )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        conn.rollback()
        conn.close()
        logging.error(f"Error creating player_team_scores table: {str(e)}")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT id, steam_id, player_name, team_id, current_score, first_seen, last_seen, is_bot
        FROM player_records
        WHERE session_id = ? AND is_bot = 0
        ''', (session_id,))
        
        players = cursor.fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching players for playtime update in session {session_id}: {str(e)}")
        return
    
    # Process players in batches
    batch_size = 10
    player_batches = [players[i:i + batch_size] for i in range(0, len(players), batch_size)]
    processed_count = 0
    
    for batch in player_batches:
        for player in batch:
            if player['is_bot']:
                continue
                
            steam_id = player['steam_id']
            player_name = player['player_name']
            current_team_id = player['team_id']
            current_score = player['current_score']
            
            first_seen = datetime.fromisoformat(player['first_seen'])
            last_seen = datetime.fromisoformat(player['last_seen'])
            
            playtime_seconds = int((last_seen - first_seen).total_seconds())
            
            # Update player stats
            conn = get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Ensure player_stats table exists
                execute_with_retry(cursor, '''
                CREATE TABLE IF NOT EXISTS player_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    steam_id TEXT UNIQUE NOT NULL,
                    player_name TEXT NOT NULL,
                    total_score INTEGER NOT NULL DEFAULT 0,
                    total_playtime_seconds INTEGER NOT NULL DEFAULT 0,
                    session_count INTEGER NOT NULL DEFAULT 0,
                    last_updated TEXT NOT NULL
                )
                ''')
                
                execute_with_retry(cursor, '''
                SELECT id, total_score, total_playtime_seconds, session_count
                FROM player_stats
                WHERE steam_id = ?
                ''', (steam_id,))
                
                player_stats = cursor.fetchone()
                
                execute_with_retry(cursor, '''
                SELECT team_id, final_score - initial_score AS score_earned
                FROM player_team_scores
                WHERE player_id = ? AND session_id = ?
                ''', (player['id'], session_id))
                
                team_scores = cursor.fetchall()
                total_session_score = 0
                
                if team_scores:
                    for ts in team_scores:
                        total_session_score += ts['score_earned']
                else:
                    total_session_score = current_score
                
                if current_team_id is not None:
                    execute_with_retry(cursor, '''
                    SELECT id FROM player_team_scores
                    WHERE player_id = ? AND session_id = ? AND team_id = ?
                    ''', (player['id'], session_id, current_team_id))
                    
                    team_score_record = cursor.fetchone()
                    
                    if team_score_record:
                        execute_with_retry(cursor, '''
                        UPDATE player_team_scores 
                        SET final_score = ?, last_updated = ?
                        WHERE id = ?
                        ''', (current_score, end_time, team_score_record['id']))
                    else:
                        execute_with_retry(cursor, '''
                        INSERT INTO player_team_scores 
                        (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (player['id'], session_id, current_team_id, 0, current_score, first_seen.isoformat(), end_time))
                
                if player_stats:
                    stats_id = player_stats['id']
                    total_score = player_stats['total_score'] + total_session_score
                    total_playtime = player_stats['total_playtime_seconds'] + playtime_seconds
                    session_count = player_stats['session_count'] + 1
                    
                    execute_with_retry(cursor, '''
                    UPDATE player_stats
                    SET player_name = ?, total_score = ?, total_playtime_seconds = ?, 
                        session_count = ?, last_updated = ?
                    WHERE id = ?
                    ''', (player_name, total_score, total_playtime, session_count, end_time, stats_id))
                else:
                    execute_with_retry(cursor, '''
                    INSERT INTO player_stats
                    (steam_id, player_name, total_score, total_playtime_seconds, session_count, last_updated)
                    VALUES (?, ?, ?, ?, 1, ?)
                    ''', (steam_id, player_name, total_session_score, playtime_seconds, end_time))
                    
                    stats_id = cursor.lastrowid
                
                conn.commit()
                conn.close()
                
                conn = get_db_connection()
                cursor = conn.cursor()
                
                try:
                    execute_with_retry(cursor, '''
                    CREATE TABLE IF NOT EXISTS player_team_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        player_stats_id INTEGER NOT NULL,
                        team_id INTEGER NOT NULL,
                        team_name TEXT NOT NULL,
                        score INTEGER NOT NULL DEFAULT 0,
                        playtime_seconds INTEGER NOT NULL DEFAULT 0,
                        last_updated TEXT NOT NULL,
                        UNIQUE(player_stats_id, team_id),
                        FOREIGN KEY (player_stats_id) REFERENCES player_stats (id)
                    )
                    ''')
                    
                    execute_with_retry(cursor, '''
                    SELECT team_id, final_score - initial_score AS score_earned
                    FROM player_team_scores
                    WHERE player_id = ? AND session_id = ?
                    ''', (player['id'], session_id))
                    
                    team_scores = cursor.fetchall()
                    
                    for ts in team_scores:
                        team_id = ts['team_id']
                        score_earned = ts['score_earned']
                        
                        execute_with_retry(cursor, '''
                        SELECT team_name FROM team_status
                        WHERE session_id = ? AND team_id = ?
                        ORDER BY timestamp DESC LIMIT 1
                        ''', (session_id, team_id))
                        
                        team_info = cursor.fetchone()
                        team_name = team_info['team_name'] if team_info else f'Team {team_id}'
                        
                        # Get existing team stats
                        execute_with_retry(cursor, '''
                        SELECT id, score, playtime_seconds
                        FROM player_team_stats
                        WHERE player_stats_id = ? AND team_id = ?
                        ''', (stats_id, team_id))
                        
                        team_stats = cursor.fetchone()
                        
                        team_playtime = int(playtime_seconds / len(team_scores))  # Simple distribution
                        
                        if team_stats:
                            team_score = team_stats['score'] + score_earned
                            team_playtime_total = team_stats['playtime_seconds'] + team_playtime
                            
                            execute_with_retry(cursor, '''
                            UPDATE player_team_stats
                            SET score = ?, playtime_seconds = ?, team_name = ?, last_updated = ?
                            WHERE id = ?
                            ''', (team_score, team_playtime_total, team_name, end_time, team_stats['id']))
                        else:
                            execute_with_retry(cursor, '''
                            INSERT INTO player_team_stats
                            (player_stats_id, team_id, team_name, score, playtime_seconds, last_updated)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''', (stats_id, team_id, team_name, score_earned, team_playtime, end_time))
                    
                    conn.commit()
                    processed_count += 1
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error updating team stats for player {player_name}: {str(e)}")
                finally:
                    conn.close()
                
            except Exception as e:
                if conn:
                    conn.rollback()
                    conn.close()
                logging.error(f"Error updating playtime for player {player['player_name']}: {str(e)}")
                continue
    
    logging.info(f"Updated playtimes and stats for {processed_count} players in session {session_id}")

def update_death_statistics(session_id):
    if not session_id:
        return
    
    # First, fetch all death events in this session
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT pr.steam_id, pr.player_name, ptc.wave_number
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_death = 1 AND pr.is_bot = 0
        ''', (session_id,))
        
        deaths = cursor.fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching death events for session {session_id}: {str(e)}")
        return
    
    # Process deaths in batches
    batch_size = 10
    death_batches = [deaths[i:i + batch_size] for i in range(0, len(deaths), batch_size)]
    processed_count = 0
    
    for batch in death_batches:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Ensure tables exist
            execute_with_retry(cursor, '''
            CREATE TABLE IF NOT EXISTS player_death_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                steam_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                total_deaths INTEGER NOT NULL DEFAULT 0,
                waves_survived INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL,
                UNIQUE(steam_id)
            )
            ''')
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            logging.error(f"Error creating death stats table: {str(e)}")
            return
            
        for death in batch:
            try:
                steam_id = death['steam_id']
                player_name = death['player_name']
                wave_number = death['wave_number'] or 0
                now = datetime.now().isoformat()
                
                execute_with_retry(cursor, '''
                SELECT id, total_deaths, waves_survived FROM player_death_stats
                WHERE steam_id = ?
                ''', (steam_id,))
                
                stats = cursor.fetchone()
                
                if stats:
                    total_deaths = stats['total_deaths'] + 1
                    waves_survived = stats['waves_survived'] + wave_number
                    
                    execute_with_retry(cursor, '''
                    UPDATE player_death_stats
                    SET player_name = ?, total_deaths = ?, waves_survived = ?, last_updated = ?
                    WHERE id = ?
                    ''', (player_name, total_deaths, waves_survived, now, stats['id']))
                else:
                    execute_with_retry(cursor, '''
                    INSERT INTO player_death_stats
                    (steam_id, player_name, total_deaths, waves_survived, last_updated)
                    VALUES (?, ?, 1, ?, ?)
                    ''', (steam_id, player_name, wave_number, now))
                
                processed_count += 1
            except Exception as e:
                logging.error(f"Error updating death statistics for player {death.get('player_name', 'Unknown')}: {str(e)}")
        
        conn.commit()
        conn.close()
    
    # Now process redeem events
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        execute_with_retry(cursor, '''
        SELECT pr.steam_id, pr.player_name
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND pr.is_bot = 0
        ''', (session_id,))
        
        redeems = cursor.fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        logging.error(f"Error fetching redeem events for session {session_id}: {str(e)}")
        return
    
    # Process redeems in batches
    redeem_batches = [redeems[i:i + batch_size] for i in range(0, len(redeems), batch_size)]
    redeem_count = 0
    
    for batch in redeem_batches:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Ensure tables exist
            execute_with_retry(cursor, '''
            CREATE TABLE IF NOT EXISTS player_redeem_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                steam_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                total_redeems INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL,
                UNIQUE(steam_id)
            )
            ''')
            conn.commit()
        except Exception as e:
            conn.rollback()
            conn.close()
            logging.error(f"Error creating redeem stats table: {str(e)}")
            return
            
        for redeem in batch:
            try:
                steam_id = redeem['steam_id']
                player_name = redeem['player_name']
                now = datetime.now().isoformat()
                
                execute_with_retry(cursor, '''
                SELECT id, total_redeems FROM player_redeem_stats
                WHERE steam_id = ?
                ''', (steam_id,))
                
                stats = cursor.fetchone()
                
                if stats:
                    total_redeems = stats['total_redeems'] + 1
                    
                    execute_with_retry(cursor, '''
                    UPDATE player_redeem_stats
                    SET player_name = ?, total_redeems = ?, last_updated = ?
                    WHERE id = ?
                    ''', (player_name, total_redeems, now, stats['id']))
                else:
                    execute_with_retry(cursor, '''
                    INSERT INTO player_redeem_stats
                    (steam_id, player_name, total_redeems, last_updated)
                    VALUES (?, ?, 1, ?)
                    ''', (steam_id, player_name, now))
                
                redeem_count += 1
            except Exception as e:
                logging.error(f"Error updating redeem statistics for player {redeem.get('player_name', 'Unknown')}: {str(e)}")
        
        conn.commit()
        conn.close()
    
    if processed_count > 0:
        logging.info(f"Updated death statistics for {processed_count} deaths in session {session_id}")
    
    if redeem_count > 0:
        logging.info(f"Updated redeem statistics for {redeem_count} redeems in session {session_id}")