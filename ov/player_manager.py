import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry, query_one, query_all, execute_query
from typing import Optional, Dict, Any, List, Union

def update_player_record(
    session_id: Optional[int], 
    steam_id: str, 
    player_name: str, 
    team_id: Optional[int], 
    score: int, 
    is_bot: bool, 
    timestamp: str
) -> None:
    if not session_id:
        return
    
    player = query_one(
        '''
        SELECT * FROM player_records 
        WHERE session_id = ? AND steam_id = ?
        ''', 
        (session_id, steam_id)
    )
    
    if player:
        player_id = player['id']
        highest_score = max(player['highest_score'], score)
        current_score = player['current_score']
        previous_team_id = player['team_id']
        
        initial_score = player['initial_score'] if 'initial_score' in player.keys() else score
        first_seen = player['first_seen'] if 'first_seen' in player.keys() else timestamp
        
        if previous_team_id != team_id and previous_team_id is not None and team_id is not None:
            handle_team_change(session_id, player_id, player_name, previous_team_id, team_id, timestamp, current_score, initial_score, first_seen)
        
        # Handle score changes
        if score != current_score:
            execute_query(
                '''
                UPDATE player_records 
                SET current_score = ?, highest_score = ?, last_seen = ?, player_name = ?, team_id = ?
                WHERE id = ?
                ''', 
                (score, highest_score, timestamp, player_name, team_id, player_id)
            )
            
            if team_id is not None:
                update_player_team_score(player_id, session_id, team_id, score, timestamp)
            
            if abs(score - current_score) > 5:
                execute_query(
                    '''
                    INSERT INTO score_history (player_id, timestamp, score)
                    VALUES (?, ?, ?)
                    ''', 
                    (player_id, timestamp, score)
                )
                
                logging.debug(f"Updated score for player {player_name} in session {session_id}: {current_score} -> {score}")
        else:
            execute_query(
                '''
                UPDATE player_records 
                SET last_seen = ?, team_id = ?
                WHERE id = ?
                ''', 
                (timestamp, team_id, player_id)
            )
    else:
        player_id = execute_query(
            '''
            INSERT INTO player_records 
            (session_id, steam_id, player_name, team_id, current_score, initial_score, highest_score, first_seen, last_seen, is_bot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', 
            (session_id, steam_id, player_name, team_id, score, score, score, timestamp, timestamp, is_bot)
        )
        
        if player_id and team_id is not None:
            create_player_team_score(player_id, session_id, team_id, score, timestamp)
        
        if player_id:
            execute_query(
                '''
                INSERT INTO score_history (player_id, timestamp, score)
                VALUES (?, ?, ?)
                ''', 
                (player_id, timestamp, score)
            )
            
            logging.debug(f"Added new player {player_name} to session {session_id} with initial score {score}")

def handle_team_change(
    session_id: int, 
    player_id: int, 
    player_name: str, 
    old_team_id: int, 
    new_team_id: int, 
    timestamp: str, 
    current_score: int, 
    initial_score: int, 
    first_seen: str
) -> None:
    logging.info(f"Team change detected for {player_name}: {old_team_id} -> {new_team_id}")
    
    old_team_score = query_one(
        '''
        SELECT id FROM player_team_scores 
        WHERE player_id = ? AND team_id = ? AND session_id = ?
        ''', 
        (player_id, old_team_id, session_id)
    )
    
    if old_team_score and 'id' in old_team_score.keys():
        execute_query(
            '''
            UPDATE player_team_scores 
            SET final_score = ?, last_updated = ?
            WHERE id = ?
            ''', 
            (current_score, timestamp, old_team_score['id'])
        )
    else:
        execute_query(
            '''
            INSERT OR IGNORE INTO player_team_scores 
            (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', 
            (player_id, session_id, old_team_id, initial_score, current_score, first_seen, timestamp)
        )
    
    log_player_team_change(session_id, player_id, player_name, old_team_id, new_team_id, timestamp, current_score)
    
    new_team_record = query_one(
        '''
        SELECT id FROM player_team_scores 
        WHERE player_id = ? AND team_id = ? AND session_id = ?
        ''', 
        (player_id, new_team_id, session_id)
    )
    
    if not new_team_record:
        execute_query(
            '''
            INSERT OR IGNORE INTO player_team_scores 
            (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', 
            (player_id, session_id, new_team_id, current_score, current_score, timestamp, timestamp)
        )

def update_player_team_score(player_id: int, session_id: int, team_id: int, score: int, timestamp: str) -> None:
    team_score_record = query_one(
        '''
        SELECT id FROM player_team_scores 
        WHERE player_id = ? AND team_id = ? AND session_id = ?
        ''', 
        (player_id, team_id, session_id)
    )
    
    if team_score_record and 'id' in team_score_record.keys():
        execute_query(
            '''
            UPDATE player_team_scores 
            SET final_score = ?, last_updated = ?
            WHERE id = ?
            ''', 
            (score, timestamp, team_score_record['id'])
        )
    else:
        execute_query(
            '''
            INSERT OR IGNORE INTO player_team_scores 
            (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', 
            (player_id, session_id, team_id, score, score, timestamp, timestamp)
        )

def create_player_team_score(player_id: int, session_id: int, team_id: int, score: int, timestamp: str) -> None:
    execute_query(
        '''
        INSERT OR IGNORE INTO player_team_scores 
        (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', 
        (player_id, session_id, team_id, score, score, timestamp, timestamp)
    )

def log_player_team_change(
    session_id: int, 
    player_id: int, 
    player_name: str, 
    old_team_id: int, 
    new_team_id: int, 
    timestamp: str, 
    current_score: Optional[int] = None
) -> None:
    if not session_id:
        return
    
    session_data = query_one(
        '''
        SELECT wave_text, wave_number FROM game_sessions WHERE id = ?
        ''', 
        (session_id,)
    )
    
    wave_text = session_data['wave_text'] if session_data and 'wave_text' in session_data.keys() else "Unknown"
    wave_number = session_data['wave_number'] if session_data and 'wave_number' in session_data.keys() else None
    
    is_death = 1 if old_team_id == 4 and (new_team_id == 3 or new_team_id == 1002) else 0
    is_redeem = 1 if old_team_id == 3 and new_team_id == 4 else 0
    
    if is_death:
        logging.info(f"Player {player_name} died at wave {wave_number} - {wave_text}")
    elif is_redeem:
        logging.info(f"Player {player_name} redeemed at wave {wave_number} - {wave_text}")
    
    execute_query(
        '''
        INSERT INTO player_team_changes
        (player_id, session_id, old_team_id, new_team_id, timestamp, wave_text, wave_number, is_death, is_redeem, score_at_change)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', 
        (player_id, session_id, old_team_id, new_team_id, timestamp, wave_text, wave_number, is_death, is_redeem, current_score)
    )

def update_player_playtimes(session_id: Optional[int], end_time: str) -> None:
    if not session_id:
        return
    
    execute_query(
        '''
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
        '''
    )
    
    # Get all players in the session
    players = query_all(
        '''
        SELECT id, steam_id, player_name, team_id, current_score, first_seen, last_seen, is_bot
        FROM player_records
        WHERE session_id = ? AND is_bot = 0
        ''', 
        (session_id,)
    )
    
    processed_count = 0
    
    # Process each non-bot player
    for player in players:
        if player['is_bot']:
            continue
        
        player_id = player['id']
        steam_id = player['steam_id']
        player_name = player['player_name']
        current_team_id = player['team_id']
        current_score = player['current_score']
        
        first_seen = player['first_seen'] if 'first_seen' in player.keys() else end_time
        last_seen = player['last_seen'] if 'last_seen' in player.keys() else end_time
        
        # Calculate playtime
        playtime_seconds = int((datetime.fromisoformat(last_seen) - datetime.fromisoformat(first_seen)).total_seconds())
        
        # Get team scores for this player
        team_scores = query_all(
            '''
            SELECT team_id, final_score - initial_score AS score_earned
            FROM player_team_scores
            WHERE player_id = ? AND session_id = ?
            ''', 
            (player_id, session_id)
        )
        
        # Calculate total session score
        total_session_score = 0
        if team_scores:
            for ts in team_scores:
                if 'score_earned' in ts.keys() and ts['score_earned'] is not None:
                    total_session_score += ts['score_earned']
        else:
            total_session_score = current_score
        
        # Update final score for current team if needed
        if current_team_id is not None:
            team_score_record = query_one(
                '''
                SELECT id FROM player_team_scores
                WHERE player_id = ? AND session_id = ? AND team_id = ?
                ''', 
                (player_id, session_id, current_team_id)
            )
            
            if team_score_record and 'id' in team_score_record.keys():
                execute_query(
                    '''
                    UPDATE player_team_scores 
                    SET final_score = ?, last_updated = ?
                    WHERE id = ?
                    ''', 
                    (current_score, end_time, team_score_record['id'])
                )
            else:
                execute_query(
                    '''
                    INSERT OR IGNORE INTO player_team_scores 
                    (player_id, session_id, team_id, initial_score, final_score, first_seen, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', 
                    (player_id, session_id, current_team_id, 0, current_score, first_seen, end_time)
                )
        
        # Update or create player stats record
        player_stats = query_one(
            '''
            SELECT id, total_score, total_playtime_seconds, session_count
            FROM player_stats
            WHERE steam_id = ?
            ''', 
            (steam_id,)
        )
        
        if player_stats and 'id' in player_stats.keys():
            stats_id = player_stats['id']
            total_score = player_stats['total_score'] + total_session_score
            total_playtime = player_stats['total_playtime_seconds'] + playtime_seconds
            session_count = player_stats['session_count'] + 1
            
            execute_query(
                '''
                UPDATE player_stats
                SET player_name = ?, total_score = ?, total_playtime_seconds = ?, 
                    session_count = ?, last_updated = ?
                WHERE id = ?
                ''', 
                (player_name, total_score, total_playtime, session_count, end_time, stats_id)
            )
        else:
            stats_id = execute_query(
                '''
                INSERT INTO player_stats
                (steam_id, player_name, total_score, total_playtime_seconds, session_count, last_updated)
                VALUES (?, ?, ?, ?, 1, ?)
                ''', 
                (steam_id, player_name, total_session_score, playtime_seconds, end_time)
            )
        
        # Update team-specific stats if we have a valid stats_id
        if stats_id:
            update_team_specific_stats(stats_id, player_id, session_id, team_scores, current_team_id, playtime_seconds, end_time)
            processed_count += 1
    
    logging.info(f"Updated playtimes and stats for {processed_count} players in session {session_id}")

def update_team_specific_stats(
    stats_id: int, 
    player_id: int, 
    session_id: int, 
    team_scores: List[Dict[str, Any]], 
    current_team_id: Optional[int], 
    playtime_seconds: int, 
    end_time: str
) -> None:
    # If we don't have team scores but player is on a team, create a dummy score record
    if not team_scores and current_team_id is not None:
        current_score_record = query_one("SELECT current_score FROM player_records WHERE id = ?", (player_id,))
        current_score = current_score_record['current_score'] if current_score_record else 0
        team_scores = [{'team_id': current_team_id, 'score_earned': current_score}]
    
    for ts in team_scores:
        if 'team_id' not in ts.keys():
            continue  
            
        team_id = ts['team_id']
        score_earned = ts['score_earned'] if 'score_earned' in ts.keys() and ts['score_earned'] is not None else 0
        
        # Get team name
        team_info = query_one(
            '''
            SELECT team_name FROM team_status
            WHERE session_id = ? AND team_id = ?
            ORDER BY timestamp DESC LIMIT 1
            ''', 
            (session_id, team_id)
        )
        
        team_name = team_info['team_name'] if team_info and 'team_name' in team_info.keys() else f'Team {team_id}'
        
        # Get existing team stats
        team_stats = query_one(
            '''
            SELECT id, score, playtime_seconds
            FROM player_team_stats
            WHERE player_stats_id = ? AND team_id = ?
            ''', 
            (stats_id, team_id)
        )
        
        # Calculate playtime distribution (divide by number of teams player was on)
        team_playtime = int(playtime_seconds / max(1, len(team_scores)))
        
        if team_stats and 'id' in team_stats.keys():
            team_score = team_stats['score'] + score_earned
            team_playtime_total = team_stats['playtime_seconds'] + team_playtime
            
            execute_query(
                '''
                UPDATE player_team_stats
                SET score = ?, playtime_seconds = ?, team_name = ?, last_updated = ?
                WHERE id = ?
                ''', 
                (team_score, team_playtime_total, team_name, end_time, team_stats['id'])
            )
        else:
            execute_query(
                '''
                INSERT OR IGNORE INTO player_team_stats
                (player_stats_id, team_id, team_name, score, playtime_seconds, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', 
                (stats_id, team_id, team_name, score_earned, team_playtime, end_time)
            )

def update_death_statistics(session_id: Optional[int]) -> None:
    if not session_id:
        return
    
    # Get all death events in this session
    deaths = query_all(
        '''
        SELECT pr.steam_id, pr.player_name, ptc.wave_number
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_death = 1 AND pr.is_bot = 0
        ''', 
        (session_id,)
    )
    
    processed_count = 0
    
    # Ensure death stats table exists
    execute_query(
        '''
        CREATE TABLE IF NOT EXISTS player_death_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            steam_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            total_deaths INTEGER NOT NULL DEFAULT 0,
            waves_survived INTEGER NOT NULL DEFAULT 0,
            last_updated TEXT NOT NULL,
            UNIQUE(steam_id)
        )
        '''
    )
    
    # Process each death event
    for death in deaths:
        steam_id = death['steam_id']
        player_name = death['player_name']
        wave_number = death['wave_number'] or 0
        now = datetime.now().isoformat()
        
        # Get existing death stats
        stats = query_one(
            '''
            SELECT id, total_deaths, waves_survived FROM player_death_stats
            WHERE steam_id = ?
            ''', 
            (steam_id,)
        )
        
        if stats and 'id' in stats.keys():
            total_deaths = stats['total_deaths'] + 1
            waves_survived = stats['waves_survived'] + wave_number
            
            execute_query(
                '''
                UPDATE player_death_stats
                SET player_name = ?, total_deaths = ?, waves_survived = ?, last_updated = ?
                WHERE id = ?
                ''', 
                (player_name, total_deaths, waves_survived, now, stats['id'])
            )
        else:
            execute_query(
                '''
                INSERT INTO player_death_stats
                (steam_id, player_name, total_deaths, waves_survived, last_updated)
                VALUES (?, ?, 1, ?, ?)
                ''', 
                (steam_id, player_name, wave_number, now)
            )
        
        processed_count += 1
    
    # Process redeem events
    redeems = query_all(
        '''
        SELECT pr.steam_id, pr.player_name
        FROM player_team_changes ptc
        JOIN player_records pr ON ptc.player_id = pr.id
        WHERE ptc.session_id = ? AND ptc.is_redeem = 1 AND pr.is_bot = 0
        ''', 
        (session_id,)
    )
    
    redeem_count = 0
    
    # Ensure redeem stats table exists
    execute_query(
        '''
        CREATE TABLE IF NOT EXISTS player_redeem_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            steam_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            total_redeems INTEGER NOT NULL DEFAULT 0,
            last_updated TEXT NOT NULL,
            UNIQUE(steam_id)
        )
        '''
    )
    
    # Process each redeem event
    for redeem in redeems:
        steam_id = redeem['steam_id']
        player_name = redeem['player_name']
        now = datetime.now().isoformat()
        
        # Get existing redeem stats
        stats = query_one(
            '''
            SELECT id, total_redeems FROM player_redeem_stats
            WHERE steam_id = ?
            ''', 
            (steam_id,)
        )
        
        if stats and 'id' in stats.keys():
            total_redeems = stats['total_redeems'] + 1
            
            execute_query(
                '''
                UPDATE player_redeem_stats
                SET player_name = ?, total_redeems = ?, last_updated = ?
                WHERE id = ?
                ''', 
                (player_name, total_redeems, now, stats['id'])
            )
        else:
            execute_query(
                '''
                INSERT INTO player_redeem_stats
                (steam_id, player_name, total_redeems, last_updated)
                VALUES (?, ?, 1, ?)
                ''', 
                (steam_id, player_name, now)
            )
        
        redeem_count += 1
    
    if processed_count > 0:
        logging.info(f"Updated death statistics for {processed_count} deaths in session {session_id}")
    
    if redeem_count > 0:
        logging.info(f"Updated redeem statistics for {redeem_count} redeems in session {session_id}")

def get_player_stats(steam_id: str) -> Optional[Dict[str, Any]]:
    # Get basic player stats
    player = query_one(
        '''
        SELECT * FROM player_stats
        WHERE steam_id = ?
        ''', 
        (steam_id,)
    )
    
    if not player:
        return None
    
    # Get death stats
    death_stats = query_one(
        '''
        SELECT total_deaths, waves_survived FROM player_death_stats
        WHERE steam_id = ?
        ''', 
        (steam_id,)
    )
    
    # Get redeem stats
    redeem_stats = query_one(
        '''
        SELECT total_redeems FROM player_redeem_stats
        WHERE steam_id = ?
        ''', 
        (steam_id,)
    )
    
    # Get team stats
    team_stats = query_all(
        '''
        SELECT team_id, team_name, score, playtime_seconds
        FROM player_team_stats
        WHERE player_stats_id = ?
        ORDER BY score DESC
        ''', 
        (player['id'],)
    )
    
    # Build complete stats object
    stats = {
        'player_name': player['player_name'],
        'steam_id': steam_id,
        'total_score': player['total_score'],
        'total_playtime_seconds': player['total_playtime_seconds'],
        'session_count': player['session_count'],
        'last_updated': player['last_updated'],
        'deaths': death_stats['total_deaths'] if death_stats and 'total_deaths' in death_stats.keys() else 0,
        'waves_survived': death_stats['waves_survived'] if death_stats and 'waves_survived' in death_stats.keys() else 0,
        'redeems': redeem_stats['total_redeems'] if redeem_stats and 'total_redeems' in redeem_stats.keys() else 0,
        'team_stats': team_stats
    }
    
    return stats

def get_top_players(limit: int = 10, sort_by: str = 'score') -> List[Dict[str, Any]]:
    sort_column = "total_score"
    if sort_by == 'playtime':
        sort_column = "total_playtime_seconds"
    elif sort_by == 'sessions':
        sort_column = "session_count"
    
    players = query_all(
        f'''
        SELECT ps.*, 
               COALESCE(pds.total_deaths, 0) as deaths,
               COALESCE(prs.total_redeems, 0) as redeems
        FROM player_stats ps
        LEFT JOIN player_death_stats pds ON ps.steam_id = pds.steam_id
        LEFT JOIN player_redeem_stats prs ON ps.steam_id = prs.steam_id
        ORDER BY {sort_column} DESC
        LIMIT ?
        ''', 
        (limit,)
    )
    
    return players