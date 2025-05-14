import logging
import sqlite3
from datetime import datetime
import time

DB_FILE = "gameservers.db"

def get_db_connection():
    conn = None
    max_attempts = 5
    attempt = 0
    backoff_time = 1.0
    
    while attempt < max_attempts:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=60.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA cache_size = 10000")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 60000")
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                attempt += 1
                logging.warning(f"Database locked, retrying... (attempt {attempt}/{max_attempts})")
                time.sleep(backoff_time)
                backoff_time *= 1.5
            else:
                raise
    
    if conn is None:
        logging.error("Could not connect to database after multiple attempts")
        raise sqlite3.OperationalError("Database is locked and could not be accessed after multiple attempts")
    return conn

def execute_with_retry(cursor, query, params=(), max_attempts=5):
    attempt = 0
    backoff_time = 1.0
    
    while attempt < max_attempts:
        try:
            result = cursor.execute(query, params)
            return result
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                attempt += 1
                logging.warning(f"Database locked during query execution, retrying... (attempt {attempt}/{max_attempts})")
                time.sleep(backoff_time)
                backoff_time *= 1.5
            else:
                raise
    
    logging.error(f"Failed to execute query after {max_attempts} attempts due to database locks")
    raise sqlite3.OperationalError(f"Database is locked and query could not be executed after {max_attempts} attempts")

def init_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS servers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        server_code TEXT UNIQUE NOT NULL,
        name TEXT,
        last_seen TEXT,
        is_active BOOLEAN DEFAULT 1
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS game_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        server_id INTEGER NOT NULL,
        map_name TEXT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT,
        max_players INTEGER NOT NULL,
        peak_player_count INTEGER NOT NULL DEFAULT 0,
        wave_text TEXT,
        wave_number INTEGER,
        is_active BOOLEAN NOT NULL DEFAULT 1,
        team1_player_count INTEGER DEFAULT 0,
        team2_player_count INTEGER DEFAULT 0,
        session_result TEXT,
        FOREIGN KEY (server_id) REFERENCES servers (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS server_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        player_count INTEGER NOT NULL,
        wave_text TEXT,
        wave_number INTEGER,
        FOREIGN KEY (session_id) REFERENCES game_sessions (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        steam_id TEXT NOT NULL,
        player_name TEXT NOT NULL,
        team_id INTEGER,
        current_score INTEGER NOT NULL DEFAULT 0,
        initial_score INTEGER NOT NULL DEFAULT 0,
        highest_score INTEGER NOT NULL DEFAULT 0,
        first_seen TEXT NOT NULL,
        last_seen TEXT NOT NULL,
        is_bot BOOLEAN NOT NULL,
        UNIQUE(session_id, steam_id),
        FOREIGN KEY (session_id) REFERENCES game_sessions (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS score_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        score INTEGER NOT NULL,
        FOREIGN KEY (player_id) REFERENCES player_records (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS team_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        team_id INTEGER NOT NULL,
        team_name TEXT NOT NULL,
        player_count INTEGER NOT NULL,
        total_score INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (session_id) REFERENCES game_sessions (id)
    )
    ''')
    
    cursor.execute('''
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
    
    cursor.execute('''
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
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_team_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        session_id INTEGER NOT NULL,
        old_team_id INTEGER NOT NULL,
        new_team_id INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        wave_text TEXT,
        wave_number INTEGER,
        is_death BOOLEAN NOT NULL DEFAULT 0,
        is_redeem BOOLEAN NOT NULL DEFAULT 0,
        FOREIGN KEY (player_id) REFERENCES player_records (id),
        FOREIGN KEY (session_id) REFERENCES game_sessions (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_death_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        steam_id TEXT UNIQUE NOT NULL,
        player_name TEXT NOT NULL,
        total_deaths INTEGER NOT NULL DEFAULT 0,
        waves_survived INTEGER NOT NULL DEFAULT 0,
        last_updated TEXT NOT NULL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_redeem_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        steam_id TEXT UNIQUE NOT NULL,
        player_name TEXT NOT NULL,
        total_redeems INTEGER NOT NULL DEFAULT 0,
        last_updated TEXT NOT NULL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wave_end_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        wave_number INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        reason TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES game_sessions (id)
    )
    ''')
    
    cursor.execute('''
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
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_servers_code ON servers(server_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_server ON game_sessions(server_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_active ON game_sessions(is_active)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_map ON game_sessions(map_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_server_status_session ON server_status(session_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_records_session ON player_records(session_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_records_steam ON player_records(steam_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_score_history_player ON score_history(player_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_score_history_time ON score_history(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_status_session ON team_status(session_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_stats_steam ON player_stats(steam_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_stats_player ON player_team_stats(player_stats_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_player ON player_team_changes(player_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_session ON player_team_changes(session_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_death ON player_team_changes(is_death)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_redeem ON player_team_changes(is_redeem)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_death_stats_steam ON player_death_stats(steam_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_redeem_stats_steam ON player_redeem_stats(steam_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wave_end_records_session ON wave_end_records(session_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wave_end_records_wave ON wave_end_records(wave_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_wave_end ON player_wave_scores(wave_end_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_player ON player_wave_scores(player_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_steam ON player_wave_scores(steam_id)')
    
    conn.commit()
    conn.close()
    logging.info("Database initialized")

def migrate_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_team_scores'")
    if not cursor.fetchone():
        cursor.execute('''
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_scores_player ON player_team_scores(player_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_scores_session ON player_team_scores(session_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_scores_team ON player_team_scores(team_id)')
        logging.info("Created player_team_scores table")
    
    cursor.execute("PRAGMA table_info(player_team_changes)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    if 'score_at_change' not in column_names:
        cursor.execute('ALTER TABLE player_team_changes ADD COLUMN score_at_change INTEGER')
        logging.info("Added score_at_change column to player_team_changes table")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_team_changes'")
    if not cursor.fetchone():
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_team_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            old_team_id INTEGER NOT NULL,
            new_team_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            wave_text TEXT,
            wave_number INTEGER,
            is_death BOOLEAN NOT NULL DEFAULT 0,
            is_redeem BOOLEAN NOT NULL DEFAULT 0,
            score_at_change INTEGER,
            FOREIGN KEY (player_id) REFERENCES player_records (id),
            FOREIGN KEY (session_id) REFERENCES game_sessions (id)
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_player ON player_team_changes(player_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_session ON player_team_changes(session_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_death ON player_team_changes(is_death)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_redeem ON player_team_changes(is_redeem)')
        logging.info("Created player_team_changes table")
    else:
        cursor.execute("PRAGMA table_info(player_team_changes)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'is_redeem' not in column_names:
            cursor.execute('ALTER TABLE player_team_changes ADD COLUMN is_redeem BOOLEAN NOT NULL DEFAULT 0')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_team_changes_redeem ON player_team_changes(is_redeem)')
            logging.info("Added is_redeem column to player_team_changes table")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_death_stats'")
    if not cursor.fetchone():
        cursor.execute('''
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_death_stats_steam ON player_death_stats(steam_id)')
        logging.info("Created player_death_stats table")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_redeem_stats'")
    if not cursor.fetchone():
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_redeem_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            steam_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            total_redeems INTEGER NOT NULL DEFAULT 0,
            last_updated TEXT NOT NULL,
            UNIQUE(steam_id)
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_redeem_stats_steam ON player_redeem_stats(steam_id)')
        logging.info("Created player_redeem_stats table")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wave_end_records'")
    if not cursor.fetchone():
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wave_end_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            wave_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            reason TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES game_sessions (id)
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wave_end_records_session ON wave_end_records(session_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wave_end_records_wave ON wave_end_records(wave_number)')
        logging.info("Created wave_end_records table")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_wave_scores'")
    if not cursor.fetchone():
        cursor.execute('''
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_wave_end ON player_wave_scores(wave_end_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_player ON player_wave_scores(player_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_wave_scores_steam ON player_wave_scores(steam_id)')
        logging.info("Created player_wave_scores table")
    
    cursor.execute("PRAGMA table_info(game_sessions)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    if 'team1_player_count' not in column_names:
        cursor.execute('ALTER TABLE game_sessions ADD COLUMN team1_player_count INTEGER DEFAULT 0')
        logging.info("Added team1_player_count column to game_sessions table")
    
    if 'team2_player_count' not in column_names:
        cursor.execute('ALTER TABLE game_sessions ADD COLUMN team2_player_count INTEGER DEFAULT 0')
        logging.info("Added team2_player_count column to game_sessions table")
    
    if 'session_result' not in column_names:
        cursor.execute('ALTER TABLE game_sessions ADD COLUMN session_result TEXT')
        logging.info("Added session_result column to game_sessions table")
    
    execute_with_retry(cursor, '''
    UPDATE game_sessions 
    SET end_time = datetime('now')
    WHERE is_active = 0 AND (end_time IS NULL OR end_time = '[NULL]')
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database migration completed")