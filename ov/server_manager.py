import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry, query_one, execute_query
from typing import Optional, Dict, Any

def get_or_create_server(server_code: str, server_name: Optional[str] = None) -> Optional[int]:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        
        try:
            execute_with_retry(cursor, 'SELECT id FROM servers WHERE server_code = ?', (server_code,))
            server = cursor.fetchone()
            
            if server:
                server_id = server['id']
                execute_with_retry(cursor, 'UPDATE servers SET last_seen = ?, name = COALESCE(?, name) WHERE id = ?',
                            (now, server_name, server_id))
            else:
                execute_with_retry(cursor, 'INSERT INTO servers (server_code, name, last_seen) VALUES (?, ?, ?)',
                            (server_code, server_name or server_code, now))
                server_id = cursor.lastrowid
                logging.info(f"Created new server record for {server_code}")
                
            conn.commit()
            return server_id
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Database error managing server {server_code}: {str(e)}")
            return None

def get_server_name(server_id: int) -> str:
    server = query_one('SELECT name FROM servers WHERE id = ?', (server_id,))
    return server['name'] if server else "Unknown Server"

def update_server_status(server_id: int, is_active: bool) -> None:
    execute_query(
        'UPDATE servers SET is_active = ?, last_seen = ? WHERE id = ?',
        (is_active, datetime.now().isoformat(), server_id)
    )