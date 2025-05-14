import logging
from datetime import datetime
from database import get_db_connection, execute_with_retry

def get_or_create_server(server_code, server_name=None):
    conn = get_db_connection()
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
    finally:
        conn.close()