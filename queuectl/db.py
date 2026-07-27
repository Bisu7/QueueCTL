import datetime
import os
import sqlite3
import uuid
from typing import Generator, Optional
from queuectl.models import Job

DEFAULT_DB_PATH = "queuectl.db"

def get_db_path() -> str:
    return os.environ.get("QUEUECTL_DB", DEFAULT_DB_PATH)

def get_connection(db_path: str = None) -> sqlite3.Connection:
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # Enable WAL mode and set a busy timeout of 5 seconds to handle concurrent writes safely.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            locked_by TEXT,
            locked_at TEXT,
            lease_expires_at TEXT
        );
    """)
    
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT;")
    except sqlite3.OperationalError:
        pass
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    conn.commit()

def claim_next_job(conn: sqlite3.Connection, worker_id: Optional[str] = None) -> Optional[Job]:
    if worker_id is None:
        worker_id = f"worker-{uuid.uuid4()}"
        
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.isoformat()
    
    from queuectl.config import get_config
    lease_duration = int(get_config(conn, "lease_duration", "300"))
    lease_expires = (now + datetime.timedelta(seconds=lease_duration)).isoformat()
    
    retry_base_delay = int(get_config(conn, "retry_base_delay", "30"))
    
    query = """
        UPDATE jobs
        SET state = 'processing',
            updated_at = ?,
            lease_expires_at = ?,
            attempts = attempts + 1,
            locked_by = ?,
            locked_at = ?
        WHERE id = (
            SELECT id FROM jobs
            WHERE state = 'pending'
               OR (
                   state = 'failed'
                   AND attempts <= max_retries
                   AND datetime(updated_at, '+' || (attempts * ?) || ' seconds') <= datetime(?)
               )
            ORDER BY created_at ASC
            LIMIT 1
        ) AND state IN ('pending', 'failed')
        RETURNING id, command, state, attempts, max_retries, created_at, updated_at, locked_by, locked_at, lease_expires_at;
    """
    
    cur = conn.execute(query, (now_str, lease_expires, worker_id, now_str, retry_base_delay, now_str))
    row = cur.fetchone()
    if row is None:
        return None
        
    conn.commit()
    return Job.from_row(row)
