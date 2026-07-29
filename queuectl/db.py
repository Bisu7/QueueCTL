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
            lease_expires_at TEXT,
            next_attempt_at TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            run_at TEXT,
            timeout_seconds INTEGER,
            output TEXT
        );
    """)
    
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT;")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN next_attempt_at TEXT;")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN run_at TEXT;")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN timeout_seconds INTEGER;")
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN output TEXT;")
    except sqlite3.OperationalError:
        pass
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workers (
            id TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            stop_requested INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()

def claim_next_job(conn: sqlite3.Connection, worker_id: Optional[str] = None) -> Optional[Job]:
    if worker_id is None:
        worker_id = f"worker-{uuid.uuid4()}"
        
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.isoformat()
    
    from queuectl.config import get_config
    lease_duration = int(get_config(conn, "lease_duration", "15"))
    lease_expires = (now + datetime.timedelta(seconds=lease_duration)).isoformat()
    
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
            WHERE (state = 'pending' AND (run_at IS NULL OR datetime(run_at) <= datetime(?)))
               OR (
                   state = 'failed'
                   AND (next_attempt_at IS NULL OR datetime(next_attempt_at) <= datetime(?))
               )
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
        ) AND state IN ('pending', 'failed')
        RETURNING id, command, state, attempts, max_retries, created_at, updated_at, locked_by, locked_at, lease_expires_at, next_attempt_at, priority, run_at, timeout_seconds, output;
    """
    
    cur = conn.execute(query, (now_str, lease_expires, worker_id, now_str, now_str, now_str))
    row = cur.fetchone()
    if row is None:
        conn.rollback()
        return None
        
    conn.commit()
    return Job.from_row(row)

def reap_expired_jobs(conn: sqlite3.Connection) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    now_str = now.isoformat()
    
    rows = conn.execute(
        "SELECT id, attempts, max_retries FROM jobs WHERE state = 'processing' AND datetime(lease_expires_at) <= datetime(?);",
        (now_str,)
    ).fetchall()
    
    if not rows:
        return
        
    from queuectl.config import get_config
    base = int(get_config(conn, "backoff-base", "2"))
    
    for row in rows:
        job_id = row["id"]
        attempts = row["attempts"]
        max_retries = row["max_retries"]
        
        if attempts < max_retries:
            delay = base ** attempts
            next_run = (now + datetime.timedelta(seconds=delay)).isoformat()
            conn.execute(
                """
                UPDATE jobs
                SET state = 'failed',
                    updated_at = ?,
                    next_attempt_at = ?,
                    locked_by = NULL,
                    locked_at = NULL,
                    lease_expires_at = NULL
                WHERE id = ? AND state = 'processing';
                """,
                (now_str, next_run, job_id)
            )
        else:
            conn.execute(
                """
                UPDATE jobs
                SET state = 'dead',
                    updated_at = ?,
                    next_attempt_at = NULL,
                    locked_by = NULL,
                    locked_at = NULL,
                    lease_expires_at = NULL
                WHERE id = ? AND state = 'processing';
                """,
                (now_str, job_id)
            )
    conn.commit()

def register_worker(conn: sqlite3.Connection, worker_id: str, pid: int) -> None:
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO workers (id, pid, started_at, last_seen, stop_requested) VALUES (?, ?, ?, ?, 0);",
        (worker_id, pid, now, now)
    )
    conn.commit()

def unregister_worker(conn: sqlite3.Connection, worker_id: str) -> None:
    conn.execute("DELETE FROM workers WHERE id = ?;", (worker_id,))
    conn.commit()

def update_worker_heartbeat(conn: sqlite3.Connection, worker_id: str) -> None:
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    conn.execute("UPDATE workers SET last_seen = ? WHERE id = ?;", (now, worker_id))
    conn.commit()

def check_stop_requested(conn: sqlite3.Connection, worker_id: str) -> bool:
    row = conn.execute("SELECT stop_requested FROM workers WHERE id = ?;", (worker_id,)).fetchone()
    if row is None:
        return True
    return bool(row["stop_requested"])

def request_all_workers_stop(conn: sqlite3.Connection) -> int:
    now = datetime.datetime.now(datetime.timezone.utc)
    window = (now - datetime.timedelta(seconds=15)).isoformat()
    cur = conn.execute("UPDATE workers SET stop_requested = 1 WHERE datetime(last_seen) >= datetime(?);", (window,))
    conn.commit()
    return cur.rowcount

def count_active_workers(conn: sqlite3.Connection, heartbeat_window_seconds: int = 15) -> int:
    now = datetime.datetime.now(datetime.timezone.utc)
    window = (now - datetime.timedelta(seconds=heartbeat_window_seconds)).isoformat()
    
    conn.execute("DELETE FROM workers WHERE datetime(last_seen) < datetime(?, '-60 seconds');", (now.isoformat(),))
    conn.commit()
    
    row = conn.execute(
        "SELECT COUNT(*) FROM workers WHERE datetime(last_seen) >= datetime(?);",
        (window,)
    ).fetchone()
    return row[0] if row else 0

