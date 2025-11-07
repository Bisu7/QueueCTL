# models.py
import json
from datetime import datetime, timedelta, timezone
from db import getConn
from utils import now_iso, gen_id

def create_job(command, max_retries=3, base_backoff=2, timeout=None, run_at=None):
    """Insert a new job into the queue."""
    job_id = gen_id("job")
    created = now_iso()
    next_run = run_at or created
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs (
                id, command, state, attempts, max_retries, base_backoff,
                created_at, updated_at, next_run_at, timeout, output_log
            )
            VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id, command, max_retries, base_backoff,
            created, created, next_run, timeout, None
        ))
        conn.commit()
    return job_id

def get_job(job_id):
    """Fetch a job by ID as a dict."""
    with getConn() as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        return cur.fetchone()

def dict_factory(cursor, row):
    """Convert SQLite row to dict."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def update_job_state(job_id, state, output_log=None, locked_by=None):
    """Update job state and optional fields."""
    now = now_iso()
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE jobs SET state=?, updated_at=?, output_log=?, locked_by=NULL
            WHERE id=?
        """, (state, now, json.dumps(output_log) if output_log else None, job_id))
        conn.commit()

def schedule_retry(job_id, attempts, base_backoff):
    """Update a job to retry after exponential backoff."""
    delay = base_backoff ** attempts
    next_run = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE jobs
            SET state='pending', next_run_at=?, updated_at=?, locked_by=NULL
            WHERE id=?
        """, (next_run, now_iso(), job_id))
        conn.commit()

def mark_dead(job_id, output_log=None):
    """Move a job to DLQ (dead state)."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE jobs
            SET state='dead', updated_at=?, output_log=?, locked_by=NULL
            WHERE id=?
        """, (now_iso(), json.dumps(output_log) if output_log else None, job_id))
        conn.commit()

def list_jobs(state=None, limit=20):
    """List jobs by state or all if None."""
    with getConn() as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?", (state, limit))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
        return cur.fetchall()
