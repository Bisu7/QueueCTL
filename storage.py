import sqlite3
import json
import threading
from datetime import datetime, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    command TEXT,
    state TEXT,
    attempts INTEGER,
    max_retries INTEGER,
    priority INTEGER DEFAULT 0,
    run_at TEXT,
    created_at TEXT,
    updated_at TEXT,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_state_priority ON jobs(state, priority DESC, created_at);
"""

class JobStorage:
    def __init__(self, path="queue.db"):
        self.path = path
        self._lock = threading.Lock()
        self._conn = None
        self._connect()
        self.init_db()

    def _connect(self):
        self._conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row

    def init_db(self):
        with self._conn:
            self._conn.executescript(SCHEMA)

    def add_job(self, job: dict):
        payload = json.dumps(job)
        command = job.get("command")
        with self._conn:
            self._conn.execute(
                "INSERT INTO jobs(id,payload,command,state,attempts,max_retries,created_at,updated_at,last_error) VALUES(?,?,?,?,?,?,?,?,?)",
                (job["id"], payload, command, job["state"], job["attempts"], job["max_retries"], job["created_at"], job["updated_at"], None)
            )

    def counts_by_state(self):
        cur = self._conn.execute("SELECT state, COUNT(*) c FROM jobs GROUP BY state")
        rows = {r["state"]: r["c"] for r in cur.fetchall()}
        # ensure keys
        for k in ("pending","processing","completed","failed","dead"):
            rows.setdefault(k, 0)
        return rows

    def list_jobs(self, state: Optional[str]=None, limit: int=100):
        if state:
            cur = self._conn.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?", (state, limit))
        else:
            cur = self._conn.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
        res = []
        for r in cur.fetchall():
            obj = json.loads(r["payload"])
            
            obj["state"] = r["state"]
            obj["attempts"] = r["attempts"]
            obj["max_retries"] = r["max_retries"]
            obj["created_at"] = r["created_at"]
            obj["updated_at"] = r["updated_at"]
            obj["last_error"] = r["last_error"]
            res.append(obj)
        return res

    def fetch_and_lock_pending(self):
        with self._conn:
            cur = self._conn.execute("SELECT id, payload, attempts, max_retries FROM jobs "
                "WHERE state='pending' AND (run_at IS NULL OR run_at <= CURRENT_TIMESTAMP) "
                "ORDER BY created_at LIMIT 1"
                )
            row = cur.fetchone()
            if not row:
                return None
            job_id = row["id"]
            now = datetime.now(timezone.utc).isoformat()
            updated = self._conn.execute("UPDATE jobs SET state=?, updated_at=? WHERE id=? AND state='pending'", ("processing", now, job_id)).rowcount
            if updated:
                r = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
                return json.loads(r["payload"])
            return None

    def update_job_completion(self, job_id, state, attempts=None, last_error=None):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            if attempts is None:
                self._conn.execute("UPDATE jobs SET state=?, updated_at=?, last_error=? WHERE id=?", (state, now, last_error, job_id))
            else:
                self._conn.execute("UPDATE jobs SET state=?, attempts=?, updated_at=?, last_error=? WHERE id=?", (state, attempts, now, last_error, job_id))

    def increment_attempts_and_backoff(self, job_id):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            cur = self._conn.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (job_id,))
            r = cur.fetchone()
            if not r:
                return None
            attempts = r["attempts"] + 1
            self._conn.execute("UPDATE jobs SET attempts=?, state='failed', updated_at=? WHERE id=?", (attempts, now, job_id))
            return attempts, r["max_retries"]

    def move_to_dead(self, job_id, last_error=None):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute("UPDATE jobs SET state='dead', updated_at=?, last_error=? WHERE id=?", (now, last_error, job_id))

    def retry_dead_job(self, job_id):
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            cur = self._conn.execute("SELECT id FROM jobs WHERE id=? AND state='dead'", (job_id,))
            if cur.fetchone():
                self._conn.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=?, last_error=NULL WHERE id=?", (now, job_id))
                return True
            return False

    def get_job(self, job_id):
        cur = self._conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        r = cur.fetchone()
        return json.loads(r["payload"]) if r else None
