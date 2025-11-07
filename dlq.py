# dlq.py
from db import getConn
from utils import now_iso

def list_dlq_jobs():
    """Return all jobs currently in the Dead Letter Queue (state='dead')."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, command, attempts, max_retries, updated_at FROM jobs WHERE state='dead'")
        rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                "id": row[0],
                "command": row[1],
                "attempts": row[2],
                "max_retries": row[3],
                "updated_at": row[4]
            })
        return result

def retry_dlq_job(job_id):
    """
    Retry a DLQ job: reset its state to 'pending', attempts=0,
    next_run_at=now, and clear locked_by.
    """
    now = now_iso()
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT state FROM jobs WHERE id=?", (job_id,))
        row = cur.fetchone()
        if not row:
            return f"Job '{job_id}' not found."
        if row[0] != "dead":
            return f"Job '{job_id}' is not in DLQ (state={row[0]})."

        cur.execute("""
            UPDATE jobs
            SET state='pending', attempts=0, locked_by=NULL,
                next_run_at=?, updated_at=?
            WHERE id=?
        """, (now, now, job_id))
        conn.commit()
        return f"Job '{job_id}' moved from DLQ back to queue."

def clear_dlq():
    """Completely remove all jobs from DLQ (permanent delete)."""
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM jobs WHERE state='dead'")
        conn.commit()
        return "DLQ cleared successfully."
