import json
from storage import JobStorage


class DLQManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.storage = JobStorage(db_path)

    def list_dlq(self, limit=100):
        rows = self.storage.list_jobs(state="dead", limit=limit)
        return rows

    def retry_job(self, job_id: str):
        return self.storage.retry_dead_job(job_id)

    def print_dlq(self, limit=100):
        rows = self.list_dlq(limit)
        if not rows:
            print("🪦 DLQ is empty.")
        for r in rows:
            print(json.dumps(r, default=str))

    def cleanup_old_jobs(self, days_old: int = 7):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days_old)
        cur = self.storage._conn.execute(
            "SELECT id, updated_at FROM jobs WHERE state='dead'"
        )
        to_delete = []
        for row in cur.fetchall():
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
                if updated_at < cutoff:
                    to_delete.append(row["id"])
            except Exception:
                pass
        for job_id in to_delete:
            with self.storage._conn:
                self.storage._conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        if to_delete:
            print(f"Cleaned up {len(to_delete)} old DLQ job(s).")
        else:
            print("No old DLQ jobs to clean up.")
