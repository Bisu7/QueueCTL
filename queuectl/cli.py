import click
import json
import sys
import datetime
import sqlite3
import os
import time
import subprocess
import signal
import multiprocessing
from queuectl.db import get_connection, init_db, claim_next_job, get_db_path
from queuectl.models import Job
from queuectl.config import get_config

@click.group()
def main() -> None:
    """QueueCTL: A CLI background job queue."""
    conn = get_connection()
    try:
        init_db(conn)
    finally:
        conn.close()

@main.command()
def status() -> None:
    """Show the current status of the job queue."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT COUNT(*) FROM jobs;").fetchone()
        count = row[0] if row else 0
        if count == 0:
            click.echo("no jobs yet")
        else:
            click.echo(f"{count} jobs in the queue")
    finally:
        conn.close()

@main.command()
@click.argument("job_json")
def enqueue(job_json: str) -> None:
    """Enqueue a job using a JSON payload."""
    try:
        parsed = json.loads(job_json)
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON payload: {e}", err=True)
        sys.exit(1)
        
    if not isinstance(parsed, dict):
        click.echo("Error: JSON payload must be an object", err=True)
        sys.exit(1)
        
    job_id = parsed.get("id")
    command = parsed.get("command")
    
    if not job_id or not command:
        click.echo("Error: 'id' and 'command' are required fields", err=True)
        sys.exit(1)
        
    conn = get_connection()
    try:
        default_max_retries = int(get_config(conn, "max_retries", "3"))
        
        # Merge input with default values
        state = parsed.get("state", "pending")
        attempts = int(parsed.get("attempts", 0))
        max_retries = int(parsed.get("max_retries", default_max_retries))
        
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (job_id, command, state, attempts, max_retries, now, now)
        )
        conn.commit()
        click.echo(f"Job '{job_id}' enqueued successfully")
    except sqlite3.IntegrityError:
        click.echo(f"Error: Job with ID '{job_id}' already exists", err=True)
        sys.exit(1)
    finally:
        conn.close()

@main.command()
@click.option("--state", type=str, help="Filter jobs by state.")
@click.option("--json", "as_json", is_flag=True, help="Output results as raw JSON to stdout.")
def list(state: str, as_json: bool) -> None:
    """List jobs in the queue."""
    conn = get_connection()
    try:
        query = "SELECT * FROM jobs"
        params = []
        if state:
            query += " WHERE state = ?"
            params.append(state)
        query += " ORDER BY created_at ASC"
        
        rows = conn.execute(query, params).fetchall()
        jobs = [Job.from_row(row) for row in rows]
        
        if as_json:
            # Output ONLY the JSON array to stdout.
            click.echo(json.dumps([job.to_dict() for job in jobs]))
        else:
            if not jobs:
                click.echo("No jobs found")
                return
            for job in jobs:
                click.echo(
                    f"{job.id:<12} | {job.state:<12} | attempts: {job.attempts}/{job.max_retries:<2} | {job.command}"
                )
    finally:
        conn.close()

import threading

class LeaseRenewer:
    def __init__(self, db_path: str, job_id: str, worker_id: str, lease_duration: int):
        self.db_path = db_path
        self.job_id = job_id
        self.worker_id = worker_id
        self.lease_duration = lease_duration
        self.interval = max(1, lease_duration // 3)
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=1.0)

    def _run(self):
        while not self.stop_event.wait(self.interval):
            conn = get_connection(self.db_path)
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                lease_expires = (now + datetime.timedelta(seconds=self.lease_duration)).isoformat()
                conn.execute(
                    """
                    UPDATE jobs
                    SET lease_expires_at = ?,
                        updated_at = ?
                    WHERE id = ? AND locked_by = ? AND state = 'processing';
                    """,
                    (lease_expires, now.isoformat(), self.job_id, self.worker_id)
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass
            finally:
                conn.close()

def worker_run(db_path: str, worker_id: str, stop_event: multiprocessing.Event) -> None:
    stop_requested = False
    
    def handle_signal(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    conn = get_connection(db_path)
    
    while not stop_requested and not stop_event.is_set():
        from queuectl.db import reap_expired_jobs
        try:
            reap_expired_jobs(conn)
        except sqlite3.OperationalError:
            pass
            
        try:
            job = claim_next_job(conn, worker_id)
        except sqlite3.OperationalError:
            time.sleep(1)
            continue
            
        if job is None:
            for _ in range(10):
                if stop_requested or stop_event.is_set():
                    break
                time.sleep(0.1)
            continue
            
        lease_duration = int(get_config(conn, "lease_duration", "15"))
        renewer = LeaseRenewer(db_path, job.id, worker_id, lease_duration)
        renewer.start()
        
        old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            result = subprocess.run(job.command, shell=True)
            exit_code = result.returncode
        except Exception:
            exit_code = -1
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            renewer.stop()
            
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        if exit_code == 0:
            conn.execute(
                """
                UPDATE jobs
                SET state = 'completed',
                    updated_at = ?,
                    locked_by = NULL,
                    locked_at = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NULL
                WHERE id = ?;
                """,
                (now, job.id)
            )
            conn.commit()
        else:
            base = int(get_config(conn, "retry_backoff_base", "2"))
            attempts = job.attempts
            
            if attempts < job.max_retries:
                delay = base ** attempts
                next_run = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay)).isoformat()
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'failed',
                        updated_at = ?,
                        next_attempt_at = ?,
                        locked_by = NULL,
                        locked_at = NULL,
                        lease_expires_at = NULL
                    WHERE id = ?;
                    """,
                    (now, next_run, job.id)
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
                    WHERE id = ?;
                    """,
                    (now, job.id)
                )
            conn.commit()
            
    conn.close()

@main.group()
def worker() -> None:
    """Manage background workers."""
    pass

@worker.command("start")
@click.option("--count", type=int, default=1, help="Number of worker processes to spawn.")
def start_workers(count: int) -> None:
    """Start N background worker processes in the foreground."""
    db_path = get_db_path()
    
    # Pre-verify the database is initialized
    conn = get_connection(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()
        
    stop_event = multiprocessing.Event()
    processes = []
    
    def handle_parent_signal(signum, frame):
        click.echo("\nStopping workers gracefully (waiting for in-flight jobs)...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_parent_signal)
    signal.signal(signal.SIGTERM, handle_parent_signal)
    
    click.echo(f"Starting {count} worker processes...")
    for i in range(count):
        worker_id = f"worker-{i}-{os.getpid()}"
        p = multiprocessing.Process(
            target=worker_run,
            args=(db_path, worker_id, stop_event)
        )
        p.start()
        processes.append(p)
        
    click.echo("Workers started. Press Ctrl+C to stop.")
    
    while any(p.is_alive() for p in processes):
        try:
            for p in processes:
                if p.is_alive():
                    p.join(timeout=0.1)
        except KeyboardInterrupt:
            stop_event.set()
            
    click.echo("All workers stopped.")

if __name__ == "__main__":
    main()

