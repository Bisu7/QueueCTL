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
    """Show the current status of the job queue and active workers."""
    conn = get_connection()
    try:
        # Get summary count per state
        rows = conn.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state;").fetchall()
        counts = {r["state"]: r["cnt"] for r in rows}
        
        from queuectl.db import count_active_workers
        active_workers = count_active_workers(conn)
        
        total_jobs = sum(counts.values())
        if total_jobs == 0:
            click.echo("no jobs yet")
        else:
            click.echo(f"Active Workers: {active_workers}")
            for state in ["pending", "processing", "completed", "failed", "dead"]:
                click.echo(f"Jobs in {state:<10}: {counts.get(state, 0)}")
    finally:
        conn.close()

@main.command()
@click.argument("job_json", nargs=-1, required=True)
def enqueue(job_json: tuple) -> None:
    """Enqueue a job using a JSON payload."""
    job_json_str = " ".join(job_json)
    try:
        parsed = json.loads(job_json_str)
    except json.JSONDecodeError:
        # Fallback lenient parser for shells (like PowerShell) that strip double quotes
        import re
        s = job_json_str.strip()
        if s.startswith('{'):
            s = s[1:]
        if s.endswith('}'):
            s = s[:-1]
            
        keys = ["id", "command", "state", "attempts", "max_retries", "priority", "run_at", "timeout_seconds"]
        occurrences = []
        for key in keys:
            pattern = rf'(?:["\'\s]|^)({key})(?:["\']|\b)\s*:\s*'
            for m in re.finditer(pattern, s):
                occurrences.append({
                    "key": key,
                    "start": m.start(),
                    "val_start": m.end()
                })
                
        occurrences.sort(key=lambda x: x["start"])
        
        parsed = {}
        for i, occ in enumerate(occurrences):
            key = occ["key"]
            val_start = occ["val_start"]
            val_end = occurrences[i+1]["start"] if i + 1 < len(occurrences) else len(s)
            
            val = s[val_start:val_end].strip()
            if val.endswith(','):
                val = val[:-1].strip()
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1].strip()
                
            parsed[key] = val
            
        if not parsed or "id" not in parsed or "command" not in parsed:
            click.echo(f"Error: Invalid JSON payload and fallback parser failed to parse it: {job_json_str}", err=True)
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
        default_max_retries = int(get_config(conn, "max-retries", "3"))
        
        state = parsed.get("state", "pending")
        attempts = int(parsed.get("attempts", 0))
        max_retries = int(parsed.get("max_retries", default_max_retries))
        priority = int(parsed.get("priority", 0))
        run_at = parsed.get("run_at")
        timeout_seconds = parsed.get("timeout_seconds")
        if timeout_seconds is not None:
            timeout_seconds = int(timeout_seconds)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
        conn.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries, priority, run_at, timeout_seconds, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (job_id, command, state, attempts, max_retries, priority, run_at, timeout_seconds, now, now)
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
                conn.execute("UPDATE workers SET last_seen = ? WHERE id = ?;", (now.isoformat(), self.worker_id))
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
    
    from queuectl.db import register_worker, unregister_worker, update_worker_heartbeat, check_stop_requested, reap_expired_jobs
    
    try:
        register_worker(conn, worker_id, os.getpid())
        
        while not stop_requested and not stop_event.is_set():
            try:
                reap_expired_jobs(conn)
            except sqlite3.OperationalError:
                pass
                
            try:
                update_worker_heartbeat(conn, worker_id)
                if check_stop_requested(conn, worker_id):
                    break
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
            output = ""
            try:
                result = subprocess.run(
                    job.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=job.timeout_seconds
                )
                exit_code = result.returncode
                output = (result.stdout or "") + (result.stderr or "")
            except subprocess.TimeoutExpired as e:
                exit_code = -999
                stdout_str = e.stdout.decode('utf-8', errors='ignore') if isinstance(e.stdout, bytes) else (e.stdout or "")
                stderr_str = e.stderr.decode('utf-8', errors='ignore') if isinstance(e.stderr, bytes) else (e.stderr or "")
                output = f"TIMEOUT EXPIRED after {job.timeout_seconds}s\n" + stdout_str + stderr_str
            except Exception as e:
                exit_code = -1
                output = f"ERROR: Failed to execute: {e}"
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
                        next_attempt_at = NULL,
                        output = ?
                    WHERE id = ?;
                    """,
                    (now, output, job.id)
                )
                conn.commit()
            else:
                base = int(get_config(conn, "backoff-base", "2"))
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
                            lease_expires_at = NULL,
                            output = ?
                        WHERE id = ?;
                        """,
                        (now, next_run, output, job.id)
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
                            lease_expires_at = NULL,
                            output = ?
                        WHERE id = ?;
                        """,
                        (now, output, job.id)
                    )
                conn.commit()
    finally:
        try:
            unregister_worker(conn, worker_id)
        except Exception:
            pass
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

@worker.command("stop")
def stop_workers() -> None:
    """Gracefully stop all running workers."""
    db_path = get_db_path()
    conn = get_connection(db_path)
    try:
        from queuectl.db import request_all_workers_stop, count_active_workers
        active_count = count_active_workers(conn)
        if active_count == 0:
            click.echo("No active workers running.")
            return
            
        click.echo(f"Signaling {active_count} active worker(s) to stop...")
        request_all_workers_stop(conn)
        
        start_time = time.time()
        while count_active_workers(conn) > 0:
            time.sleep(0.5)
            if time.time() - start_time > 60:
                click.echo("\nTimeout waiting for workers to stop gracefully. Force exiting.")
                return
                
        click.echo("All workers stopped successfully.")
    finally:
        conn.close()

@main.group()
def dlq() -> None:
    """Manage the Dead Letter Queue (DLQ)."""
    pass

@dlq.command("list")
@click.option("--json", "as_json", is_flag=True, help="Output results as raw JSON to stdout.")
def dlq_list(as_json: bool) -> None:
    """List all dead jobs in the DLQ."""
    conn = get_connection(get_db_path())
    try:
        rows = conn.execute("SELECT * FROM jobs WHERE state = 'dead' ORDER BY created_at ASC;").fetchall()
        jobs = [Job.from_row(row) for row in rows]
        
        if as_json:
            click.echo(json.dumps([job.to_dict() for job in jobs]))
        else:
            if not jobs:
                click.echo("No dead jobs found in DLQ")
                return
            for job in jobs:
                click.echo(f"{job.id:<12} | attempts: {job.attempts}/{job.max_retries:<2} | {job.command}")
    finally:
        conn.close()

@dlq.command("retry")
@click.argument("job_id")
def dlq_retry(job_id: str) -> None:
    """Re-enqueue a dead job by its ID, resetting attempts to 0."""
    conn = get_connection(get_db_path())
    try:
        row = conn.execute("SELECT id FROM jobs WHERE id = ? AND state = 'dead';", (job_id,)).fetchone()
        if row is None:
            click.echo(f"Error: Job '{job_id}' is not in the DLQ (dead state).", err=True)
            sys.exit(1)
            
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        conn.execute(
            """
            UPDATE jobs
            SET state = 'pending',
                attempts = 0,
                updated_at = ?,
                locked_by = NULL,
                locked_at = NULL,
                lease_expires_at = NULL,
                next_attempt_at = NULL
            WHERE id = ?;
            """,
            (now, job_id)
        )
        conn.commit()
        click.echo(f"Job '{job_id}' successfully re-enqueued from DLQ.")
    finally:
        conn.close()

@main.group()
def config() -> None:
    """Manage queue configuration options."""
    pass

@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str) -> None:
    """Set a configuration option (max-retries or backoff-base)."""
    if key not in ["max-retries", "backoff-base"]:
        click.echo(f"Error: Invalid configuration key '{key}'. Must be 'max-retries' or 'backoff-base'.", err=True)
        sys.exit(1)
        
    try:
        int(value)
    except ValueError:
        click.echo(f"Error: Configuration value for '{key}' must be an integer.", err=True)
        sys.exit(1)
        
    conn = get_connection(get_db_path())
    try:
        from queuectl.config import set_config
        set_config(conn, key, value)
        click.echo(f"Configuration '{key}' set to '{value}'")
    finally:
        conn.close()

@config.command("get")
@click.argument("key")
def config_get(key: str) -> None:
    """Get the value of a configuration option."""
    if key not in ["max-retries", "backoff-base"]:
        click.echo(f"Error: Unknown configuration key '{key}'", err=True)
        sys.exit(1)
        
    conn = get_connection(get_db_path())
    try:
        from queuectl.config import get_config
        val = get_config(conn, key)
        if val is None:
            val = "3" if key == "max-retries" else "2"
        click.echo(val)
    finally:
        conn.close()

@main.command("logs")
@click.argument("job_id")
def logs(job_id: str) -> None:
    """Print the stdout/stderr output of the latest attempt for a job."""
    conn = get_connection(get_db_path())
    try:
        row = conn.execute("SELECT output FROM jobs WHERE id = ?;", (job_id,)).fetchone()
        if row is None:
            click.echo(f"Error: Job '{job_id}' not found", err=True)
            sys.exit(1)
        output = row["output"]
        if output is None or output == "":
            click.echo("No logs recorded for this job.")
        else:
            click.echo(output)
    finally:
        conn.close()

@main.command("metrics")
@click.option("--minutes", type=int, default=10, help="Window in minutes for throughput metrics.")
def metrics(minutes: int) -> None:
    """Show throughput metrics and status summaries."""
    conn = get_connection(get_db_path())
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        cutoff = (now - datetime.timedelta(minutes=minutes)).isoformat()
        
        # Throughput query
        rows = conn.execute(
            """
            SELECT state, COUNT(*) as cnt 
            FROM jobs 
            WHERE state IN ('completed', 'failed', 'dead') AND datetime(updated_at) >= datetime(?)
            GROUP BY state;
            """,
            (cutoff,)
        ).fetchall()
        throughput = {r["state"]: r["cnt"] for r in rows}
        
        # Average time to completion
        comp_rows = conn.execute("SELECT created_at, updated_at FROM jobs WHERE state = 'completed';").fetchall()
        durations = []
        for r in comp_rows:
            try:
                c = datetime.datetime.fromisoformat(r["created_at"])
                u = datetime.datetime.fromisoformat(r["updated_at"])
                durations.append((u - c).total_seconds())
            except Exception:
                pass
        avg_completion = sum(durations) / len(durations) if durations else 0.0
        
        # Queue depth
        depth_rows = conn.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state;").fetchall()
        depths = {r["state"]: r["cnt"] for r in depth_rows}
        
        click.echo(f"--- Queue Metrics (Throughput Window: Last {minutes} mins) ---")
        click.echo(f"Completed Jobs : {throughput.get('completed', 0)}")
        click.echo(f"Failed Jobs    : {throughput.get('failed', 0)}")
        click.echo(f"Dead Jobs      : {throughput.get('dead', 0)}")
        click.echo(f"Avg Time-to-Completion: {avg_completion:.2f} seconds")
        click.echo("\n--- Current Queue Depth ---")
        for state in ["pending", "processing", "completed", "failed", "dead"]:
            click.echo(f"Jobs in {state:<10}: {depths.get(state, 0)}")
    finally:
        conn.close()

def _load_template() -> str:
    template_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(template_path, encoding="utf-8") as f:
        return f.read()

def render_dashboard_html(counts, recent_jobs, active_workers: int) -> str:
    if not recent_jobs:
        rows = '<tr><td colspan="6" class="empty">No jobs found</td></tr>'
    else:
        rows = ""
        for job in recent_jobs:
            state = job.state.strip()
            rows += (
                f'<tr>'
                f'<td>{job.id}</td>'
                f'<td>{job.command}</td>'
                f'<td><span class="state {state}">{state}</span></td>'
                f'<td>{job.attempts}/{job.max_retries}</td>'
                f'<td>{job.priority}</td>'
                f'<td>{job.updated_at}</td>'
                f'</tr>\n'
            )


    import datetime as _dt
    now_str = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return (
        _load_template()
        .replace("{{pending}}",    str(counts.get("pending",    0)))
        .replace("{{processing}}", str(counts.get("processing", 0)))
        .replace("{{completed}}",  str(counts.get("completed",  0)))
        .replace("{{failed}}",     str(counts.get("failed",     0)))
        .replace("{{dead}}",       str(counts.get("dead",       0)))
        .replace("{{rows}}",       rows)
        .replace("{{workers}}",    str(active_workers))
        .replace("{{now}}",        now_str)
    )

@main.command("dashboard")
@click.option("--port", type=int, default=8000, help="Port to run the web dashboard on.")
def dashboard(port: int) -> None:
    """Start a read-only web dashboard (open http://localhost:<port>/)."""
    import http.server
    import socketserver

    class DashboardHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/":
                self.send_error(404, "Not Found")
                return

            db_path = get_db_path()
            conn = get_connection(db_path)
            try:
                rows = conn.execute(
                    "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state;"
                ).fetchall()
                counts = {r["state"]: r["cnt"] for r in rows}

                recent_rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 20;"
                ).fetchall()
                recent_jobs = [Job.from_row(r) for r in recent_rows]

                from queuectl.db import count_active_workers
                active_workers = count_active_workers(conn)
            finally:
                conn.close()

            html = render_dashboard_html(counts, recent_jobs, active_workers)
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", port), DashboardHandler) as httpd:
        click.echo(f"Dashboard running at http://localhost:{port}/  (Ctrl+C to stop)")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            click.echo("\nStopping dashboard.")

if __name__ == "__main__":
    main()


