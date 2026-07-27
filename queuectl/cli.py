import click
import json
import sys
import datetime
import sqlite3
from queuectl.db import get_connection, init_db
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

if __name__ == "__main__":
    main()
