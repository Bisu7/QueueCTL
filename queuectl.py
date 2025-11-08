import click
import subprocess
import time
import json
import os
import sys

from datetime import datetime, timezone
from typing import Optional

from database import Job, initialize_db, get_session
from worker import start_workers, stop_workers
from config import Config
from cli_utils import (
    STATE_EMOJIS,
    print_job_table,
    get_job_summary,
    check_workers_active,
    get_worker_pids,
    get_log_dir
)

# --- CLI Group ---

@click.group()
def cli():
    """
    queuectl: A CLI-based background job queue system.

    Manages background jobs with worker processes, exponential backoff retries,
    and a Dead Letter Queue (DLQ).
    """
    initialize_db()
    pass

# --- ENQUEUE Command ---

@cli.command()
@click.argument('job_data', type=str)
def enqueue(job_data: str):
    """
    Add a new job to the queue.

    JOB_DATA must be a JSON string containing at least 'command'.
    Example: '{"command": "sleep 2 && echo success", "max_retries": 5}'
    """
    try:
        data = json.loads(job_data)
    except json.JSONDecodeError:
        click.echo(click.style("Error: JOB_DATA is not valid JSON.", fg='red'))
        return

    command = data.get('command')
    if not command:
        click.echo(click.style("Error: Job data must contain a 'command' field.", fg='red'))
        return

    session = get_session()
    try:
        # Get configured max_retries, or use the value from job_data if provided
        default_max_retries = Config.get('max-retries')
        max_retries = data.get('max_retries', default_max_retries)

        new_job = Job(
            command=command,
            state="pending",
            max_retries=max_retries,
            # Other fields like id, created_at, updated_at are set by the ORM/DB
        )

        session.add(new_job)
        session.commit()

        click.echo(click.style(f"✅ Job '{new_job.id}' enqueued successfully.", fg='green'))
        click.echo(f"   Command: {new_job.command}")
        click.echo(f"   Max Retries: {new_job.max_retries}")

    except Exception as e:
        session.rollback()
        click.echo(click.style(f"Error enqueuing job: {e}", fg='red'))
    finally:
        session.close()

# --- WORKER Group ---

@cli.group()
def worker():
    """Manage background worker processes."""
    pass

@worker.command()
@click.option('--count', default=1, type=int, help='Number of workers to start.')
def start(count: int):
    """Start one or more workers in the background."""
    active_count = check_workers_active()
    if active_count > 0:
        click.echo(f"⚠️ {active_count} worker(s) already running.")
        if not click.confirm("Do you want to start additional workers?"):
            return

    try:
        start_workers(count)
        click.echo(click.style(f"🚀 Started {count} worker(s).", fg='green'))
        click.echo("   Use 'queuectl status' to monitor their activity.")
    except Exception as e:
        click.echo(click.style(f"Error starting workers: {e}", fg='red'))

@worker.command()
def stop():
    """Stop all running workers gracefully."""
    if stop_workers():
        click.echo(click.style("🛑 All workers signaled for graceful shutdown.", fg='green'))
        click.echo("   They will finish their current job before exiting.")
    else:
        click.echo("No active workers found.")

# --- STATUS Command ---

@cli.command()
def status():
    """Show summary of all job states and active workers."""
    click.echo(click.style("## 📊 System Status", fg='cyan'))

    # 1. Worker Status
    active_count = check_workers_active()
    pids = get_worker_pids()
    click.echo(f"\n### 🧑‍💻 Worker Status ({len(pids)} Active)")
    if pids:
        click.echo("PIDs: " + ", ".join(map(str, pids)))
    else:
        click.echo(click.style("No workers are currently running.", fg='yellow'))
    
    # 2. Job Summary
    summary = get_job_summary()
    click.echo("\n### 📋 Job Summary")
    summary_data = []
    for state, count in summary.items():
        summary_data.append([
            f"{STATE_EMOJIS.get(state, '')} {state.capitalize()}",
            f"{count} jobs"
        ])
    
    if summary_data:
        click.echo(print_job_table(
            headers=['State', 'Count'],
            data=summary_data
        ))
    else:
        click.echo("No jobs found in the queue.")

# --- LIST Command ---

@cli.command()
@click.option('--state', '-s', type=click.Choice(list(STATE_EMOJIS.keys()) + ['all']), default='all', help='Filter jobs by state.')
@click.option('--limit', '-l', default=10, type=int, help='Limit the number of jobs displayed.')
def list(state: str, limit: int):
    """List jobs by state."""
    session = get_session()
    try:
        query = session.query(Job)
        if state != 'all':
            query = query.filter(Job.state == state)

        jobs = query.order_by(Job.created_at.desc()).limit(limit).all()

        click.echo(click.style(f"## 📜 Job List (State: {state.capitalize()})", fg='cyan'))

        if not jobs:
            click.echo(f"No {state} jobs found.")
            return

        table_data = []
        for job in jobs:
            table_data.append([
                str(job.id)[:8],
                STATE_EMOJIS.get(job.state, '') + ' ' + job.state,
                str(job.attempts),
                job.command[:40] + ('...' if len(job.command) > 40 else ''),
                job.updated_at.strftime('%Y-%m-%d %H:%M:%S UTC') if job.updated_at else 'N/A'
            ])

        click.echo(print_job_table(
            headers=['ID', 'State', 'Attempts', 'Command', 'Updated At'],
            data=table_data
        ))

    finally:
        session.close()

# --- DLQ Group ---

@cli.group()
def dlq():
    """Manage the Dead Letter Queue (DLQ)."""
    pass

@dlq.command(name='list')
@click.option('--limit', '-l', default=10, type=int, help='Limit the number of jobs displayed.')
def dlq_list(limit: int):
    """List jobs permanently failed in the DLQ."""
    session = get_session()
    try:
        jobs = session.query(Job).filter(Job.state == 'dead').order_by(Job.updated_at.desc()).limit(limit).all()

        click.echo(click.style("## ⚰️ Dead Letter Queue", fg='red'))

        if not jobs:
            click.echo("The DLQ is empty.")
            return

        table_data = []
        for job in jobs:
            table_data.append([
                str(job.id)[:8],
                str(job.attempts),
                job.command[:40] + ('...' if len(job.command) > 40 else ''),
                job.updated_at.strftime('%Y-%m-%d %H:%M:%S UTC') if job.updated_at else 'N/A'
            ])

        click.echo(print_job_table(
            headers=['ID', 'Final Attempts', 'Command', 'Moved to DLQ At'],
            data=table_data
        ))

    finally:
        session.close()

@dlq.command()
@click.argument('job_id', type=str)
def retry(job_id: str):
    """
    Retry a specific job from the DLQ.

    This resets the job's state to 'pending' and attempts to 0.
    """
    session = get_session()
    try:
        job: Optional[Job] = session.query(Job).filter(Job.id == job_id, Job.state == 'dead').first()

        if not job:
            click.echo(click.style(f"Error: Dead job with ID '{job_id}' not found.", fg='red'))
            return

        old_attempts = job.attempts
        job.state = 'pending'
        job.attempts = 0
        job.updated_at = datetime.now(timezone.utc)
        session.commit()

        click.echo(click.style(f"🔁 Job '{job_id}' retried successfully.", fg='green'))
        click.echo(f"   Command: {job.command}")
        click.echo(f"   Old Attempts: {old_attempts}. Reset to 0.")
        click.echo("   It will be picked up by a worker soon.")

    except Exception as e:
        session.rollback()
        click.echo(click.style(f"Error retrying job: {e}", fg='red'))
    finally:
        session.close()

# --- CONFIG Group ---

@cli.group()
def config():
    """Manage system configuration."""
    pass

@config.command(name='set')
@click.argument('key')
@click.argument('value')
def config_set(key: str, value: str):
    """Set a configuration KEY to VALUE."""
    if key in Config.ALLOWED_KEYS:
        try:
            Config.set(key, value)
            click.echo(click.style(f"⚙️ Configuration '{key}' set to '{value}'.", fg='green'))
        except ValueError as e:
            click.echo(click.style(f"Error: {e}", fg='red'))
    else:
        click.echo(click.style(f"Error: Unknown configuration key '{key}'. Allowed keys: {', '.join(Config.ALLOWED_KEYS)}", fg='red'))

@config.command(name='get')
@click.argument('key', required=False)
def config_get(key: Optional[str]):
    """Get the value of a configuration KEY, or all if none specified."""
    if key:
        if key in Config.ALLOWED_KEYS:
            value = Config.get(key)
            click.echo(f"Configuration '{key}': {value}")
        else:
            click.echo(click.style(f"Error: Unknown configuration key '{key}'.", fg='red'))
    else:
        click.echo(click.style("## ⚙️ Current Configuration", fg='cyan'))
        config_data = []
        for k in Config.ALLOWED_KEYS:
            config_data.append([k, str(Config.get(k))])

        click.echo(print_job_table(
            headers=['Key', 'Value'],
            data=config_data
        ))

# --- Main Execution ---

# At the bottom of queuectl.py

if __name__ == '__main__':
    # Add check for the special worker flag used in Popen
    if len(sys.argv) > 1 and sys.argv[1] == '__worker__':
        # This branch runs if the process was started by the start_workers Popen call
        # Initialize and run a single worker instance
        initialize_db()
        current_pid = os.getpid()
        
        # On Windows, we still write the PID file, but it's less reliable
        pid_file_path = get_pid_file(current_pid)
        with open(pid_file_path, 'w') as f:
            f.write(str(current_pid))
            
        worker = Worker(current_pid)
        worker.run()
        
    else:
        # This runs for all CLI commands (enqueue, status, worker start, etc.)
        cli()