import click
from enqueue import enqueueJobs
from worker import start_workers  
from db import init_db
from datetime import datetime

@click.group()
def cli():
    init_db()

@cli.command()
@click.argument("job_str")
def enqueue(job_str):
    enqueueJobs(job_str)
    click.echo("Enqueued")

@cli.group()
def worker():
    pass

@worker.command()
@click.option('--count', default=1)
def start(count):
    start_workers(count)

@cli.command()
def status():
    """Show job queue status"""
    from db import getConn
    with getConn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
        rows = cur.fetchall()

    click.echo(f"\n🕒 Status checked at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo("📊 Job Summary:")
    if rows:
        for state, count in rows:
            click.echo(f"  {state.capitalize():<12}: {count}")
    else:
        click.echo("  No jobs found.")

if __name__ == '__main__':
    cli()
