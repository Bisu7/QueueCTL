import click
from queuectl.db import get_connection, init_db

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

if __name__ == "__main__":
    main()
