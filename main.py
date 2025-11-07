import click
from enqueue import enqueue_job
from worker import start_workers  
from db import init_db

@click.group()
def cli():
    init_db()

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    enqueue_job(job_json)
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
    pass

if __name__ == '__main__':
    cli()
