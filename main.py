from db import init_DB
import click

def main():
    init_DB()

@click.group()
def cli():
    init_DB