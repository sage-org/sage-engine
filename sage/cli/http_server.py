# http_server.py
# Author: Thomas MINIER - MIT License 2017-2020
import click
import uvicorn
import uvloop
from asyncio import set_event_loop_policy
from os.path import isfile
from sage.http_server.server import run_app

set_event_loop_policy(uvloop.EventLoopPolicy())

@click.command()
@click.argument("config")
@click.option("-p", "--port", type=int, default=8000, show_default=True, help="The port to bind")
@click.option("-w", "--workers", type=int, default=4, show_default=True, help="he number of server workers")
@click.option("--log-level", type=click.Choice(["debug", "info", "warning", "error"]), default="info", show_default=True, help="The granularity of log outputs")
def start_sage_server(config, port, workers, log_level):
  """Launch the Sage server using the CONFIG configuration file"""
  app = run_app(config)
  uvicorn.run(app, port=port, log_level=log_level)