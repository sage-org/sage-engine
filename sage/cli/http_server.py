# http_server.py
# Author: Thomas MINIER - MIT License 2017-2020
from asyncio import set_event_loop_policy
from os import environ

import click
import uvicorn
import uvloop

set_event_loop_policy(uvloop.EventLoopPolicy())


@click.command()
@click.argument("config")
@click.option("-p", "--port", type=int, default=8000, show_default=True, help="The port to bind")
@click.option("-w", "--workers", type=int, default=4, show_default=True, help="he number of server workers")
@click.option('-h', "--host", type=str, default="0.0.0.0", show_default=True, help="Set the host address.")
@click.option("--log-level", type=click.Choice(["debug", "info", "warning", "error"]), default="info", show_default=True, help="The granularity of log outputs")
def start_sage_server(config, port, workers, host, log_level):
  """Launch the Sage server using the CONFIG configuration file"""
  environ['SAGE_CONFIG_FILE'] = config
  uvicorn.run("sage.http_server.server:app", port=port, host=host, workers=workers, log_level=log_level)
