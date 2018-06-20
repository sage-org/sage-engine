#!/usr/bin/env bash

CONFIG_FILE=$1
NB_WORKERS=${2:-4}
PORT=${3:-8000}

if [ "$#" -lt 1 ]; then
  echo "Illegal number of parameters."
  echo "Usage: ./run.sh <config-file-path> [nb-workers] [port]"
  exit
fi

export YALDF_CONFIG="${CONFIG_FILE}"

gunicorn -w $NB_WORKERS -b 0.0.0.0:$PORT --log-level info http_server.server:app
