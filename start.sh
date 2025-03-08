#!/bin/bash
# set -euo pipefail
# # pushd ./routers
# celery -A celery_app worker --loglevel=info

nohup fastapi run app/main.py &

# popd