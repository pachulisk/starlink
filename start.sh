#!/bin/bash
set -euo pipefail
# pushd ./routers
celery -A celery_app worker --loglevel=info

# popd