#!/usr/bin/env bash
# Start the Pointwise data-quality executor (API + console UI).
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
  .venv/bin/pip install -r requirements.txt
fi

export DQ_DB_PATH="${DQ_DB_PATH:-$(pwd)/dq.db}"
PORT="${PORT:-8000}"

echo "Serving on http://127.0.0.1:${PORT}  (DB: ${DQ_DB_PATH})"
exec .venv/bin/uvicorn app.api:app --host 0.0.0.0 --port "${PORT}"
