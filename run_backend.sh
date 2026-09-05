#!/usr/bin/env bash
# RecoverSense — start the FastAPI backend locally.
set -e

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from .env.example — edit it to add API keys (optional; the system runs fine without them)."
fi

export $(grep -v '^#' .env | xargs -0 2>/dev/null || true)

cd backend
echo ""
echo "Starting RecoverSense API on http://localhost:8000  (docs at /docs)"
echo ""
uvicorn app.main:app --reload --port 8000
