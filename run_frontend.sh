#!/usr/bin/env bash
# RecoverSense — serve the frontend locally (static files, no build step).
set -e
cd "$(dirname "$0")/frontend"
echo "Serving RecoverSense frontend on http://localhost:5500"
echo "(Make sure the backend is running separately: ./run_backend.sh)"
python3 -m http.server 5500
