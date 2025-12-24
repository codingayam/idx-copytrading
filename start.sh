#!/bin/sh
# Start script to handle PORT variable expansion correctly
PORT="${PORT:-8000}"
echo "Starting application on port $PORT"
exec uvicorn api:app --host 0.0.0.0 --port "$PORT"
