#!/bin/sh
# Start script for cron job service
# This runs the crawler and aggregation pipeline
echo "Starting cron runner..."
exec python cron_runner.py
