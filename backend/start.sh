#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# start.sh  —  Production startup for InstaAuto backend
#
# Usage:
#   Development:  bash start.sh dev
#   Production:   bash start.sh prod
#   Custom:       WORKERS=8 bash start.sh prod
# ─────────────────────────────────────────────────────────────────────────────

MODE=${1:-dev}

# Auto-detect workers: (2 × CPU cores) + 1  — standard Gunicorn formula
CPU_CORES=$(python -c "import os; print(os.cpu_count() or 2)")
DEFAULT_WORKERS=$(( CPU_CORES * 2 + 1 ))
WORKERS=${WORKERS:-$DEFAULT_WORKERS}

echo "🚀 Starting InstaAuto backend"
echo "   Mode:    $MODE"
echo "   Workers: $WORKERS"
echo "   CPUs:    $CPU_CORES"
echo ""

if [ "$MODE" = "dev" ]; then
    # ── Development: single worker, auto-reload ───────────────────────────────
    echo "⚡ Dev mode — single worker with auto-reload"
    uvicorn main:sio_app \
        --host 0.0.0.0 \
        --port 8000 \
        --reload \
        --log-level debug

elif [ "$MODE" = "prod" ]; then
    # ── Production: multiple workers, optimised settings ─────────────────────
    # NOTE: Socket.io requires sticky sessions when using multiple workers.
    # If deploying on a single machine this works fine.
    # For multi-machine: add a Redis Socket.io adapter (python-socketio[asyncio_client])
    # and set SOCKETIO_MESSAGE_QUEUE=redis://... in your .env
    echo "🏭 Production mode — $WORKERS workers"
    uvicorn main:sio_app \
        --host 0.0.0.0 \
        --port 8000 \
        --workers "$WORKERS" \
        --log-level info \
        --access-log \
        --proxy-headers \
        --forwarded-allow-ips="*"
else
    echo "❌ Unknown mode: $MODE  (use 'dev' or 'prod')"
    exit 1
fi
