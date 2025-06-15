#!/usr/bin/env bash
# Unified entrypoint for TabulaRasa BI Core demo
# Usage: ./run.sh [prod|test|kill|down]
set -e
cd "$(dirname "$0")"

ROOT_DIR=$(pwd)
SCRIPTS_DIR="$ROOT_DIR/root/scripts"
DOCKER_DIR="$ROOT_DIR/root/docker"

function usage() {
  cat <<EOF
Usage: ./run.sh <command>

Commands:
  prod   Launch the main pipeline (Spark + Spring Boot) via run_q1_app.sh
  test   Execute the full end-to-end test harness via run_e2e_test.sh
  kill   Terminate demo-related Java, Python and Spark processes
  dash   Start FastAPI dashboard backend on http://localhost:8000
  down   Same as 'kill' plus docker-compose down -v for project stacks
EOF
}

function kill_processes() {
  echo "[INFO] Killing demo-related processes…"
  # Spring Boot jar
  pkill -f "q1_realtime_stream_processing.*spring-boot.jar" 2>/dev/null || true
  # Spark submit jobs
  pkill -f "AdEventSparkStreamer" 2>/dev/null || true
  # Python producer
  pkill -f "ad_events_producer.py" 2>/dev/null || true
  # FastAPI uvicorn server
  pkill -f "uvicorn main:app" 2>/dev/null || true
}

case "$1" in
  prod)
    bash "$SCRIPTS_DIR/run_q1_app.sh"
    ;;
  test)
    shift # remove 'test' positional
    bash "$SCRIPTS_DIR/run_e2e_test.sh" "$@"
    ;;
  kill)
    kill_processes
    ;;
  dash)
    kill_processes  # ensure no stale uvicorn
    echo "[INFO] Launching FastAPI dashboard backend (http://localhost:8000)… (Ctrl+C to stop)"
    
    # Activate conda environment if it exists and is specified
    if [ -n "$CONDA_EXE" ] && [ -n "$CONDA_PREFIX" ]; then
      echo "[INFO] Activating conda environment: taboola"
      source "$(dirname "$CONDA_EXE")/../bin/activate" taboola
    fi

    cd "$ROOT_DIR/root/dashboard_backend"
    exec uvicorn main:app --reload
    ;;
  down)
    kill_processes
    if [ -d "$DOCKER_DIR" ]; then
      echo "[INFO] Docker stack down…"
      (cd "$DOCKER_DIR" && docker compose down -v --remove-orphans || true)
    fi
    ;;
  ""|help|-h|--help)
    usage
    ;;
  *)
    echo "[ERROR] Unknown command: $1" >&2
    usage
    exit 1
    ;;
esac
