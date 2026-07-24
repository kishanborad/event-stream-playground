#!/usr/bin/env bash
# demo.sh — Launch a full local event-stream-playground demo
#
# Starts three concurrent processes:
#   1. event_producer.py — generates events at configurable rate
#   2. event_consumer.py — subscribes and logs metrics
#   3. npm run dev        — Vite dev server for the browser visualization
#
# Cleans up all background processes on exit (Ctrl-C or error).
#
# Usage:
#   ./scripts/demo.sh
#   ./scripts/demo.sh --rate 100 --partitions 8
#   ./scripts/demo.sh --no-frontend  # run only producer + consumer
#   ./scripts/demo.sh --mode kafka   # run Kafka simulator instead

set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────────── Colors ──────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
RESET='\033[0m'

info()     { echo -e "${CYAN}[demo]${RESET} $*"; }
success()  { echo -e "${GREEN}[demo] ✔${RESET} $*"; }
warn()     { echo -e "${YELLOW}[demo] ⚠${RESET} $*"; }
error()    { echo -e "${RED}[demo] ✘${RESET} $*" >&2; }
producer() { echo -e "${GREEN}[producer]${RESET} $*"; }
consumer() { echo -e "${MAGENTA}[consumer]${RESET} $*"; }
frontend() { echo -e "${CYAN}[frontend]${RESET} $*"; }

# ─────────────────────────────── Paths ───────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_DIR="$PROJECT_ROOT/python"
VENV_DIR="$PYTHON_DIR/.venv"
LOG_DIR="/tmp/esp-demo-logs"
EVENT_LOG="$LOG_DIR/events.jsonl"

mkdir -p "$LOG_DIR"

# ─────────────────────────────── Defaults ────────────────────────────────────
RATE=25
PARTITIONS=4
EVENT_TYPES="PageView Click Purchase Heartbeat MetricSample SensorReading"
FRONTEND=true
MODE="stream"   # "stream" or "kafka"
FRONTEND_PORT=5173

# ─────────────────────────────── Argument parsing ────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate)         shift; RATE="$1"           ;;
    --partitions)   shift; PARTITIONS="$1"     ;;
    --event-types)  shift; EVENT_TYPES="$1"    ;;
    --no-frontend)  FRONTEND=false              ;;
    --mode)         shift; MODE="$1"            ;;
    --port)         shift; FRONTEND_PORT="$1"   ;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo ""
      echo "  --rate <n>           Event rate in eps (default: 25)"
      echo "  --partitions <n>     Number of partitions (default: 4)"
      echo "  --no-frontend        Skip starting the Vite dev server"
      echo "  --mode kafka         Run Kafka simulator instead of stream"
      echo "  --port <n>           Vite dev server port (default: 5173)"
      exit 0
      ;;
    *) warn "Unknown argument: $1" ;;
  esac
  shift
done

# ─────────────────────────────── Process tracking ────────────────────────────
declare -a PIDS=()

cleanup() {
  echo ""
  echo -e "${YELLOW}[demo] Shutting down...${RESET}"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
  echo -e "${GREEN}[demo] Stopped. Event log saved at: ${CYAN}$EVENT_LOG${RESET}"
  echo ""
}
trap cleanup EXIT INT TERM

# ─────────────────────────────── Prerequisites ───────────────────────────────
if [[ -f "$VENV_DIR/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$VENV_DIR/bin/activate"
  PYTHON_BIN="python3"
else
  PYTHON_BIN="python3"
  if ! command -v python3 &>/dev/null; then
    error "python3 not found. Run ./scripts/setup.sh first."
    exit 1
  fi
fi

# ─────────────────────────────── Banner ──────────────────────────────────────
echo ""
echo -e "${BOLD}Event-Stream Playground — Local Demo${RESET}"
echo ""
echo -e "  Rate:        ${GREEN}$RATE events/s${RESET}"
echo -e "  Partitions:  ${GREEN}$PARTITIONS${RESET}"
echo -e "  Mode:        ${GREEN}$MODE${RESET}"
echo -e "  Log:         ${CYAN}$EVENT_LOG${RESET}"
echo -e "  Logs dir:    ${CYAN}$LOG_DIR${RESET}"
echo ""

# ─────────────────────────────── Kafka sim mode ──────────────────────────────
if [[ "$MODE" == "kafka" ]]; then
  info "Launching Kafka simulator demo..."
  "$PYTHON_BIN" "$PYTHON_DIR/kafka_simulator.py" demo \
    --partitions "$PARTITIONS" \
    --events 500 \
    2>&1 | while IFS= read -r line; do
      echo -e "${CYAN}[kafka-sim]${RESET} $line"
    done
  exit 0
fi

# ─────────────────────────────── Start producer ──────────────────────────────
info "Starting event producer → $EVENT_LOG"

# shellcheck disable=SC2086
"$PYTHON_BIN" "$PYTHON_DIR/event_producer.py" \
  --mode stdout \
  --rate "$RATE" \
  --partitions "$PARTITIONS" \
  --output-file "$EVENT_LOG" \
  2>"$LOG_DIR/producer.stderr" &

PRODUCER_PID=$!
PIDS+=("$PRODUCER_PID")
producer "PID $PRODUCER_PID — writing to $EVENT_LOG"

# Wait briefly for the producer to create the log file
WAIT_COUNT=0
while [[ ! -f "$EVENT_LOG" ]] && [[ $WAIT_COUNT -lt 30 ]]; do
  sleep 0.1
  WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [[ ! -f "$EVENT_LOG" ]]; then
  warn "Producer didn't create log file yet — consumer will wait"
fi

# ─────────────────────────────── Start consumer ──────────────────────────────
info "Starting event consumer ← $EVENT_LOG"

"$PYTHON_BIN" "$PYTHON_DIR/event_consumer.py" \
  --mode file \
  --source "$EVENT_LOG" \
  --consumer-group demo-group \
  --partitions "$PARTITIONS" \
  --stats-format table \
  2>"$LOG_DIR/consumer.stderr" &

CONSUMER_PID=$!
PIDS+=("$CONSUMER_PID")
consumer "PID $CONSUMER_PID"

# ─────────────────────────────── Start frontend ──────────────────────────────
if $FRONTEND; then
  cd "$PROJECT_ROOT"

  if [[ ! -d node_modules ]]; then
    info "Installing Node dependencies..."
    npm ci
  fi

  info "Starting Vite dev server on port $FRONTEND_PORT"
  npx vite --port "$FRONTEND_PORT" \
    2>"$LOG_DIR/vite.stderr" &
  VITE_PID=$!
  PIDS+=("$VITE_PID")

  # Wait for Vite to print its URL
  sleep 2
  frontend "PID $VITE_PID — http://localhost:$FRONTEND_PORT"
fi

# ─────────────────────────────── Status dashboard ────────────────────────────
echo ""
echo -e "${BOLD}══ Demo Running ══${RESET}"
echo ""
echo -e "  Producer:    ${GREEN}PID $PRODUCER_PID${RESET}"
echo -e "  Consumer:    ${GREEN}PID $CONSUMER_PID${RESET}"
if $FRONTEND; then
  echo -e "  Frontend:    ${GREEN}http://localhost:$FRONTEND_PORT${RESET}"
fi
echo ""
echo -e "  Event log:   ${CYAN}$EVENT_LOG${RESET}"
echo -e "  Stderr logs: ${CYAN}$LOG_DIR/*.stderr${RESET}"
echo ""
echo -e "  Press ${BOLD}Ctrl-C${RESET} to stop all processes."
echo ""

# ─────────────────────────────── Live tail ───────────────────────────────────
# Show a rolling count of events every 5 seconds
LAST_COUNT=0
while true; do
  sleep 5

  # Check that producer is still running
  if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
    warn "Producer process exited"
    break
  fi

  if [[ -f "$EVENT_LOG" ]]; then
    CURRENT_COUNT=$(wc -l < "$EVENT_LOG" | tr -d ' ')
    NEW_EVENTS=$((CURRENT_COUNT - LAST_COUNT))
    LAST_COUNT=$CURRENT_COUNT
    RATE_ACTUAL=$((NEW_EVENTS / 5))
    info "Events: ${GREEN}${CURRENT_COUNT}${RESET} total  (+${NEW_EVENTS} in last 5s ~${RATE_ACTUAL} eps)"
  fi
done
