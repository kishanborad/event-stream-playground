#!/usr/bin/env bash
# docker-run.sh — Build and run the event-stream Docker stack
#
# Handles building the Docker image, creating shared volumes, and launching
# the docker-compose services. Provides commands for monitoring and teardown.
#
# Usage:
#   ./scripts/docker-run.sh                # build + start full stack
#   ./scripts/docker-run.sh --build-only   # build image without starting
#   ./scripts/docker-run.sh --producer     # start only the producer service
#   ./scripts/docker-run.sh --status       # show running containers
#   ./scripts/docker-run.sh --logs         # tail all service logs
#   ./scripts/docker-run.sh --stop         # stop all services
#   ./scripts/docker-run.sh --clean        # stop + remove volumes

set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────────── Colors ──────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[docker]${RESET} $*"; }
success() { echo -e "${GREEN}[docker] ✔${RESET} $*"; }
warn()    { echo -e "${YELLOW}[docker] ⚠${RESET} $*"; }
error()   { echo -e "${RED}[docker] ✘${RESET} $*" >&2; }
die()     { error "$*"; exit 1; }

step() {
  echo ""
  echo -e "${BOLD}${CYAN}══ $* ══${RESET}"
}

# ─────────────────────────────── Paths ───────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EVENT_LOG_PATH="/tmp/esp-event-logs"
IMAGE_NAME="event-stream-playground"
IMAGE_TAG="latest"

# ─────────────────────────────── Docker detection ────────────────────────────
detect_compose() {
  if command -v docker-compose &>/dev/null; then
    echo "docker-compose"
  elif docker compose version &>/dev/null 2>&1; then
    echo "docker compose"
  else
    echo ""
  fi
}

COMPOSE_CMD=$(detect_compose)

require_docker() {
  command -v docker &>/dev/null || die "Docker not found — install from https://docker.com"
  [[ -n "$COMPOSE_CMD" ]] || die "Docker Compose not found"
  docker info &>/dev/null || die "Docker daemon is not running"
}

# ─────────────────────────────── Argument parsing ────────────────────────────
COMMAND="up"
BUILD_ONLY=false
SERVICES=()
DETACH=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-only)  COMMAND="build"; BUILD_ONLY=true ;;
    --producer)    SERVICES+=("producer")            ;;
    --consumer)    SERVICES+=("consumer")            ;;
    --analyzer)    SERVICES+=("analyzer")            ;;
    --status)      COMMAND="status"                  ;;
    --logs)        COMMAND="logs"                    ;;
    --stop)        COMMAND="stop"                    ;;
    --clean)       COMMAND="clean"                   ;;
    --detach|-d)   DETACH=true                       ;;
    --help|-h)
      echo "Usage: $0 [command] [options]"
      echo ""
      echo "Commands (default: start full stack):"
      echo "  --build-only    Build image only, don't start services"
      echo "  --status        Show running containers"
      echo "  --logs          Tail logs from all services"
      echo "  --stop          Stop all services"
      echo "  --clean         Stop and remove volumes"
      echo ""
      echo "Service filters:"
      echo "  --producer      Start/target producer service"
      echo "  --consumer      Start/target consumer service"
      echo "  --analyzer      Start/target analyzer service"
      echo ""
      echo "Options:"
      echo "  --detach/-d     Run services in background"
      exit 0
      ;;
    *) warn "Unknown argument: $1" ;;
  esac
  shift
done

# ─────────────────────────────── Banner ──────────────────────────────────────
echo ""
echo -e "${BOLD}Event-Stream Playground — Docker Runner${RESET}"
echo -e "Project: ${CYAN}$PROJECT_ROOT${RESET}"
echo ""

cd "$PROJECT_ROOT"

# ─────────────────────────────── Status ──────────────────────────────────────
if [[ "$COMMAND" == "status" ]]; then
  require_docker
  echo -e "${BOLD}Running containers:${RESET}"
  $COMPOSE_CMD ps
  exit 0
fi

# ─────────────────────────────── Logs ────────────────────────────────────────
if [[ "$COMMAND" == "logs" ]]; then
  require_docker
  info "Tailing logs (Ctrl-C to stop)..."
  $COMPOSE_CMD logs --follow "${SERVICES[@]:-}"
  exit 0
fi

# ─────────────────────────────── Stop ────────────────────────────────────────
if [[ "$COMMAND" == "stop" ]]; then
  require_docker
  step "Stopping services"
  $COMPOSE_CMD stop "${SERVICES[@]:-}"
  success "Services stopped"
  exit 0
fi

# ─────────────────────────────── Clean ───────────────────────────────────────
if [[ "$COMMAND" == "clean" ]]; then
  require_docker
  step "Removing services and volumes"
  warn "This will delete all event logs in the shared volume."
  read -r -p "Continue? [y/N] " confirm
  [[ "${confirm,,}" == "y" ]] || { info "Cancelled."; exit 0; }
  $COMPOSE_CMD down -v --remove-orphans
  success "Stack cleaned"
  exit 0
fi

# ─────────────────────────────── Prerequisites ───────────────────────────────
step "Checking prerequisites"
require_docker

DOCKER_VER=$(docker version --format '{{.Client.Version}}' 2>/dev/null || echo "unknown")
COMPOSE_VER=$($COMPOSE_CMD version --short 2>/dev/null || echo "unknown")
success "Docker $DOCKER_VER"
success "Compose $COMPOSE_VER"

# ─────────────────────────────── Prepare event log dir ───────────────────────
step "Preparing shared volumes"
mkdir -p "$EVENT_LOG_PATH"
success "Event log directory: $EVENT_LOG_PATH"
export EVENT_LOG_PATH

# ─────────────────────────────── Build ───────────────────────────────────────
step "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"

BUILD_START=$SECONDS
$COMPOSE_CMD build \
  --progress=plain \
  "${SERVICES[@]:-}"
BUILD_TIME=$((SECONDS - BUILD_START))

success "Build complete in ${BUILD_TIME}s"

# Get image size
IMAGE_SIZE=$(docker image inspect "$IMAGE_NAME:$IMAGE_TAG" \
  --format '{{.Size}}' 2>/dev/null | awk '{printf "%.1f MB", $1/1048576}' || echo "unknown")
info "Image size: $IMAGE_SIZE"

# Exit if build-only
if $BUILD_ONLY; then
  echo ""
  success "Build-only complete. Run './scripts/docker-run.sh' to start services."
  exit 0
fi

# ─────────────────────────────── Start stack ─────────────────────────────────
step "Starting services"

UP_ARGS=("${SERVICES[@]:-}")
$DETACH && UP_ARGS+=(--detach) || true

$COMPOSE_CMD up "${UP_ARGS[@]}"

# ─────────────────────────────── Post-start info ─────────────────────────────
if $DETACH; then
  echo ""
  echo -e "${GREEN}${BOLD}Services started in background.${RESET}"
  echo ""
  $COMPOSE_CMD ps
  echo ""
  echo "  Commands:"
  echo "    $COMPOSE_CMD logs -f              — tail all logs"
  echo "    $COMPOSE_CMD logs -f producer     — tail producer only"
  echo "    $COMPOSE_CMD exec producer bash   — shell into producer"
  echo "    ./scripts/docker-run.sh --stop    — stop services"
  echo "    ./scripts/docker-run.sh --clean   — remove everything"
  echo ""
  echo "  Event logs at: $EVENT_LOG_PATH/stream.jsonl"
fi
