#!/usr/bin/env bash
# setup.sh — Development environment setup for event-stream-playground
#
# Installs and verifies all prerequisites (Node.js, Python, Docker),
# then bootstraps both the frontend and Python environments.
#
# Usage:
#   ./scripts/setup.sh           # full setup
#   ./scripts/setup.sh --python  # Python env only
#   ./scripts/setup.sh --node    # Node env only

set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────────── Colors ──────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ─────────────────────────────── Helpers ─────────────────────────────────────
info()    { echo -e "${CYAN}[setup]${RESET} $*"; }
success() { echo -e "${GREEN}[setup] ✔${RESET} $*"; }
warn()    { echo -e "${YELLOW}[setup] ⚠${RESET} $*"; }
error()   { echo -e "${RED}[setup] ✘${RESET} $*" >&2; }
die()     { error "$*"; exit 1; }

step() {
  echo ""
  echo -e "${BOLD}${CYAN}══ $* ══${RESET}"
}

require_cmd() {
  local cmd="$1"
  local install_hint="${2:-}"
  if ! command -v "$cmd" &>/dev/null; then
    error "Required command not found: $cmd"
    [[ -n "$install_hint" ]] && error "  Install: $install_hint"
    return 1
  fi
  success "$cmd found: $(command -v "$cmd")"
}

version_gte() {
  # Returns 0 if $1 >= $2 (semver comparison)
  local IFS=.
  local i ver1=($1) ver2=($2)
  for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do ver1[i]=0; done
  for ((i=0; i<${#ver1[@]}; i++)); do
    [[ -z "${ver2[i]+x}" ]] && ver2[i]=0
    ((10#${ver1[i]} > 10#${ver2[i]})) && return 0
    ((10#${ver1[i]} < 10#${ver2[i]})) && return 1
  done
  return 0
}

# ─────────────────────────────── Constants ───────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_DIR="$PROJECT_ROOT/python"
MIN_NODE="18.0.0"
MIN_PYTHON="3.9.0"
VENV_DIR="$PYTHON_DIR/.venv"

# ─────────────────────────────── Argument parsing ────────────────────────────
SETUP_NODE=true
SETUP_PYTHON=true

for arg in "$@"; do
  case "$arg" in
    --node)   SETUP_PYTHON=false ;;
    --python) SETUP_NODE=false   ;;
    --help|-h)
      echo "Usage: $0 [--node|--python]"
      echo "  --node    Only set up the Node.js / frontend environment"
      echo "  --python  Only set up the Python environment"
      exit 0
      ;;
  esac
done

# ─────────────────────────────── Banner ──────────────────────────────────────
echo ""
echo -e "${BOLD}Event-Stream Playground — Development Setup${RESET}"
echo -e "Project root: ${CYAN}$PROJECT_ROOT${RESET}"
echo ""

# ─────────────────────────────── Prerequisites check ─────────────────────────
step "Checking prerequisites"

MISSING=0

if $SETUP_NODE; then
  if ! require_cmd node "https://nodejs.org"; then
    MISSING=$((MISSING + 1))
  else
    NODE_VER=$(node --version | tr -d 'v')
    if ! version_gte "$NODE_VER" "$MIN_NODE"; then
      error "Node.js $NODE_VER is too old — need >= $MIN_NODE"
      MISSING=$((MISSING + 1))
    else
      info "Node.js $NODE_VER OK"
    fi
  fi
  require_cmd npm "comes with Node.js" || MISSING=$((MISSING + 1))
fi

if $SETUP_PYTHON; then
  if ! require_cmd python3 "https://python.org (>=3.9)"; then
    MISSING=$((MISSING + 1))
  else
    PY_VER=$(python3 --version | awk '{print $2}')
    if ! version_gte "$PY_VER" "$MIN_PYTHON"; then
      error "Python $PY_VER is too old — need >= $MIN_PYTHON"
      MISSING=$((MISSING + 1))
    else
      info "Python $PY_VER OK"
    fi
  fi
fi

if [[ $MISSING -gt 0 ]]; then
  die "$MISSING prerequisite(s) missing — please install them and re-run."
fi

success "All prerequisites met"

# ─────────────────────────────── Node / frontend ─────────────────────────────
if $SETUP_NODE; then
  step "Installing Node.js dependencies"
  cd "$PROJECT_ROOT"

  if [[ -f package-lock.json ]]; then
    info "Running npm ci (locked)"
    npm ci
  else
    info "Running npm install"
    npm install
  fi

  success "Node dependencies installed"

  step "Frontend typecheck"
  npx tsc --noEmit
  success "TypeScript type-check passed"
fi

# ─────────────────────────────── Python ──────────────────────────────────────
if $SETUP_PYTHON; then
  step "Setting up Python virtual environment"
  cd "$PYTHON_DIR"

  if [[ ! -d "$VENV_DIR" ]]; then
    info "Creating venv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  else
    info "Venv already exists at $VENV_DIR"
  fi

  # Activate venv
  # shellcheck source=/dev/null
  source "$VENV_DIR/bin/activate"
  success "Venv activated"

  step "Installing Python dependencies"
  pip install --upgrade pip --quiet
  pip install -r requirements.txt --quiet
  success "Python dependencies installed"

  # Verify imports work
  step "Verifying Python module imports"
  python3 - <<'EOF'
import importlib, sys
modules = ["event_producer", "event_consumer", "stream_analyzer", "kafka_simulator", "load_generator"]
for mod in modules:
    try:
        importlib.import_module(mod)
        print(f"  ✔ {mod}")
    except ImportError as e:
        print(f"  ✘ {mod}: {e}")
        sys.exit(1)
print("All modules importable.")
EOF
  success "Python module imports verified"
fi

# ─────────────────────────────── Optional: Docker check ──────────────────────
step "Optional: Docker availability"
if command -v docker &>/dev/null; then
  DOCKER_VER=$(docker --version | awk '{print $3}' | tr -d ',')
  success "Docker $DOCKER_VER found"
  if command -v docker-compose &>/dev/null || docker compose version &>/dev/null 2>&1; then
    success "Docker Compose available"
  else
    warn "Docker Compose not found — docker-run.sh won't work"
  fi
else
  warn "Docker not found — Docker features unavailable (optional)"
fi

# ─────────────────────────────── Summary ─────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}Setup complete!${RESET}"
echo ""
echo "  Quick start:"
if $SETUP_NODE; then
  echo "    npm run dev           — start Vite dev server"
fi
if $SETUP_PYTHON; then
  ACTIVATE_CMD="source $VENV_DIR/bin/activate"
  echo "    $ACTIVATE_CMD"
  echo "    python python/event_producer.py --count 100"
fi
echo "    ./scripts/demo.sh     — launch full local demo"
echo "    ./scripts/test.sh     — run all tests"
echo ""
