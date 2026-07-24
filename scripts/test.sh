#!/usr/bin/env bash
# test.sh — Run the full test suite (Python + frontend)
#
# Exit codes:
#   0  — all suites passed
#   1  — one or more suites failed
#   2  — prerequisites missing
#
# Usage:
#   ./scripts/test.sh              # run everything
#   ./scripts/test.sh --python     # only Python tests
#   ./scripts/test.sh --frontend   # only frontend tests
#   ./scripts/test.sh --coverage   # Python tests with HTML coverage report
#   ./scripts/test.sh --fast       # skip slow integration tests

set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────────── Colors ──────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[test]${RESET} $*"; }
success() { echo -e "${GREEN}[test] ✔${RESET} $*"; }
warn()    { echo -e "${YELLOW}[test] ⚠${RESET} $*"; }
error()   { echo -e "${RED}[test] ✘${RESET} $*" >&2; }

step() {
  echo ""
  echo -e "${BOLD}${CYAN}══ $* ══${RESET}"
}

# ─────────────────────────────── Paths ───────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_DIR="$PROJECT_ROOT/python"
VENV_DIR="$PYTHON_DIR/.venv"
RESULTS_DIR="$PROJECT_ROOT/.test-results"

mkdir -p "$RESULTS_DIR"

# ─────────────────────────────── Argument parsing ────────────────────────────
RUN_PYTHON=true
RUN_FRONTEND=true
COVERAGE=false
FAST=false

for arg in "$@"; do
  case "$arg" in
    --python)    RUN_FRONTEND=false ;;
    --frontend)  RUN_PYTHON=false   ;;
    --coverage)  COVERAGE=true       ;;
    --fast)      FAST=true           ;;
    --help|-h)
      echo "Usage: $0 [--python|--frontend] [--coverage] [--fast]"
      echo ""
      echo "  --python      Run only Python test suite"
      echo "  --frontend    Run only frontend test suite"
      echo "  --coverage    Generate HTML coverage report (Python)"
      echo "  --fast        Skip slow / integration tests"
      exit 0
      ;;
  esac
done

# ─────────────────────────────── Timing helper ───────────────────────────────
SECONDS=0
start_time() { SECONDS=0; }
elapsed()    { echo "${SECONDS}s"; }

# ─────────────────────────────── Outcome tracking ────────────────────────────
declare -A SUITE_RESULTS   # suite_name -> "PASS" | "FAIL" | "SKIP"
OVERALL_PASS=0

record_result() {
  local suite="$1" status="$2"
  SUITE_RESULTS["$suite"]="$status"
  [[ "$status" == "FAIL" ]] && OVERALL_PASS=1
}

# ─────────────────────────────── Banner ──────────────────────────────────────
echo ""
echo -e "${BOLD}Event-Stream Playground — Test Runner${RESET}"
echo -e "Root: ${CYAN}$PROJECT_ROOT${RESET}"
echo -e "Results: ${CYAN}$RESULTS_DIR${RESET}"
echo ""

# ─────────────────────────────── Python tests ────────────────────────────────
if $RUN_PYTHON; then
  step "Python test suite"
  start_time

  # Activate venv if available, otherwise use system Python
  if [[ -f "$VENV_DIR/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
    info "Using venv: $VENV_DIR"
  else
    warn "Venv not found — using system Python. Run ./scripts/setup.sh first."
  fi

  if ! command -v python3 &>/dev/null; then
    error "python3 not found"
    record_result "python" "FAIL"
  elif ! command -v pytest &>/dev/null && ! python3 -m pytest --version &>/dev/null 2>&1; then
    error "pytest not installed — run: pip install pytest"
    record_result "python" "FAIL"
  else
    cd "$PYTHON_DIR"

    PYTEST_ARGS=(
      tests/
      -v
      --tb=short
      --no-header
      --junitxml="$RESULTS_DIR/python-results.xml"
    )

    if $COVERAGE; then
      PYTEST_ARGS+=(
        --cov=.
        --cov-report=term-missing
        "--cov-report=html:$RESULTS_DIR/htmlcov"
        "--cov-report=xml:$RESULTS_DIR/coverage.xml"
      )
    fi

    if $FAST; then
      PYTEST_ARGS+=(-m "not slow")
    fi

    info "Running: pytest ${PYTEST_ARGS[*]}"
    echo ""

    if python3 -m pytest "${PYTEST_ARGS[@]}"; then
      success "Python tests passed in $(elapsed)"
      record_result "python" "PASS"
      if $COVERAGE; then
        info "Coverage HTML: $RESULTS_DIR/htmlcov/index.html"
      fi
    else
      error "Python tests FAILED in $(elapsed)"
      record_result "python" "FAIL"
    fi
  fi
fi

# ─────────────────────────────── Frontend tests ───────────────────────────────
if $RUN_FRONTEND; then
  step "Frontend test suite"
  start_time
  cd "$PROJECT_ROOT"

  if ! command -v node &>/dev/null; then
    error "node not found — install Node.js >= 18"
    record_result "frontend" "FAIL"
  elif [[ ! -d node_modules ]]; then
    warn "node_modules not found — running npm ci"
    npm ci
    cd "$PROJECT_ROOT"
  fi

  if [[ -d node_modules ]]; then
    info "Running vitest..."
    echo ""

    if npx vitest run \
        --reporter=verbose \
        --reporter=junit \
        --outputFile.junit="$RESULTS_DIR/frontend-results.xml"; then
      success "Frontend tests passed in $(elapsed)"
      record_result "frontend" "PASS"
    else
      error "Frontend tests FAILED in $(elapsed)"
      record_result "frontend" "FAIL"
    fi
  else
    error "Could not install node_modules"
    record_result "frontend" "FAIL"
  fi
fi

# ─────────────────────────────── TypeScript check ────────────────────────────
if $RUN_FRONTEND; then
  step "TypeScript type-check"
  start_time
  cd "$PROJECT_ROOT"

  if npx tsc --noEmit; then
    success "TypeScript check passed in $(elapsed)"
    record_result "typecheck" "PASS"
  else
    error "TypeScript check FAILED"
    record_result "typecheck" "FAIL"
  fi
fi

# ─────────────────────────────── Summary ─────────────────────────────────────
echo ""
echo -e "${BOLD}══ Test Summary ══${RESET}"
echo ""
for suite in "${!SUITE_RESULTS[@]}"; do
  status="${SUITE_RESULTS[$suite]}"
  if [[ "$status" == "PASS" ]]; then
    echo -e "  ${GREEN}✔ PASS${RESET}  $suite"
  elif [[ "$status" == "FAIL" ]]; then
    echo -e "  ${RED}✘ FAIL${RESET}  $suite"
  else
    echo -e "  ${YELLOW}– SKIP${RESET}  $suite"
  fi
done
echo ""

if [[ $OVERALL_PASS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}All test suites passed.${RESET}"
else
  echo -e "${RED}${BOLD}One or more test suites failed.${RESET}"
  echo "  See results in: $RESULTS_DIR"
fi
echo ""

exit $OVERALL_PASS
