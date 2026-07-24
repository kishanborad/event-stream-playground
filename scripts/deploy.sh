#!/usr/bin/env bash
# deploy.sh — Build and deploy the frontend to GitHub Pages
#
# Performs a production build, verifies the output, then pushes dist/
# to the gh-pages branch using git subtree or gh-pages npm package.
#
# Usage:
#   ./scripts/deploy.sh                  # build + deploy
#   ./scripts/deploy.sh --dry-run        # build only, no push
#   ./scripts/deploy.sh --skip-tests     # skip pre-deploy tests
#   ./scripts/deploy.sh --branch staging # deploy to custom branch

set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────────── Colors ──────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[deploy]${RESET} $*"; }
success() { echo -e "${GREEN}[deploy] ✔${RESET} $*"; }
warn()    { echo -e "${YELLOW}[deploy] ⚠${RESET} $*"; }
error()   { echo -e "${RED}[deploy] ✘${RESET} $*" >&2; }
die()     { error "$*"; exit 1; }

step() {
  echo ""
  echo -e "${BOLD}${CYAN}══ $* ══${RESET}"
}

# ─────────────────────────────── Paths ───────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist"

# ─────────────────────────────── Argument parsing ────────────────────────────
DRY_RUN=false
SKIP_TESTS=false
DEPLOY_BRANCH="gh-pages"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)      DRY_RUN=true              ;;
    --skip-tests)   SKIP_TESTS=true           ;;
    --branch)       shift; DEPLOY_BRANCH="$1" ;;
    --help|-h)
      echo "Usage: $0 [--dry-run] [--skip-tests] [--branch <name>]"
      echo ""
      echo "  --dry-run       Build only — do not push to GitHub Pages"
      echo "  --skip-tests    Skip pre-deploy test suite"
      echo "  --branch <name> Deploy to this branch (default: gh-pages)"
      exit 0
      ;;
    *) warn "Unknown argument: $1" ;;
  esac
  shift
done

# ─────────────────────────────── Banner ──────────────────────────────────────
echo ""
echo -e "${BOLD}Event-Stream Playground — Deploy${RESET}"
echo -e "Root:     ${CYAN}$PROJECT_ROOT${RESET}"
echo -e "Branch:   ${CYAN}$DEPLOY_BRANCH${RESET}"
echo -e "Dry run:  ${CYAN}$DRY_RUN${RESET}"
echo ""

cd "$PROJECT_ROOT"

# ─────────────────────────────── Git sanity checks ───────────────────────────
step "Git sanity checks"

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  die "Not inside a git repository"
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT_SHA=$(git rev-parse --short HEAD)
info "Current branch: $CURRENT_BRANCH"
info "Commit SHA:     $COMMIT_SHA"

# Warn if deploying from a non-main branch
if [[ "$CURRENT_BRANCH" != "main" ]] && ! $DRY_RUN; then
  warn "Deploying from '$CURRENT_BRANCH' — not 'main'. Continue? [y/N]"
  read -r confirm
  [[ "${confirm,,}" == "y" ]] || die "Deploy cancelled."
fi

# Check for uncommitted changes
if ! git diff --quiet HEAD; then
  warn "There are uncommitted changes in the working tree."
  if ! $DRY_RUN; then
    warn "It's recommended to commit before deploying. Continue? [y/N]"
    read -r confirm
    [[ "${confirm,,}" == "y" ]] || die "Deploy cancelled."
  fi
fi

success "Git checks passed"

# ─────────────────────────────── Prerequisites ───────────────────────────────
step "Checking prerequisites"

command -v node &>/dev/null  || die "node not found"
command -v npm  &>/dev/null  || die "npm not found"
command -v git  &>/dev/null  || die "git not found"

NODE_VER=$(node --version)
success "Node.js $NODE_VER"

[[ -d node_modules ]] || { info "Installing dependencies..."; npm ci; }
success "Dependencies ready"

# ─────────────────────────────── Pre-deploy tests ────────────────────────────
if ! $SKIP_TESTS; then
  step "Pre-deploy tests"
  info "Running type-check..."
  npx tsc --noEmit
  success "TypeScript type-check passed"

  info "Running test suite..."
  if npx vitest run --reporter=verbose; then
    success "All tests passed"
  else
    die "Tests failed — aborting deploy. Use --skip-tests to bypass."
  fi
fi

# ─────────────────────────────── Build ───────────────────────────────────────
step "Production build"
info "Building with Vite..."

VITE_BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
export VITE_BUILD_TIME
export VITE_COMMIT_SHA="$COMMIT_SHA"
export NODE_ENV=production

npm run build

# Verify build output
if [[ ! -f "$DIST_DIR/index.html" ]]; then
  die "Build failed — dist/index.html not found"
fi

DIST_SIZE=$(du -sh "$DIST_DIR" | cut -f1)
JS_COUNT=$(find "$DIST_DIR" -name "*.js" | wc -l | tr -d ' ')
CSS_COUNT=$(find "$DIST_DIR" -name "*.css" | wc -l | tr -d ' ')

success "Build complete"
info "  Size:       $DIST_SIZE"
info "  JS files:   $JS_COUNT"
info "  CSS files:  $CSS_COUNT"

# ─────────────────────────────── Dry-run exit ────────────────────────────────
if $DRY_RUN; then
  echo ""
  echo -e "${YELLOW}Dry run complete — skipping push to GitHub Pages.${RESET}"
  echo "  Build output: $DIST_DIR"
  exit 0
fi

# ─────────────────────────────── Push to gh-pages ────────────────────────────
step "Deploying to GitHub Pages ($DEPLOY_BRANCH)"

DEPLOY_MSG="deploy: $COMMIT_SHA from $CURRENT_BRANCH — $VITE_BUILD_TIME"

# Use git subtree push (works without gh-pages npm package)
info "Pushing $DIST_DIR to branch '$DEPLOY_BRANCH'..."

# Add dist to a temporary commit if it's gitignored, then push subtree
if git ls-files --error-unmatch "$DIST_DIR" &>/dev/null 2>&1; then
  # dist is tracked — use subtree push
  git subtree push --prefix dist "origin" "$DEPLOY_BRANCH"
else
  # dist is gitignored — use a worktree approach
  WORKTREE_DIR="$(mktemp -d)"
  trap 'rm -rf "$WORKTREE_DIR"' EXIT

  git worktree add --orphan "$WORKTREE_DIR" "$DEPLOY_BRANCH" 2>/dev/null || \
    git worktree add "$WORKTREE_DIR" "$DEPLOY_BRANCH"

  cp -r "$DIST_DIR"/. "$WORKTREE_DIR/"

  # Add a .nojekyll file to disable Jekyll processing
  touch "$WORKTREE_DIR/.nojekyll"

  pushd "$WORKTREE_DIR" >/dev/null
  git add -A
  git commit -m "$DEPLOY_MSG" --allow-empty
  git push origin "$DEPLOY_BRANCH" --force
  popd >/dev/null

  git worktree remove "$WORKTREE_DIR" --force 2>/dev/null || true
fi

success "Deployed to branch: $DEPLOY_BRANCH"

# ─────────────────────────────── Post-deploy info ────────────────────────────
REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "unknown")
PAGES_URL=""

if [[ "$REMOTE_URL" =~ github\.com[:/]([^/]+)/([^.]+) ]]; then
  GH_USER="${BASH_REMATCH[1]}"
  GH_REPO="${BASH_REMATCH[2]}"
  PAGES_URL="https://${GH_USER}.github.io/${GH_REPO}/"
fi

echo ""
echo -e "${GREEN}${BOLD}Deploy complete!${RESET}"
[[ -n "$PAGES_URL" ]] && echo -e "  URL: ${CYAN}$PAGES_URL${RESET}"
echo -e "  SHA: ${CYAN}$COMMIT_SHA${RESET}"
echo ""
