#!/bin/bash
# Slowly enrich food bank heatmap city coordinate cache from unmatched city names.
# This is deliberately separate from the Google Trends fetch so the live update
# path never blocks on geocoding.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/enrich_city_coordinates_$(date +%Y%m%d_%H%M%S).log"
LOCK_DIR="/tmp/openclaw-foodbank-heatmap-coordinate-enrich.lock"
LIMIT="${CITY_COORD_ENRICH_LIMIT:-30}"
DELAY="${CITY_COORD_ENRICH_DELAY:-1.2}"
WORKTREE_DIR=""
LOCK_HELD=false

mkdir -p "$LOG_DIR"

log() { echo "$*" | tee -a "$LOG_FILE"; }

cleanup() {
  if [[ -n "$WORKTREE_DIR" ]]; then
    git -C "$PROJECT_DIR" worktree remove --force "$WORKTREE_DIR" >/dev/null 2>&1 || true
  fi
  if [[ "$LOCK_HELD" == "true" ]]; then
    rmdir "$LOCK_DIR" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ "${COORD_ENRICH_LOCK_HELD:-false}" != "true" ]]; then
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    log "SKIP: coordinate enrichment already running (lock: $LOCK_DIR)"
    exit 0
  fi
  LOCK_HELD=true
fi

cd "$PROJECT_DIR"

log "=== Food Bank Heatmap City Coordinate Enrichment ==="
log "Started: $(date)"
log "Project: $PROJECT_DIR"
log "Limit: $LIMIT"
log "Delay: $DELAY"

if [[ "${SKIP_GIT_PUSH:-false}" != "true" && "${COORD_ENRICH_IN_WORKTREE:-false}" != "true" ]]; then
  log "Preparing clean worktree from current origin/main..."
  git fetch origin main 2>&1 | tee -a "$LOG_FILE"
  WORKTREE_DIR="$(mktemp -d /tmp/foodbank-coordinate-enrich.XXXXXX)"
  rmdir "$WORKTREE_DIR"
  git worktree add --detach "$WORKTREE_DIR" origin/main 2>&1 | tee -a "$LOG_FILE"
  log "Running enrichment in clean worktree: $WORKTREE_DIR"
  COORD_ENRICH_IN_WORKTREE=true \
  COORD_ENRICH_LOCK_HELD=true \
  CITY_COORD_ENRICH_LIMIT="$LIMIT" \
  CITY_COORD_ENRICH_DELAY="$DELAY" \
    /bin/bash "$WORKTREE_DIR/scripts/enrich_city_coordinates_nightly.sh" 2>&1 | tee -a "$LOG_FILE"
  log "Completed: $(date)"
  log "=== Done ==="
  exit 0
fi

if [[ ! -f data/unmatched_cities.json ]]; then
  log "No unmatched_cities.json found; nothing to enrich."
  exit 0
fi

BEFORE_COUNT=$(python3 - <<'PY'
import json
from pathlib import Path
p=Path('data/unmatched_cities.json')
try:
    print(len(json.loads(p.read_text()).get('cities') or []))
except Exception:
    print('ERR')
PY
)
log "Unmatched before: $BEFORE_COUNT"

if [[ "$BEFORE_COUNT" == "0" ]]; then
  log "No unmatched cities."
  log "=== Done ==="
  exit 0
fi

python3 "$SCRIPT_DIR/enrich_city_coordinates.py" \
  --coordinates "$PROJECT_DIR/data/city_coordinates.json" \
  --unmatched "$PROJECT_DIR/data/unmatched_cities.json" \
  --limit "$LIMIT" \
  --delay "$DELAY" \
  2>&1 | tee -a "$LOG_FILE"

python3 -m json.tool "$PROJECT_DIR/data/city_coordinates.json" >/dev/null
python3 -m json.tool "$PROJECT_DIR/data/unmatched_cities.json" >/dev/null

AFTER_COUNT=$(python3 - <<'PY'
import json
from pathlib import Path
p=Path('data/unmatched_cities.json')
print(len(json.loads(p.read_text()).get('cities') or []))
PY
)
COORD_COUNT=$(python3 - <<'PY'
import json
from pathlib import Path
p=Path('data/city_coordinates.json')
print(len(json.loads(p.read_text()).get('cities') or {}))
PY
)
log "Unmatched after: $AFTER_COUNT"
log "Coordinate count: $COORD_COUNT"

if [[ "${SKIP_GIT_PUSH:-false}" != "true" ]]; then
  if git diff --quiet -- data/city_coordinates.json data/unmatched_cities.json 2>/dev/null; then
    log "No coordinate changes detected."
  else
    log "Coordinate files changed — committing and pushing..."
    git add data/city_coordinates.json data/unmatched_cities.json scripts/enrich_city_coordinates.py scripts/enrich_city_coordinates_nightly.sh
    git commit -m "Enrich food bank heatmap city coordinates $(date -u +'%Y-%m-%d %H:%M UTC')" 2>&1 | tee -a "$LOG_FILE"
    if ! git push origin HEAD:main 2>&1 | tee -a "$LOG_FILE"; then
      log "Push rejected; rebasing on latest origin/main and retrying..."
      git fetch origin main 2>&1 | tee -a "$LOG_FILE"
      git rebase origin/main 2>&1 | tee -a "$LOG_FILE"
      git push origin HEAD:main 2>&1 | tee -a "$LOG_FILE"
    fi
    log "Pushed coordinate enrichment changes."
  fi
else
  log "Skipping git commit/push (SKIP_GIT_PUSH=true)."
fi

log "Completed: $(date)"
log "=== Done ==="
