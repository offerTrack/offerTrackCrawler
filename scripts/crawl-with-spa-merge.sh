#!/usr/bin/env bash
# Crawl → out/jobs.json, then pipe SPA/http_json stdout into merge and overwrite jobs.json (no second JSON in out/).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/rust"
CONFIG_PATH="$ROOT/config/crawl_sites.json"
DEFAULT_OUT="$ROOT/out/jobs.json"
DEFAULT_DB="$ROOT/state/jobs.db"

# Parse args so crawl and merge use the exact same output path.
HAS_CONFIG=0
HAS_OUT=0
HAS_DB=0
OUT_PATH="$DEFAULT_OUT"
ARGS=("$@")
for ((i=0; i<${#ARGS[@]}; i++)); do
  a="${ARGS[$i]}"
  case "$a" in
    --config)
      HAS_CONFIG=1
      i=$((i+1))
      ;;
    --out)
      HAS_OUT=1
      if (( i + 1 < ${#ARGS[@]} )); then
        OUT_PATH="${ARGS[$((i+1))]}"
      fi
      i=$((i+1))
      ;;
    --db)
      HAS_DB=1
      i=$((i+1))
      ;;
  esac
done

# Resolve relative --out against current cwd (ROOT/rust), same as clap behavior.
if [[ "$OUT_PATH" != /* ]]; then
  OUT_PATH="$ROOT/rust/$OUT_PATH"
fi

CRAWL_ARGS=("$@")
if [[ $HAS_CONFIG -eq 0 ]]; then
  CRAWL_ARGS+=(--config "$CONFIG_PATH")
fi
if [[ $HAS_OUT -eq 0 ]]; then
  CRAWL_ARGS+=(--out "$DEFAULT_OUT")
  OUT_PATH="$DEFAULT_OUT"
fi
if [[ $HAS_DB -eq 0 ]]; then
  CRAWL_ARGS+=(--db "$DEFAULT_DB")
fi

cargo run --release -p offertrack-crawler --bin offertrack-crawl -- "${CRAWL_ARGS[@]}"
node "$ROOT/scripts/spa-careers/crawl.mjs" | cargo run --release -p offertrack-crawler --bin offertrack-merge-jobs -- \
  --base "$OUT_PATH" --extra - --in-place
