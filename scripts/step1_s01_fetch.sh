#!/bin/bash
# Step 1 – S01: Download FAERS drug event data from OpenFDA
#
# Runs in foreground (suitable for tmux session).
# S01 downloads many ZIP files in parallel — may take several hours.
#
# Parallel workers: controlled by S01_JOBS (overrides config parallel_downloads).
# Bulk files are served from AWS S3/CDN (not rate-limited by FDA's 240 req/min API limit).
# Safe range: 8–20. Increase if network bandwidth allows.
#   S01_JOBS=15 bash scripts/step1_s01_fetch.sh   # 15 concurrent downloads
#   S01_JOBS=20 bash scripts/step1_s01_fetch.sh   # 20 concurrent downloads (fast network)
#
# Usage:
#   cd /share/galaxy/thanyathon/candrug_pipeline/full_dataset
#   tmux new-session -s s01 "bash scripts/step1_s01_fetch.sh"
#   # or directly:
#   bash scripts/step1_s01_fetch.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

# Load .env file if present (for OPENFDA_API_KEY etc.)
if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source ".env"
    set +a
fi

RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"

echo "=========================================="
echo "Step 1 – S01: Fetch OpenFDA FAERS data"
echo "RUN_ID  : ${RUN_ID}"
echo "Log dir : ${LOG_DIR}"
echo "Start   : $(date)"
echo "=========================================="
echo ""

if [ -z "${OPENFDA_API_KEY:-}" ]; then
    echo "⚠️  WARNING: OPENFDA_API_KEY is not set."
    echo "   Set it in .env or: export OPENFDA_API_KEY=your_key_here"
    echo "   Without key: 240 req/min limit applies."
    echo ""
else
    echo "✅ OPENFDA_API_KEY loaded  (API limit: 240 req/min, 120,000 req/day)"
fi

# Parallel workers info
JOBS_DISPLAY="${S01_JOBS:-from config}"
echo "⚡ Parallel workers: ${JOBS_DISPLAY}"
echo "   (override: S01_JOBS=15 bash scripts/step1_s01_fetch.sh)"
echo ""

# Force Python to flush stdout immediately (prevents silent hangs in tee pipes)
export PYTHONUNBUFFERED=1

python -u -m src.cli run-stage s01_fetch_openfda --run-id "${RUN_ID}_s01" \
    2>&1 | tee "${LOG_DIR}/s01_fetch.log"

echo ""
echo "=========================================="
echo "✅ S01 complete — $(date)"
echo "=========================================="
echo ""
echo "Next: Run Step 2 (S02) to format ER tables"
echo "  bash scripts/step2_s02_format.sh"
