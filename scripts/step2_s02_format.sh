#!/bin/bash
# Step 2 – S02: Build normalised ER tables from raw OpenFDA CSV.gz files
#
# Uses Polars streaming so memory footprint stays bounded even on the full dataset.
# Runs in foreground (suitable for tmux session).
#
# Usage:
#   cd /share/galaxy/thanyathon/candrug_pipeline/full_dataset
#   tmux new-session -s s02 "bash scripts/step2_s02_format.sh"
#   # or directly:
#   bash scripts/step2_s02_format.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "${CONDA_ENV}"

RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"

echo "=========================================="
echo "Step 2 – S02: Entity Format (Polars streaming)"
echo "RUN_ID  : ${RUN_ID}"
echo "Log dir : ${LOG_DIR}"
echo "Start   : $(date)"
echo "=========================================="
echo ""

# Force Python stdout flush immediately (same as S01)
export PYTHONUNBUFFERED=1

python -u -m src.cli run-stage s02_entity_format --run-id "${RUN_ID}_s02" \
    2>&1 | tee "${LOG_DIR}/s02_format.log"

echo ""
echo "=========================================="
echo "✅ S02 complete — $(date)"
echo "=========================================="
echo ""
echo "Output ER tables: data/staging/s02_entity_format/"
echo ""
echo "Next: Run Step 3 (S03–S07)"
echo "  bash scripts/run_s03_s07.sh"
echo "  # or SLURM: sbatch scripts/slurm_run_s03_s07.sh"
