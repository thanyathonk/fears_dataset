#!/bin/bash
#SBATCH --job-name=s08s09_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s08s09_%j.out
#SBATCH --error=logs/slurm_s08s09_%j.err

# Batch 3: S08-S09 (no internet required)
# Requires S07 enrichment to be complete
#
# Usage:
#   sbatch scripts/slurm_stages/run_s08_to_s09.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

set --
source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

# Verify S07 output exists
S07_DIR="data/staging/s07_enrich_drug_identifiers"
if [ ! -d "${S07_DIR}" ] || [ -z "$(ls -A ${S07_DIR} 2>/dev/null)" ]; then
    echo "ERROR: S07 output not found at ${S07_DIR}"
    echo "Run S07 first: tmux new-session -s s07 'bash scripts/slurm_stages/run_s07_tmux.sh'"
    exit 1
fi

echo "============================================================"
echo "BATCH 3: S08 → S09 (finalize + package)"
echo "Start: $(date)"
echo "============================================================"

echo "[$(date +%H:%M:%S)] Running S08 — Finalize merge..."
python -m src.cli run-stage s08_finalize_merge_and_report

echo "[$(date +%H:%M:%S)] Running S09 — Package deliverables..."
python -m src.cli run-stage s09_package_deliverables

echo "============================================================"
echo "PIPELINE COMPLETE: $(date)"
echo ""
echo "Output:"
echo "  data/output/Adult/"
echo "  data/output/Pediatric/"
echo "============================================================"
