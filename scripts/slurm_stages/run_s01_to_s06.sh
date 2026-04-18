#!/bin/bash
#SBATCH --job-name=s01s06_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s01s06_%j.out
#SBATCH --error=logs/slurm_s01s06_%j.err

# Batch 1: S01-S06 (no internet required)
# Skips S01/S02 if data exists, skips S06b (LLM, optional)
#
# Usage:
#   sbatch scripts/slurm_stages/run_s01_to_s06.sh
#   sbatch scripts/slurm_stages/run_s01_to_s06.sh --skip s01_fetch_openfda,s02_entity_format

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

_saved_args=("$@")
set --
source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
set -- "${_saved_args[@]}"

export PYTHONUNBUFFERED=1
EXTRA_ARGS="${*}"

echo "============================================================"
echo "BATCH 1: S01 → S06 (no internet needed)"
echo "Skip:  ${EXTRA_ARGS:-none}"
echo "Start: $(date)"
echo "============================================================"

python -m src.cli run-all --skip s06b_llm_clean,s07_enrich_drug_identifiers,s08_finalize_merge_and_report,s09_package_deliverables${EXTRA_ARGS:+,$EXTRA_ARGS}

echo "============================================================"
echo "BATCH 1 DONE: $(date)"
echo ""
echo "Next step: run S07 on frontend (needs internet):"
echo "  tmux new-session -s s07 'bash scripts/slurm_stages/run_s07_tmux.sh'"
echo "============================================================"
