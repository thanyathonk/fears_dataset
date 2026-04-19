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
# (After --skip, pass only comma-separated stage ids — same as src.cli run-all --skip.)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"
_saved_args=("$@")
set --

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

# Restore args — optional: --skip stage1,stage2,... (must not concatenate "--skip" into the
# comma-separated list or bash word-splitting turns extra stages into stray positional args.)
set -- "${_saved_args[@]}"
EXTRA_SKIP=""
if [[ "${1:-}" == "--skip" ]]; then
  shift
  EXTRA_SKIP="${1:-}"
else
  EXTRA_SKIP="${*}"
fi

SKIP_BASE="s06b_llm_clean,s07_enrich_drug_identifiers,s08_finalize_merge_and_report,s09_package_deliverables"
if [[ -n "${EXTRA_SKIP}" ]]; then
  SKIP_ALL="${SKIP_BASE},${EXTRA_SKIP}"
else
  SKIP_ALL="${SKIP_BASE}"
fi

echo "============================================================"
echo "BATCH 1: S01 → S06 (no internet needed)"
echo "Skip:  ${EXTRA_SKIP:-none}"
echo "Start: $(date)"
echo "============================================================"

python -m src.cli run-all --skip "${SKIP_ALL}"

echo "============================================================"
echo "BATCH 1 DONE: $(date)"
echo ""
echo "Next step: run S07 on frontend (needs internet):"
echo "  tmux new-session -s s07 'bash scripts/slurm_stages/run_s07_tmux.sh'"
echo "============================================================"
