#!/bin/bash
#SBATCH --job-name=pipeline_FD
#SBATCH --partition=deadline
#SBATCH --account=scads
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:0
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_pipeline_%j.out
#SBATCH --error=logs/slurm_pipeline_%j.err

# Run full pipeline S01-S10 (use --skip to skip stages)
#
# Usage:
#   sbatch scripts/slurm_stages/run_all.sh
#   sbatch scripts/slurm_stages/run_all.sh --skip s01_fetch_openfda,s02_entity_format
#   sbatch scripts/slurm_stages/run_all.sh --skip s01_fetch_openfda,s02_entity_format,s06b_llm_clean

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

# Save positional args before conda activation —
# conda's activate script reads $@ and will error on unexpected args like --skip
_saved_args=("$@")
set --

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

# Restore args
set -- "${_saved_args[@]}"
EXTRA_ARGS="${*}"

echo "=========================================="
echo "FULL PIPELINE RUN"
echo "Args:  ${EXTRA_ARGS:-none}"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-all ${EXTRA_ARGS}

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
