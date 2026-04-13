#!/bin/bash
#SBATCH --job-name=s09_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s09_%j.out
#SBATCH --error=logs/slurm_s09_%j.err

# S09 — Final 3-way merge and reporting
# Usage: sbatch scripts/slurm_stages/run_s09.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Stage: s09_finalize_merge_and_report"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-stage s09_finalize_merge_and_report

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
