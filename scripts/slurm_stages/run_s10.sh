#!/bin/bash
#SBATCH --job-name=s10_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s10_%j.out
#SBATCH --error=logs/slurm_s10_%j.err

# S10 — Package output dimension tables
# Usage: sbatch scripts/slurm_stages/run_s10.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Stage: s10_package_deliverables"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-stage s10_package_deliverables

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
