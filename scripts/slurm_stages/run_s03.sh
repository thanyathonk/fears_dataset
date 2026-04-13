#!/bin/bash
#SBATCH --job-name=s03_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s03_%j.out
#SBATCH --error=logs/slurm_s03_%j.err

# S03 — Join tables and split Adult/Pediatric cohorts
# Usage: sbatch scripts/slurm_stages/run_s03.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Stage: s03_join_partition_age"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-stage s03_join_partition_age

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
