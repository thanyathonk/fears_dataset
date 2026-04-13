#!/bin/bash
#SBATCH --job-name=s05b_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s05b_%j.out
#SBATCH --error=logs/slurm_s05b_%j.err

# S05b — Map MedDRA full hierarchy (PT→HLT→HLGT→SOC)
# Usage: sbatch scripts/slurm_stages/run_s05b.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Stage: s05b_map_omop_meddra_full_hierarchy"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-stage s05b_map_omop_meddra_full_hierarchy

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
