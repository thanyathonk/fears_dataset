#!/bin/bash
#SBATCH --job-name=s07_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm_s07_%j.out
#SBATCH --error=logs/slurm_s07_%j.err

# S07 — Enrich drugs via RxNorm/ChEMBL/KEGG
# Usage: sbatch scripts/slurm_stages/run_s07.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Stage: s07_enrich_drug_identifiers"
echo "Start: $(date)"
echo "=========================================="

python -m src.cli run-stage s07_enrich_drug_identifiers

echo "=========================================="
echo "Done:  $(date)"
echo "=========================================="
