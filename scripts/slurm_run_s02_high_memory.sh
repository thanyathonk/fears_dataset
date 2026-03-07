#!/bin/bash

#SBATCH --job-name=s02-hm
#SBATCH --partition=qgpu_gtx1070ti,qgpu_rtx2070
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --gres=gpu:0
#SBATCH --mem=120G
#SBATCH --time=4-00:00:00
#SBATCH --output=slurm-s02-hm-%j.out
#SBATCH --error=slurm-s02-hm-%j.err

# Usage: sbatch scripts/slurm_run_s02_high_memory.sh
# Must be submitted from inside full_dataset/:
#   cd /path/to/full_dataset
#   sbatch scripts/slurm_run_s02_high_memory.sh
#
# High-memory mode: in-memory processing, no disk shards. Simpler and faster.
# Requires ~80–120GB RAM. Set S02_OPENFDA_HIGH_MEMORY=1.
# Use on machines with sufficient RAM (e.g. 128GB+).

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1
export S02_OPENFDA_HIGH_MEMORY=1

echo "=========================================="
echo "S02 – Entity format (HIGH-MEMORY mode)"
echo "Memory allocated: 120G"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date)"
echo "=========================================="

python -u -m src.cli run-stage s02_entity_format

echo ""
echo "=========================================="
echo "✅ S02 completed"
echo "End: $(date)"
echo "=========================================="
