#!/bin/bash

#SBATCH --job-name=s02-hm
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=40
#SBATCH --gres=gpu:0
#SBATCH --mem=600G
#SBATCH --time=4-00:00:00
#SBATCH --output=logs/slurm-s02-hm-%j.out
#SBATCH --error=logs/slurm-s02-hm-%j.err

# Usage: sbatch scripts/slurm_run_s02_high_memory.sh
# Must be submitted from project root:
#   cd /path/to/fears_dataset
#   sbatch scripts/slurm_run_s02_high_memory.sh
#
# High-memory mode: in-memory processing, no disk shards. Simpler and faster.
# Partition: cpu (ist-compute-1-001~004: 80 CPUs, ~724GB RAM/node). Requests 250G.

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1
export S02_OPENFDA_HIGH_MEMORY=1
export S02_OPENFDA_JOBS=40
export S02_MERGE_PARALLEL_BATCHES=2

echo "=========================================="
echo "S02 – Entity format (HIGH-MEMORY mode)"
echo "Memory allocated: 600G | Workers: 40"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date)"
echo "=========================================="

python -u -m src.cli run-stage s02_entity_format

echo ""
echo "=========================================="
echo "✅ S02 completed"
echo "End: $(date)"
echo "=========================================="
