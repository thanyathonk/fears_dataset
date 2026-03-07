#!/bin/bash

#SBATCH --job-name=s02
#SBATCH --partition=qgpu_gtx1070ti,qgpu_rtx2070
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --gres=gpu:0
#SBATCH --mem=60G
#SBATCH --time=4-00:00:00
#SBATCH --output=slurm-s02-%j.out
#SBATCH --error=slurm-s02-%j.err

# Usage: sbatch scripts/slurm_run_s02.sh
# Must be submitted from inside full_dataset/:
#   cd /path/to/full_dataset
#   sbatch scripts/slurm_run_s02.sh
#
# S02 Pass 2 (aggregate 1704 shards → pivot) needs high memory (~30–50GB).
# Partition qgpu_gtx1070ti offers 60GB — max available on this cluster.

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "S02 – Entity format (drug_openfda_wide, drug_mapping_input)"
echo "Memory allocated: 60G"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date)"
echo "=========================================="

python -u -m src.cli run-stage s02_entity_format

echo ""
echo "=========================================="
echo "✅ S02 completed"
echo "End: $(date)"
echo "=========================================="
