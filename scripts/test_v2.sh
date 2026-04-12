#!/bin/bash

#SBATCH --job-name=test
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=250G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm-test-%j.out
#SBATCH --error=logs/slurm-test-%j.err

# Usage: sbatch scripts/test.sh
# Must be submitted from project root:
#   cd /ist/ist-share/scads/thanyathonk/fears_dataset
#   sbatch scripts/test.sh

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Test – Create test sample"
echo "Memory allocated: 250G"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date)"
echo "==========================================="

python scripts/create_test_sample.py \
    --source-dir data/staging/s02_entity_format \
    --vocab-dir data/vocab \
    --n-reports 1000 \
    --copy-vocab

echo ""
echo "=========================================="
echo "✅ test completed"
echo "End: $(date)"
echo "=========================================="
