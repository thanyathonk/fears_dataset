#!/bin/bash
# Run S02 validation only — no table rebuild.
# Validates drugcharacteristics_extended + drug_mapping_input and exports
# drug_mapping_input_quality_report.json to the S02 staging directory.
#
# Usage (interactive node or local):
#   bash scripts/run_s02_validate.sh
#
# Usage (SLURM — light job, 8 GB is enough):
#   sbatch scripts/run_s02_validate.sh

#SBATCH --job-name=s02-validate
#SBATCH --partition=scads
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=0-01:00:00
#SBATCH --output=logs/slurm-s02-validate-%j.out
#SBATCH --error=logs/slurm-s02-validate-%j.err

set -euo pipefail

cd "${SLURM_SUBMIT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "S02 — Validation only (no table rebuild)"
echo "Start: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="

python -u -m src.stages.s02_entity_format_stream --validate-only

echo ""
echo "=========================================="
echo "✅ S02 validation done"
echo "End: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
