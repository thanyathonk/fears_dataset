#!/bin/bash
# S07 — Enrich drug identifiers via RxNav/ChEMBL/KEGG APIs
#
# ** MUST run on frontend node (needs internet) **
# ** Run inside tmux (long-running, hours) **
#
# Usage:
#   tmux new-session -s s07 'bash scripts/slurm_stages/run_s07_tmux.sh'
#
# After completion, submit batch 3:
#   sbatch scripts/slurm_stages/run_s08_to_s09.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${SCRIPT_DIR}"

source ~/miniforge3/bin/activate
conda activate "${CONDA_ENV:-fulldata}"
export PYTHONUNBUFFERED=1

RUN_ID="s07_$(date +%Y%m%dT%H%M%S)"
LOG_FILE="logs/${RUN_ID}_s07_enrich.log"
mkdir -p logs

echo "============================================================"
echo "S07 — Drug Enrichment (RxNav/ChEMBL/KEGG)"
echo "Running on frontend node (internet required)"
echo "Log: ${LOG_FILE}"
echo "Start: $(date)"
echo "============================================================"

python -m src.cli run-stage s07_enrich_drug_identifiers 2>&1 | tee "${LOG_FILE}"

echo ""
echo "============================================================"
echo "S07 DONE: $(date)"
echo ""
echo "Next step: submit batch 3 (S08-S09):"
echo "  sbatch scripts/slurm_stages/run_s08_to_s09.sh"
echo "============================================================"
