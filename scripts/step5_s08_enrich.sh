#!/bin/bash
# Step 5 – S08: Enrich drug names with RxNorm (RxCUI, ingredients)
#
# Calls RxNav API and falls back to PubChem — requires internet access.
# Long-running (hours to days depending on dataset size).
# Run in a tmux session to avoid disconnection.
#
# Usage:
#   cd /share/galaxy/thanyathon/candrug_pipeline/full_dataset
#   tmux new-session -s s08 "bash scripts/step5_s08_enrich.sh"
#   # or both cohorts in separate windows:
#   tmux new-session -s s08_ped  "bash scripts/step5_s08_enrich.sh pediatric"
#   tmux new-session -s s08_adult "bash scripts/step5_s08_enrich.sh adult"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."

COHORT="${1:-}"   # optional: "pediatric" or "adult"; empty = both

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "${CONDA_ENV}"

RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"

echo "=========================================="
echo "Step 5 – S08: Enrich Drug Identifiers (RxNorm/PubChem)"
echo "RUN_ID  : ${RUN_ID}"
echo "Cohort  : ${COHORT:-both}"
echo "Log dir : ${LOG_DIR}"
echo "Start   : $(date)"
echo "=========================================="
echo ""

# Check prerequisites
S07B_DIR="data/staging/s07b_llm_clean"
if [ ! -d "${S07B_DIR}" ] || [ -z "$(ls -A ${S07B_DIR} 2>/dev/null)" ]; then
    echo "❌ ERROR: S07b output not found: ${S07B_DIR}"
    echo "   Please run Step 4 (S07b LLM cleaning) first."
    exit 1
fi
echo "✅ S07b output found"
echo ""

# Build enrich-local arguments
ENRICH_ARGS="--run-id ${RUN_ID}_s08"
if [ -n "${COHORT}" ]; then
    case "${COHORT}" in
        pediatric|ped) ENRICH_ARGS="${ENRICH_ARGS} --cohort ped" ;;
        adult)         ENRICH_ARGS="${ENRICH_ARGS} --cohort adult" ;;
        *)
            echo "❌ ERROR: Unknown cohort '${COHORT}'. Use 'pediatric' or 'adult'."
            exit 1
            ;;
    esac
fi

# Run S08 enrichment
# shellcheck disable=SC2086
python -m src.cli enrich-local ${ENRICH_ARGS} \
    2>&1 | tee "${LOG_DIR}/s08_enrich${COHORT:+_$COHORT}.log"

echo ""
echo "=========================================="
echo "✅ Step 5 (S08) complete — $(date)"
echo "=========================================="
echo ""
echo "Output: data/staging/s08_enrich_drug_identifiers/"
echo ""
echo "Next: Step 6 – S09 + S10 (after S07b and S08 are BOTH done)"
echo "  bash scripts/run_s09_s10.sh"
