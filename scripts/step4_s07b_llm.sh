#!/bin/bash
# Step 4 – S07b: LLM drug name cleaning + validation
#
# Phase A: Run Qwen2.5-7B-Instruct to clean raw drug names for each cohort.
# Phase B: Run S07b validation stage (accepts/rejects LLM output).
#
# Requires GPU (CUDA).  Submit with sbatch or run locally if GPU available.
#
# Usage:
#   sbatch scripts/step4_s07b_llm.sh
#   # or local:
#   bash scripts/step4_s07b_llm.sh

#SBATCH --job-name=s07b_llm_clean
#SBATCH --output=logs/step4_s07b_%j.out
#SBATCH --partition=qgpu_gtx1070ti
#SBATCH --gres=gpu:2
#SBATCH --cpus-per-task=12
#SBATCH --mem=60G
#SBATCH --time=7-00:00:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FULL_DATASET_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${FULL_DATASET_DIR}"

# ── Conda env for LLM (needs torch + transformers) ────────────────────────────
if [ -f ~/miniforge3/etc/profile.d/conda.sh ]; then
    source ~/miniforge3/etc/profile.d/conda.sh
else
    source ~/miniforge3/bin/activate
fi
conda activate drug1

# ── Paths ─────────────────────────────────────────────────────────────────────
MODEL_PATH="${MODEL_PATH:-/share/galaxy/thanyathon/models}"
DRUG_DIR="${FULL_DATASET_DIR}/data/staging/s07_split_drug"
LOG_DIR="${FULL_DATASET_DIR}/logs/$(date +"%Y%m%dT%H%M%S")_step4_s07b"
mkdir -p "${LOG_DIR}"

RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}

echo "=========================================="
echo "Step 4 – S07b LLM Drug Cleaning"
echo "Full dataset dir : ${FULL_DATASET_DIR}"
echo "Drug input dir   : ${DRUG_DIR}"
echo "Model path       : ${MODEL_PATH}"
echo "Log dir          : ${LOG_DIR}"
echo "Start            : $(date)"
echo "=========================================="
echo ""

# ── GPU check ────────────────────────────────────────────────────────────────
if command -v nvidia-smi &>/dev/null; then
    echo "GPU Info:"
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv
    echo ""
fi

export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

# ── Phase A: LLM cleaning ─────────────────────────────────────────────────────
for COHORT in pediatric adult; do
    INPUT="${DRUG_DIR}/${COHORT}_drugs_full_data.parquet"
    OUTPUT="${DRUG_DIR}/${COHORT}_drugs_llm_cleaned_full_data.parquet"

    if [ ! -f "${INPUT}" ]; then
        echo "❌ ERROR: Input not found: ${INPUT}"
        echo "   Please run Step 3 (S03–S07) first."
        exit 1
    fi

    echo "──────────────────────────────────────────"
    echo "Phase A – LLM cleaning: ${COHORT}"
    echo "  Input  : ${INPUT}"
    echo "  Output : ${OUTPUT}"
    echo "──────────────────────────────────────────"

    python "${FULL_DATASET_DIR}/scripts/llm_clean_drugs.py" \
        --input  "${INPUT}"  \
        --output "${OUTPUT}" \
        --model-path "${MODEL_PATH}" \
        --batch-size 2 \
        --device cuda \
        2>&1 | tee "${LOG_DIR}/llm_${COHORT}.log"

    echo "✅ LLM cleaning complete: ${COHORT}"
    echo ""
done

# ── Phase B: S07b validation stage ───────────────────────────────────────────
echo "──────────────────────────────────────────"
echo "Phase B – S07b validation stage"
echo "──────────────────────────────────────────"
echo ""

# Switch to pipeline conda env for the CLI
conda deactivate
if [ -f ~/miniforge3/etc/profile.d/conda.sh ]; then
    source ~/miniforge3/etc/profile.d/conda.sh
fi
conda activate can-drug-pipeline

python -m src.cli run-stage s07b_llm_clean --run-id "${RUN_ID}_s07b" \
    2>&1 | tee "${LOG_DIR}/s07b_validate.log"

echo ""
echo "=========================================="
echo "✅ Step 4 (S07b) complete — $(date)"
echo "=========================================="
echo ""
echo "Output directory: data/staging/s07b_llm_clean/"
echo ""
echo "Next: Step 5 – S08 Drug enrichment via API"
echo "  tmux new-session -s s08 \"bash scripts/step5_s08_enrich.sh\""
