#!/bin/bash
# Step 4 – S07b: in-process LLM drug decomposition + merge (single stage).
#
# Runs ``python -m src.cli run-stage s07b_llm_clean`` — loads Qwen inside
# ``src/stages/s07b_llm_clean.py``, reads ``s07_split_drug/*_drugs_full_data.parquet``,
# writes ``s07b_llm_clean/*_drugs_clean_full_data.parquet``.
#
# Requires GPU (CUDA), torch, transformers. Submit with sbatch or run locally.
#
# Usage:
#   sbatch scripts/step4_s07b_llm.sh
#   bash scripts/step4_s07b_llm.sh

#SBATCH --job-name=s07b_llm_clean
#SBATCH --output=logs/step4_s07b_%j.out
#SBATCH --partition=gpu-cluster
#SBATCH --gres=gpu:2
#SBATCH --cpus-per-task=12
#SBATCH --mem=60G
#SBATCH --time=7-00:00:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FULL_DATASET_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${FULL_DATASET_DIR}"

# ── Conda env (torch + transformers) ───────────────────────────────────────────
if [ -f ~/miniforge3/etc/profile.d/conda.sh ]; then
    source ~/miniforge3/etc/profile.d/conda.sh
else
    source ~/miniforge3/bin/activate
fi
conda activate fulldata

MODEL_PATH="${MODEL_PATH:-/share/galaxy/thanyathon/models}"
export LLM_MODEL_NAME="${LLM_MODEL_NAME:-Qwen/Qwen2.5-32B-Instruct}"
export HF_CACHE="${HF_CACHE:-${LLM_CACHE_DIR:-${MODEL_PATH}}}"
export LLM_BATCH_SIZE="${LLM_BATCH_SIZE:-64}"

LOG_DIR="${FULL_DATASET_DIR}/logs/$(date +"%Y%m%dT%H%M%S")_step4_s07b"
mkdir -p "${LOG_DIR}"

RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}

echo "=========================================="
echo "Step 4 – S07b (LLM + merge, one stage)"
echo "Full dataset dir : ${FULL_DATASET_DIR}"
echo "LLM_MODEL_NAME   : ${LLM_MODEL_NAME}"
echo "HF_CACHE         : ${HF_CACHE}"
echo "LLM_BATCH_SIZE   : ${LLM_BATCH_SIZE}"
echo "Log dir          : ${LOG_DIR}"
echo "Start            : $(date)"
echo "=========================================="
echo ""

if command -v nvidia-smi &>/dev/null; then
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv
    echo ""
fi

export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

python -m src.cli run-stage s07b_llm_clean --run-id "${RUN_ID}_s07b" \
    2>&1 | tee "${LOG_DIR}/s07b_llm.log"

echo ""
echo "✅ Step 4 complete — $(date)"
echo "Output: data/staging/s07b_llm_clean/"
