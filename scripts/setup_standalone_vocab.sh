#!/bin/bash
# Copy OMOP vocabulary จาก parent มาที่ full_dataset/data/vocab/
# ใช้เมื่อต้องการให้ full_dataset standalone (ไม่อ้าง parent)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FULL_DATASET="$(cd "${SCRIPT_DIR}/.." && pwd)"
VOCAB_SRC="${FULL_DATASET}/../data/vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC"
VOCAB_DST="${FULL_DATASET}/data/vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC"

if [[ ! -d "${VOCAB_SRC}" ]]; then
    echo "Error: Source vocabulary not found at ${VOCAB_SRC}"
    echo "Please ensure the parent pipeline has vocab, or copy manually."
    exit 1
fi

mkdir -p "${FULL_DATASET}/data/vocab"
echo "Copying vocabulary to ${VOCAB_DST}..."
cp -r "${VOCAB_SRC}" "${FULL_DATASET}/data/vocab/"
echo "Done. full_dataset/data/vocab/ is ready for standalone use."
