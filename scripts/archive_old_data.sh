#!/bin/bash
# Archive existing staging and output data to a timestamped folder.
# Run this before re-running the pipeline to preserve old results.
#
# Usage:
#   cd /share/galaxy/thanyathon/candrug_pipeline/full_dataset
#   bash scripts/archive_old_data.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FULL_DATASET_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${FULL_DATASET_DIR}"

TIMESTAMP=$(date +"%Y%m%dT%H%M%S")
ARCHIVE_DIR="data/archive_${TIMESTAMP}"
mkdir -p "${ARCHIVE_DIR}"

echo "=========================================="
echo "Archiving old pipeline results"
echo "Archive target: ${FULL_DATASET_DIR}/${ARCHIVE_DIR}"
echo "Timestamp: ${TIMESTAMP}"
echo "=========================================="
echo ""

archived_any=0

# ── staging ──────────────────────────────────────────────────────────────────
if [ -d "data/staging" ] && [ -n "$(ls -A data/staging 2>/dev/null)" ]; then
    mv data/staging "${ARCHIVE_DIR}/staging"
    mkdir -p data/staging
    echo "✅ data/staging  →  ${ARCHIVE_DIR}/staging"
    archived_any=1
else
    echo "⚪ data/staging is empty or missing — skipped"
fi

# ── output ────────────────────────────────────────────────────────────────────
if [ -d "data/output" ] && [ -n "$(ls -A data/output 2>/dev/null)" ]; then
    mv data/output "${ARCHIVE_DIR}/output"
    mkdir -p data/output
    echo "✅ data/output   →  ${ARCHIVE_DIR}/output"
    archived_any=1
else
    echo "⚪ data/output is empty or missing — skipped"
fi

# ── logs ─────────────────────────────────────────────────────────────────────
if [ -d "logs" ] && [ -n "$(ls -A logs 2>/dev/null)" ]; then
    mv logs "${ARCHIVE_DIR}/logs"
    mkdir -p logs
    echo "✅ logs          →  ${ARCHIVE_DIR}/logs"
    archived_any=1
else
    echo "⚪ logs is empty or missing — skipped"
fi

echo ""
if [ "${archived_any}" -eq 1 ]; then
    echo "Archive complete: ${FULL_DATASET_DIR}/${ARCHIVE_DIR}"
    echo ""
    du -sh "${ARCHIVE_DIR}" 2>/dev/null || true
else
    echo "Nothing to archive."
    rmdir "${ARCHIVE_DIR}" 2>/dev/null || true
fi
echo ""
echo "Ready to run fresh pipeline."
