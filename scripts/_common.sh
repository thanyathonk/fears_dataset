#!/bin/bash
# _common.sh — Shared prologue for all pipeline shell scripts.
#
# Source this at the top of every script:
#   source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
#
# Provides:
#   - set -euo pipefail (strict error handling)
#   - SCRIPT_DIR, PROJECT_ROOT (portable path resolution)
#   - Conda environment activation (CONDA_ENV, default: fulldata)
#   - .env loading (if present)
#   - RUN_ID and LOG_DIR initialisation

set -euo pipefail

# ── Portable path resolution ─────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
cd "${PROJECT_ROOT}"

# ── Conda activation ────────────────────────────────────────────────────────
if [ -f ~/miniforge3/etc/profile.d/conda.sh ]; then
    source ~/miniforge3/etc/profile.d/conda.sh
else
    source ~/miniforge3/bin/activate
fi
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

# ── Load .env file if present (for API keys etc.) ───────────────────────────
if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source ".env"
    set +a
fi

# ── Run ID and log directory ────────────────────────────────────────────────
RUN_ID=${RUN_ID:-$(date +"%Y%m%dT%H%M%S")}
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"
