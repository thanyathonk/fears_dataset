#!/bin/bash

#SBATCH --job-name=s03-s07
#SBATCH --partition=qgpu_gtx1070ti
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --gres=gpu:0
#SBATCH --mem=60G
#SBATCH --time=4-00:00:00
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err

# Usage: sbatch scripts/slurm_run_s03_s07.sh
# Must be submitted from inside full_dataset/:
#   cd /path/to/full_dataset
#   sbatch scripts/slurm_run_s03_s07.sh

set -euo pipefail

# SLURM_SUBMIT_DIR = directory where sbatch was called
cd "${SLURM_SUBMIT_DIR}"

# Activate conda environment
source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

RUN_ID_ROOT=${RUN_ID_ROOT:-$(date +"%Y%m%dT%H%M%S")}
LOG_ROOT="logs/${RUN_ID_ROOT}"
mkdir -p "${LOG_ROOT}"

info() {
    echo "=========================================="
    echo "$1"
    echo "time: $(date)"
    echo "=========================================="
}

run_cmd() {
    local description=$1
    shift

    info "start: ${description}"
    "$@" 2>&1 | tee -a "${LOG_ROOT}/${description// /_}.log"
    local exit_code=$?
    if [[ ${exit_code} -ne 0 ]]; then
        echo "❌ ${description} failed (exit ${exit_code})"
        exit ${exit_code}
    fi
    echo "✅ ${description} completed"
    echo ""
}

echo "=========================================="
echo "Step 3 – S03, S05, S06, S06b, S07 (full_dataset)"
echo "RUN_ID_ROOT: ${RUN_ID_ROOT}"
echo "Log directory: ${LOG_ROOT}"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Memory allocated: 60G"
echo "=========================================="
echo ""

# S03: Join tables with early quality filtering
run_cmd "S03 – Join tables + Early quality filters" \
    python -u -m src.cli run-stage s03_join_partition_age --run-id "${RUN_ID_ROOT}_s03"

# S05: Split ADR reports
run_cmd "S05 – Split ADR reports" \
    python -u -m src.cli run-stage s05_split_adr --run-id "${RUN_ID_ROOT}_s05"

# S06: MedDRA mapping (PT→SOC via CONCEPT_ANCESTOR)
run_cmd "S06 – MedDRA mapping" \
    python -u -m src.cli run-stage s06_map_omop_meddra --run-id "${RUN_ID_ROOT}_s06"

# S06b: MedDRA full hierarchy (PT→HLT→HLGT→SOC via CONCEPT_RELATIONSHIP)
run_cmd "S06b – MedDRA full hierarchy" \
    python -u -m src.cli run-stage s06b_map_omop_meddra_full_hierarchy --run-id "${RUN_ID_ROOT}_s06b"

# S07: Split drug names for LLM cleaning
run_cmd "S07 – Split drug names" \
    python -u -m src.cli run-stage s07_split_drug --run-id "${RUN_ID_ROOT}_s07"

echo "=========================================="
echo "✅ All stages S03, S05, S06, S06b, S07 completed successfully"
echo "End time: $(date)"
echo "=========================================="
echo ""
echo "📋 Next steps:"
echo ""
echo "1️⃣  Step 4 – S07b (LLM drug cleaning – GPU required):"
echo "   sbatch scripts/step4_s07b_llm.sh"
echo ""
echo "2️⃣  Step 5 – S08 (Drug enrichment via API – tmux recommended):"
echo "   tmux new-session -s s08 \"bash scripts/step5_s08_enrich.sh\""
echo ""
echo "3️⃣  Step 6 – S09+S10 (after S07b and S08 complete):"
echo "   bash scripts/run_s09_s10.sh"
echo ""
