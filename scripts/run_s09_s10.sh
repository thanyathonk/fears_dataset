#!/bin/bash
# Step 6 – S09 + S10: Finalize merge/report and package deliverables
#
# Prerequisite: Step 4 (S07b) and Step 5 (S08) must be complete.
#
# Usage:
#   cd /share/galaxy/thanyathon/candrug_pipeline/full_dataset
#   bash scripts/run_s09_s10.sh
#   # or SLURM: sbatch scripts/run_s09_s10.sh

#SBATCH --job-name=s09s10_FD
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=300G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/s09s10_FD_%j.out
#SBATCH --error=logs/s09s10_FD_%j.err

PROJECT_ROOT="/ist/users/thanyathonk/thanyathonk_bak/fears_dataset"
cd "${PROJECT_ROOT}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

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

# Check prerequisites
check_prerequisite() {
    local stage=$1
    local path=$2
    
    if [[ ! -d "$path" ]] && [[ ! -f "$path" ]]; then
        echo "❌ ERROR: Prerequisite ${stage} output not found: ${path}"
        echo "   Please run ${stage} first"
        exit 1
    fi
}

echo "=========================================="
echo "Step 6 – S09 + S10 pipeline (full_dataset)"
echo "RUN_ID_ROOT: ${RUN_ID_ROOT}"
echo "Log directory: ${LOG_ROOT}"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
check_prerequisite "S07b (LLM clean)"   "data/staging/s07b_llm_clean"
check_prerequisite "S08 (Drug enrich)" "data/staging/s08_enrich_drug_identifiers"
echo "✅ All prerequisites found"
echo ""

# S09: Finalize merge and report
run_cmd "S09 – Finalize merge and report" \
    python -m src.cli finalize --run-id "${RUN_ID_ROOT}_s09"

# S10: Package deliverables
run_cmd "S10 – Package deliverables" \
    python -m src.cli package --run-id "${RUN_ID_ROOT}_s10"

echo "=========================================="
echo "✅ All stages S09-S10 completed successfully"
echo "End time: $(date)"
echo "=========================================="
echo ""
echo "📦 Output files are in:"
echo "   - data/output/Pediatric/"
echo "   - data/output/Adult/"
echo ""

