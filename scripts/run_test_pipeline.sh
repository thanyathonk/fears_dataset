#!/bin/bash
# Run the pipeline on test sample data for validation after refactoring.
#
# Prerequisites:
#   1. python scripts/create_test_sample.py --copy-vocab
#   2. Sample data at data/test_sample/s02_entity_format/
#
# Usage:
#   bash scripts/run_test_pipeline.sh              # Run all testable stages
#   bash scripts/run_test_pipeline.sh s03 s05      # Run specific stages only
#
# Output:
#   data/test_sample/           — S02 sample + S03+ staging (see config.test.yaml)
#   data/test_output/           — final outputs
#   logs/test_*/                — logs + manifests
#SBATCH --job-name=test_pipeline
#SBATCH --partition=gpu-cluster
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=250G
#SBATCH --time=1-00:00:00
#SBATCH --output=logs/slurm-test_pipeline-%j.out
#SBATCH --error=logs/slurm-test_pipeline-%j.err

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${SLURM_SUBMIT_DIR:-$REPO_ROOT}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

# Run identity and logging (required: script uses `set -u`)
RUN_ID="${RUN_ID:-test_$(date +"%Y%m%dT%H%M%S")}"
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"

# Python reads config via pydantic env PIPELINE_CONFIG_PATH (see src/settings.EnvSettings).
# Also accept legacy PIPELINE_CONFIG as in configs/config.test.yaml comments.
export PIPELINE_CONFIG_PATH="${PIPELINE_CONFIG_PATH:-${PIPELINE_CONFIG:-configs/config.test.yaml}}"

# ── Verify sample data exists ───────────────────────────────────────────────
SAMPLE_DIR="data/test_sample/s02_entity_format"
if [ ! -d "${SAMPLE_DIR}" ] || [ -z "$(ls -A ${SAMPLE_DIR} 2>/dev/null)" ]; then
    echo "ERROR: Sample data not found at ${SAMPLE_DIR}"
    echo "Run first: python scripts/create_test_sample.py --copy-vocab"
    exit 1
fi

# ── Stage list ──────────────────────────────────────────────────────────────
# Default: all testable stages (S03-S10 except S07b)
ALL_STAGES=(
    s03_join_partition_age
    s05_split_adr
    s06_map_omop_meddra
    s06b_map_omop_meddra_full_hierarchy
    s07_split_drug
    # s07b_llm_clean — skipped (requires GPU)
    s08_enrich_drug_identifiers
    s09_finalize_merge_and_report
    s10_package_deliverables
)

# Allow partial stage selection via command line args
if [ $# -gt 0 ]; then
    STAGES=()
    for arg in "$@"; do
        # Match short names like "s03" to full stage names
        for full in "${ALL_STAGES[@]}"; do
            if [[ "${full}" == "${arg}"* ]]; then
                STAGES+=("${full}")
                break
            fi
        done
    done
else
    STAGES=("${ALL_STAGES[@]}")
fi

echo "============================================================"
echo "TEST PIPELINE RUN"
echo "============================================================"
echo "Config:    ${PIPELINE_CONFIG_PATH}"
echo "Run ID:    ${RUN_ID}"
echo "Log dir:   ${LOG_DIR}"
echo "Stages:    ${STAGES[*]}"
echo "Sample:    ${SAMPLE_DIR}"
echo "Start:     $(date)"
echo "============================================================"
echo ""

# ── Helper: run a stage and capture exit code ───────────────────────────────
run_stage() {
    local stage="$1"
    local log_file="${LOG_DIR}/${stage}.log"

    echo -n "[$(date +%H:%M:%S)] Running ${stage}... "

    if python -m src.cli run-stage "${stage}" --run-id "${RUN_ID}" \
        > "${log_file}" 2>&1; then
        echo "PASS ($(wc -l < "${log_file}") log lines)"
        return 0
    else
        local exit_code=$?
        echo "FAIL (exit ${exit_code}) — see ${log_file}"
        # Print last 10 lines for quick debugging
        echo "  --- Last 10 lines ---"
        tail -10 "${log_file}" | sed 's/^/  | /'
        echo "  ---------------------"
        return ${exit_code}
    fi
}

# ── Run stages ──────────────────────────────────────────────────────────────
PASSED=0
FAILED=0
RESULTS=()

for stage in "${STAGES[@]}"; do
    if run_stage "${stage}"; then
        RESULTS+=("PASS  ${stage}")
        # Not ((PASSED++)): with set -e, post-increment from 0 exits status 1 and kills the script.
        PASSED=$((PASSED + 1))
    else
        RESULTS+=("FAIL  ${stage}")
        FAILED=$((FAILED + 1))
        # Continue running remaining stages even if one fails
    fi
done

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "TEST RESULTS"
echo "============================================================"
for result in "${RESULTS[@]}"; do
    echo "  ${result}"
done
echo ""
echo "Passed: ${PASSED} / $((PASSED + FAILED))"
echo "Failed: ${FAILED}"
echo "End:    $(date)"
echo "Logs:   ${LOG_DIR}/"
echo "============================================================"

# Write summary to file for easy retrieval
SUMMARY_FILE="${LOG_DIR}/test_summary.txt"
{
    echo "Test Run: ${RUN_ID}"
    echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "Config: ${PIPELINE_CONFIG_PATH}"
    echo ""
    for result in "${RESULTS[@]}"; do
        echo "${result}"
    done
    echo ""
    echo "Passed: ${PASSED} / $((PASSED + FAILED))"
    echo "Failed: ${FAILED}"
} > "${SUMMARY_FILE}"

echo ""
echo "Summary saved to ${SUMMARY_FILE}"

# Exit with failure if any stage failed
[ "${FAILED}" -eq 0 ] || exit 1
