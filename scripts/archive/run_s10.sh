#!/bin/bash

#SBATCH --job-name=s09_s10
#SBATCH --partition=qgpu_gtx1070ti
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --gres=gpu:0
#SBATCH --mem=60G
#SBATCH --time=4-00:00:00
#SBATCH --output=test1_s10.out


set -euo pipefail

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-can-drug-pipeline}
conda activate "can-drug-pipeline"

RUN_ID_ROOT=${RUN_ID_ROOT:-$(date +"%Y%m%dT%H%M%S")}
LOG_ROOT="logs/slurm_${RUN_ID_ROOT}"
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
    "$@"
    local exit_code=$?
    if [[ ${exit_code} -ne 0 ]]; then
        echo "❌ ${description} failed (exit ${exit_code})"
        exit ${exit_code}
    fi
    echo "✅ ${description} completed"
    echo ""
}

echo "Starting S09 and S10 (RUN_ID_ROOT=${RUN_ID_ROOT})"


run_cmd "S09 – finalize merge and report" \
    python -m src.cli finalize --run-id "${RUN_ID_ROOT}_s09" && \
    python -m src.cli package --run-id "${RUN_ID_ROOT}_s10"



echo "All stages completed successfully."
echo "End time: $(date)"
