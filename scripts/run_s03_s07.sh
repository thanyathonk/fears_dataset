#!/bin/bash

#SBATCH --job-name=s03-s07
#SBATCH --partition=scads
#SBATCH --nodes=1
#SBATCH --cpus-per-task=64
#SBATCH --gres=gpu:0
#SBATCH --mem=460G
#SBATCH --time=4-00:00:00
#SBATCH --output=logs/slurm-s03s07-%j.out
#SBATCH --error=logs/slurm-s03s07-%j.err

# Usage: sbatch scripts/run_s03_s07.sh
# Must be submitted from project root:
#   cd /ist/ist-share/scads/thanyathonk/fears_dataset
#   sbatch scripts/run_s03_s07.sh
#
# Partition: scads → ist-dgx04: 80 CPUs, 515 GB RAM [matches S02 environment]
# Resources: 64 CPUs + 460 GB

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1

echo "=========================================="
echo "Step 3 – S03, S05, S06, S06b, S07 (full_dataset)"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
echo ""

# S03: Join tables with early quality filtering
echo "=========================================="
echo "start: S03 – Join tables + Early quality filters"
echo "time: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
python -u -m src.cli run-stage s03_join_partition_age
echo "✅ S03 completed"
echo ""

# S05: Split ADR reports
echo "=========================================="
echo "start: S05 – Split ADR reports"
echo "time: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
python -u -m src.cli run-stage s04_split_adr
echo "✅ S05 completed"
echo ""

# S06: MedDRA mapping (PT→SOC via CONCEPT_ANCESTOR)
echo "=========================================="
echo "start: S06 – MedDRA mapping"
echo "time: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
python -u -m src.cli run-stage s05_map_omop_meddra
echo "✅ S06 completed"
echo ""

# S06b: MedDRA full hierarchy (PT→HLT→HLGT→SOC via CONCEPT_RELATIONSHIP)
echo "=========================================="
echo "start: S06b – MedDRA full hierarchy"
echo "time: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
python -u -m src.cli run-stage s05b_map_omop_meddra_full_hierarchy
echo "✅ S06b completed"
echo ""

# S07: Split drug names for LLM cleaning
echo "=========================================="
echo "start: S07 – Split drug names"
echo "time: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
python -u -m src.cli run-stage s06_split_drug
echo "✅ S07 completed"
echo ""

echo "=========================================="
echo "✅ All stages S03, S05, S06, S06b, S07 completed successfully"
echo "End: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
echo ""
echo "📋 Next steps (run separately in tmux sessions):"
echo ""
echo "1️⃣  Step 4 – S07b (LLM drug cleaning – GPU required):"
echo "   sbatch scripts/step4_s07b_llm.sh"
echo ""
echo "2️⃣  Step 5 – S08 (Drug enrichment via API – tmux recommended):"
echo "   tmux new-session -s s08 'bash scripts/step5_s08_enrich.sh'"
echo ""
echo "3️⃣  Step 6 – S09+S10 (after S07b and S08 complete):"
echo "   bash scripts/run_s09_s10.sh"
echo ""
