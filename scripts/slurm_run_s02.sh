#!/bin/bash

#SBATCH --job-name=s02
#SBATCH --partition=scads
#SBATCH --nodes=1
#SBATCH --cpus-per-task=64
#SBATCH --gres=gpu:0
#SBATCH --mem=460G
#SBATCH --time=4-00:00:00
#SBATCH --output=logs/slurm-s02-%j.out
#SBATCH --error=logs/slurm-s02-%j.err

# Usage: sbatch scripts/slurm_run_s02.sh
# Must be submitted from project root:
#   cd /ist/ist-share/scads/thanyathonk/fears_dataset
#   sbatch scripts/slurm_run_s02.sh
#
# Partition summary (as of 2026-03-17):
#   scads  → ist-dgx04:  80 CPUs, 515 GB RAM, 8 GPUs  [IDLE — best choice]
#   cpu    → ist-compute-1-[001-004]: 80 CPUs, 741 GB  [currently all allocated]
#
# Resources requested: 64 CPUs + 460 GB
#   Leaves 16 CPUs / ~55 GB for OS + NFS buffer.
#   DuckDB Phase 3: 64 threads + 400 GB in-memory → no disk spill → ~3-8 min.

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"

source ~/miniforge3/bin/activate
CONDA_ENV=${CONDA_ENV:-fulldata}
conda activate "${CONDA_ENV}"

export PYTHONUNBUFFERED=1
# ── Phase 1+2: Shard building + batch merge ──────────────────────────────────
# S02_OPENFDA_JOBS=20          - parallel CSV readers (NFS I/O bound)
# S02_MERGE_BATCH=120          - shards per batch (15 batches for 1710 shards)
# S02_MERGE_PARALLEL_BATCHES=4 - 4 parallel merge batches (safe with 460 GB)
# ── Phase 3: Wide table (DuckDB hash-partitioned GROUP BY) ───────────────────
# OOM root cause: one-pass GROUP BY on 55GB → 54M rows × 17 list cols needs
# ~400-500GB peak RAM (input decompress ~275GB + hash table ~150GB+).
# Fix: S02_DUCK_PARTITIONS=8 splits rows into 8 hash-buckets (hash(id)%8=P).
#   Each query: ~55GB input (streaming) + 6.75M-row hash table → ~80-100GB peak.
#   Total I/O: 55GB × 8 = 440GB NFS reads. Safe on 460GB allocation.
# ── Phase 7b: Dedup drug_mapping_input (DuckDB hash-partitioned window) ──────
# OOM root cause: ROW_NUMBER() OVER (PARTITION BY 10 keys) on 74M rows
#   needs ~111.7 GB+ (DuckDB hit 120 GB limit).
# Fix: S02_DEDUP_PARTITIONS=8 splits rows by hash(drug_profile_keys)%8=P.
#   Each pass: ~9.25M rows for window → ~15-25 GB peak.
#   Total I/O: 29GB × 8 = 232GB NFS reads. Resume-safe per partition file.
# S02_THREADS=64               - DuckDB threads per partition query
# S02_MEMORY_GB=120            - DuckDB limit per query (well above peak)
# S02_DUCK_PARTITIONS=8        - hash partitions for Phase 3 wide table
# S02_DEDUP_PARTITIONS=8       - hash partitions for Phase 7b dedup
# Polars fallback (if DuckDB unavailable):
# S02_WIDE_PARTITIONS=64       - hash partitions for partition-pivot
# S02_WIDE_JOBS=8              - parallel pivot workers
export S02_OPENFDA_JOBS=20
export S02_MERGE_BATCH=120
export S02_MERGE_PARALLEL_BATCHES=4
export S02_THREADS=64
export S02_MEMORY_GB=120
export S02_DUCK_PARTITIONS=8
export S02_DEDUP_PARTITIONS=8
export S02_WIDE_JOBS=8

echo "=========================================="
echo "S02 – Entity format (drug_openfda_wide, drug_mapping_input)"
echo "Memory allocated: 460G | Workers: 64"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Start: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="

python -u -m src.cli run-stage s02_entity_format

echo ""
echo "=========================================="
echo "✅ S02 completed"
echo "End: $(date 2>/dev/null || echo 'N/A')"
echo "=========================================="
