#!/usr/bin/env bash

# SLURM ETL chain: S01 -> S02 -> S03-07 -> S08 -> S09 -> S10
# Usage: sbatch scripts/slurm_etl_chain.sh

# Tunables (override via sbatch --export):
# RUN_ID, PART_FETCH, PART_FORMAT, PART_BUILD, PART_ENRICH, PART_FINAL,
# TIME_FETCH, TIME_FORMAT, TIME_BUILD, TIME_ENRICH, TIME_FINAL,
# MEM_FETCH, MEM_FORMAT, MEM_BUILD, MEM_ENRICH, MEM_FINAL

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

: "${RUN_ID:=}"

# Partitions (fallbacks)
: "${PART_FETCH:=general}"
: "${PART_FORMAT:=highmem}"
: "${PART_BUILD:=general}"
: "${PART_ENRICH:=general}"
: "${PART_FINAL:=general}"

# Times
: "${TIME_FETCH:=24:00:00}"
: "${TIME_FORMAT:=08:00:00}"
: "${TIME_BUILD:=06:00:00}"
: "${TIME_ENRICH:=12:00:00}"
: "${TIME_FINAL:=04:00:00}"

# Memory
: "${MEM_FETCH:=8G}"
: "${MEM_FORMAT:=64G}"
: "${MEM_BUILD:=16G}"
: "${MEM_ENRICH:=32G}"
: "${MEM_FINAL:=16G}"

LOG_DIR="${ROOT_DIR}/logs/slurm_etl"
mkdir -p "$LOG_DIR"

job_fetch=$(sbatch \
  --parsable \
  -p "$PART_FETCH" -t "$TIME_FETCH" --mem="$MEM_FETCH" \
  -J can_ETL_S01 \
  -o "$LOG_DIR"/s01_%j.out -e "$LOG_DIR"/s01_%j.err \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli fetch ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S01 (fetch) as job $job_fetch"

job_format=$(sbatch \
  --parsable \
  -p "$PART_FORMAT" -t "$TIME_FORMAT" --mem="$MEM_FORMAT" \
  -J can_ETL_S02 \
  -o "$LOG_DIR"/s02_%j.out -e "$LOG_DIR"/s02_%j.err \
  --dependency=afterok:${job_fetch} \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli format ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S02 (format) as job $job_format"

job_build=$(sbatch \
  --parsable \
  -p "$PART_BUILD" -t "$TIME_BUILD" --mem="$MEM_BUILD" \
  -J can_ETL_S03_07 \
  -o "$LOG_DIR"/s03_07_%j.out -e "$LOG_DIR"/s03_07_%j.err \
  --dependency=afterok:${job_format} \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli build ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S03-07 (build) as job $job_build"

job_enrich=$(sbatch \
  --parsable \
  -p "$PART_ENRICH" -t "$TIME_ENRICH" --mem="$MEM_ENRICH" \
  -J can_ETL_S08 \
  -o "$LOG_DIR"/s08_%j.out -e "$LOG_DIR"/s08_%j.err \
  --dependency=afterok:${job_build} \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli enrich ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S08 (enrich) as job $job_enrich"

job_finalize=$(sbatch \
  --parsable \
  -p "$PART_FINAL" -t "$TIME_FINAL" --mem="$MEM_FINAL" \
  -J can_ETL_S09 \
  -o "$LOG_DIR"/s09_%j.out -e "$LOG_DIR"/s09_%j.err \
  --dependency=afterok:${job_enrich} \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli finalize ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S09 (finalize) as job $job_finalize"

job_package=$(sbatch \
  --parsable \
  -p "$PART_FINAL" -t "$TIME_FINAL" --mem="$MEM_FINAL" \
  -J can_ETL_S10 \
  -o "$LOG_DIR"/s10_%j.out -e "$LOG_DIR"/s10_%j.err \
  --dependency=afterok:${job_finalize} \
  --export=ALL,RUN_ID="$RUN_ID" \
  --wrap "python -m src.cli package ${RUN_ID:+--run-id $RUN_ID}")
echo "Submitted S10 (package) as job $job_package"

echo "Chain submitted. Jobs: S01=$job_fetch S02=$job_format S03-07=$job_build S08=$job_enrich S09=$job_finalize S10=$job_package"


