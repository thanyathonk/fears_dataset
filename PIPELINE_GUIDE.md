# Full Dataset Pipeline — คู่มือการใช้งาน

> อัปเดตล่าสุด: มีนาคม 2026  
> เอกสารหลักสำหรับการรัน pipeline FAERS/OpenFDA → final dataset

---

## สารบัญ

1. [ภาพรวม](#1-ภาพรวม)
2. [Quick Start](#2-quick-start)
3. [ขั้นตอนการรัน (Step-by-Step)](#3-ขั้นตอนการรัน-step-by-step)
4. [Data Flow & Lineage](#4-data-flow--lineage)
5. [Configuration](#5-configuration)
6. [Output Locations](#6-output-locations)
7. [Scripts Reference](#7-scripts-reference)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. ภาพรวม

Pipeline นี้ประมวลผล **FAERS (FDA Adverse Event Reporting System)** จาก OpenFDA เพื่อสร้าง dataset สำหรับการวิเคราะห์ Adverse Drug Reactions (ADR) แบ่งเป็น 2 cohort:

| Cohort | เกณฑ์อายุ |
|--------|-----------|
| **Adult** | 21 < อายุ ≤ 120 ปี |
| **Pediatric** | 0 < อายุ ≤ 21 ปี (ต้องมี NICHD age band) |

**เทคโนโลยีหลัก:** Polars, RxNav API, PubChem API, Qwen2.5-7B (LLM), OMOP Vocabulary

**Dependency Flow:**
```
S01 (fetch) → S02 (format) → S03 (join+partition)
    → S05 (split ADR) → S06/S06b (MedDRA)
    → S07 (split drug) → S07b [GPU] (LLM clean) → S08 [API] (enrich)
        → S09 (merge) → S10 (package)
```

---

## 2. Quick Start

### เตรียม Standalone (ครั้งแรก)

full_dataset ใช้ path ภายในตัวเอง ไม่อ้าง parent — ต้อง copy vocab ก่อน:

```bash
cd full_dataset
bash scripts/setup_standalone_vocab.sh   # copy จาก parent
# หรือ copy vocabulary_SNOMED_MEDDRA_RxNorm_ATC มาที่ data/vocab/
```

ดูรายละเอียด: [SETUP_STANDALONE.md](SETUP_STANDALONE.md)

### รัน Pipeline

```bash
cd /path/to/full_dataset
source ~/miniforge3/bin/activate
conda activate can-drug-pipeline

# Setup
python -m src.cli setup

# รันตามลำดับ (ดูรายละเอียดใน Section 3)
# S01: tmux + scripts/step1_s01_fetch.sh
# S02: sbatch scripts/slurm_run_s02.sh  หรือ  python -m src.cli format
# S03-S07: bash scripts/run_s03_s07.sh
# S07b: sbatch scripts/step4_s07b_llm.sh
# S08: tmux + scripts/step5_s08_enrich.sh
# S09-S10: bash scripts/run_s09_s10.sh
```

---

## 3. ขั้นตอนการรัน (Step-by-Step)

### S01 — Fetch OpenFDA FAERS

รันใน **tmux** (ใช้เวลานาน, หลาย GB)

```bash
tmux new-session -s s01 "bash scripts/step1_s01_fetch.sh"
tmux attach -t s01
```

- **Output:** `data/openFDA_drug_event/{report,patient,patient_drug,patient_drug_openfda,patient_reaction}/`
- **ต้องการ:** `OPENFDA_API_KEY` (optional, ลด rate-limit)

---

### S02 — Entity Format

สร้าง ER tables จาก CSV. มี 2 โหมด:

**Low-memory (default, ~16–60GB RAM):** ใช้ disk shards, resumable
```bash
sbatch scripts/slurm_run_s02.sh
# หรือ: python -m src.cli run-stage s02_entity_format
```

**High-memory (RAM ≥ 80GB):** เร็วกว่า, ไม่ใช้ disk shards
```bash
export S02_OPENFDA_HIGH_MEMORY=1
python -m src.cli run-stage s02_entity_format
# หรือแก้ config: stages.s02_openfda_high_memory: true
# หรือ: sbatch scripts/slurm_run_s02_high_memory.sh
```

- **Output:** `data/staging/s02_entity_format/`  
  - report, patient, drugcharacteristics, reactions, drug_openfda_wide, drug_mapping_input*

---

### S03–S07 — Join, MedDRA, Split Drug

```bash
# Local
tmux new-session -s step3 "bash scripts/run_s03_s07.sh"

# หรือ SLURM
sbatch scripts/slurm_run_s03_s07.sh
```

| Stage | ทำอะไร | Output |
|-------|--------|--------|
| S03 | Join + partition Pediatric/Adult + NICHD | `adult_events_full_data.parquet`, `pediatric_events_full_data.parquet` |
| S05 | Split ADR reactions | `{cohort}_adr_full_data.parquet` |
| S06 | Map MedDRA PT→SOC | `pt_soc_dictionary_full_data.parquet` |
| S06b | Map MedDRA full hierarchy (PT→HLT→HLGT→SOC) | `pt_hierarchy_dictionary_full_data.parquet` |
| S07 | Extract unique drug names | `{cohort}_drugs_full_data.parquet` |

---

### S07b — LLM Drug Name Cleaning (GPU)

```bash
sbatch scripts/step4_s07b_llm.sh
```

- **Output:** `data/staging/s07b_llm_clean/{cohort}_drugs_clean_full_data.parquet`

---

### S08 — Drug Enrichment (RxNorm/PubChem API)

รันใน **tmux** (ใช้เวลานาน, เรียก API)

```bash
# แยก cohort
tmux new-session -s s08_ped   "bash scripts/step5_s08_enrich.sh pediatric"
tmux new-session -s s08_adult "bash scripts/step5_s08_enrich.sh adult"

# หรือรันทั้ง 2
tmux new-session -s s08 "bash scripts/step5_s08_enrich.sh"
```

- **Output:** `data/staging/s08_enrich_drug_identifiers/{cohort}_drugs_enriched_final_full_data.parquet`

---

### S09–S10 — Finalize & Package

รัน **หลัง** S07b และ S08 เสร็จ

```bash
bash scripts/run_s09_s10.sh
# หรือ: sbatch scripts/run_s09_s10.sh
```

- **Output:** `data/output/Adult/`, `data/output/Pediatric/`

---

## 4. Data Flow & Lineage

```
OpenFDA API
    │
    ▼
[S01] Download → CSV (report/, patient/, patient_drug/, patient_drug_openfda/, patient_reaction/)
    │
    ▼
[S02] Parse → Parquet ER tables (report, patient, drugcharacteristics, reactions, drug_openfda_wide, drug_mapping_input)
    │
    ▼
[S03] Join 6 tables + Age filter → adult_events_full_data, pediatric_events_full_data
    │
    ├──────────────────────┐
    ▼                      ▼
[S05] Split ADR        [S07] Extract unique drugs
    │                      │
    ▼                      ▼
[S06/S06b] MedDRA     [S07b] LLM clean
    │                      │
    │                  [S08] RxNorm/PubChem enrich
    │                      │
    └──────────┬───────────┘
               ▼
           [S09] Merge → patient_report_reporter_drug_reaction_full_data.parquet
               │
               ▼
           [S10] Package → output/{Adult,Pediatric}/
```

### Filter ที่ทำใน S03

| เงื่อนไข | รายละเอียด |
|---------|-----------|
| `age_years` | > 0 และ ≤ 120 |
| `reaction_meddrapt` | ไม่ null/empty |
| `medicinal_product` | ไม่ null/empty |
| `reporter_qualification` | ตัด Unknown, Lawyer, Consumer |
| Date | 2014–2025 |

### Output Naming

ไฟล์ output ใช้ suffix `_full_data` เพื่อแยกจาก main pipeline:
- `adult_events_full_data.parquet`
- `{cohort}_drugs_clean_full_data.parquet`
- `patient_report_reporter_drug_reaction_full_data.parquet`

---

## 5. Configuration

**ไฟล์:** `configs/config.local.yaml`

| พารามิเตอร์ | ความหมาย |
|-------------|----------|
| `paths.vocab_root` | OMOP Vocabulary (ใช้ร่วมกับ parent) |
| `paths.data_root` | โฟลเดอร์ข้อมูล |
| `cohorts.age_cutoff` | 21 (ปี) |
| `stages.s02_openfda_high_memory` | true = ใช้โหมด high-memory สำหรับ S02 |
| `openfda.parallel_downloads` | 10 |

**Environment variables:**
- `OPENFDA_API_KEY` — ลด rate-limit
- `S02_OPENFDA_HIGH_MEMORY=1` — เปิดโหมด high-memory สำหรับ S02

---

## 6. Output Locations

| โฟลเดอร์ | เนื้อหา |
|----------|---------|
| `data/openFDA_drug_event/` | Raw CSV จาก S01 |
| `data/staging/s02_entity_format/` | ER tables จาก S02 |
| `data/staging/s03_join_partition_age/` | Cohort events |
| `data/staging/s05_split_adr/` | ADR tables |
| `data/staging/s06_map_omop_meddra/` | MedDRA mapping |
| `data/staging/s06b_map_omop_meddra_full_hierarchy/` | MedDRA full hierarchy |
| `data/staging/s07_split_drug/` | Unique drugs |
| `data/staging/s07b_llm_clean/` | LLM-cleaned drugs |
| `data/staging/s08_enrich_drug_identifiers/` | Enriched drugs |
| `data/staging/s09_finalize_merge_and_report/` | Final merge |
| `data/output/Adult/`, `data/output/Pediatric/` | Package deliverables |
| `logs/` | Run logs |

---

## 7. Scripts Reference

| ไฟล์ | ใช้สำหรับ |
|------|-----------|
| `scripts/setup_standalone_vocab.sh` | Copy vocab จาก parent (เตรียม standalone) |
| `scripts/step1_s01_fetch.sh` | S01 Fetch (tmux) |
| `scripts/step2_s02_format.sh` | S02 Format (local) |
| `scripts/slurm_run_s02.sh` | S02 Format (SLURM, 60GB) |
| `scripts/slurm_run_s02_high_memory.sh` | S02 Format (SLURM, 120GB, high-memory) |
| `scripts/run_s03_s07.sh` | S03–S07 (local) |
| `scripts/slurm_run_s03_s07.sh` | S03–S07 (SLURM) |
| `scripts/step4_s07b_llm.sh` | S07b LLM (SLURM GPU) |
| `scripts/step5_s08_enrich.sh` | S08 Enrich (tmux) |
| `scripts/run_s09_s10.sh` | S09–S10 |
| `scripts/archive_old_data.sh` | Archive ผลลัพธ์เก่า |

---

## 8. Troubleshooting

### S02 — Segmentation fault / OOM

- ใช้ **pandas+pyarrow** แทน Polars สำหรับ openfda (แก้แล้วในโค้ด)
- ไฟล์ gzip เสีย 6 ไฟล์ (2010q4, 2020q3) จะถูก skip อัตโนมัติ
- ถ้ามี RAM เยอะ: ตั้ง `S02_OPENFDA_HIGH_MEMORY=1`

### S02 — Corrupt shards

- ลบ `data/staging/s02_entity_format/_tmp_openfda_shards/` แล้วรันใหม่
- อย่ารัน S02 พร้อมกัน 2 process

### ตรวจสอบ Progress

```bash
tmux ls
tmux attach -t <session>
tail -f logs/*/S*.log
squeue -u $USER  # SLURM jobs
```

---

## Checklist

- [ ] S01 เสร็จ
- [ ] S02 เสร็จ (drug_openfda_wide, drug_mapping_input)
- [ ] S03–S07 เสร็จ
- [ ] S07b เสร็จ
- [ ] S08 เสร็จ (ทั้ง pediatric และ adult)
- [ ] S09–S10 เสร็จ
- [ ] ตรวจสอบ `data/output/Adult/` และ `data/output/Pediatric/`
