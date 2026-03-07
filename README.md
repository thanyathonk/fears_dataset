# Full Dataset Pipeline

Pipeline สำหรับประมวลผล **FAERS (FDA Adverse Event Reporting System)** จาก OpenFDA → dataset สำหรับวิเคราะห์ Adverse Drug Reactions (ADR) แบ่ง Pediatric / Adult cohort.

## Quick Start

```bash
cd full_dataset
source ~/miniforge3/bin/activate
conda activate can-drug-pipeline

python -m src.cli setup
```

**คู่มือฉบับเต็ม:** [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)

## โครงสร้าง

```
full_dataset/
├── configs/config.local.yaml   # การตั้งค่า
├── src/                        # Pipeline code
├── scripts/                    # Run scripts (S01–S10)
├── data/
│   ├── openFDA_drug_event/    # Raw จาก S01
│   ├── staging/               # Intermediate (S02–S09)
│   └── output/                # Final (Adult/, Pediatric/)
└── logs/
```

## ขั้นตอนหลัก

| Step | Stage | คำสั่ง |
|------|-------|--------|
| 1 | S01 Fetch | `tmux` + `scripts/step1_s01_fetch.sh` |
| 2 | S02 Format | `sbatch scripts/slurm_run_s02.sh` |
| 3 | S03–S07 | `bash scripts/run_s03_s07.sh` |
| 4 | S07b LLM | `sbatch scripts/step4_s07b_llm.sh` |
| 5 | S08 Enrich | `tmux` + `scripts/step5_s08_enrich.sh` |
| 6 | S09–S10 | `bash scripts/run_s09_s10.sh` |

## Configuration

- **Standalone:** full_dataset ใช้ path ภายในตัวเอง (`data/vocab`, `data/staging`) ไม่อ้าง parent
- **เตรียม vocab:** ต้อง copy OMOP vocabulary มาที่ `data/vocab/` — ดู [SETUP_STANDALONE.md](SETUP_STANDALONE.md)
- **Output:** เก็บใน `full_dataset/data/output/`

ดูรายละเอียดเพิ่มใน [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)
