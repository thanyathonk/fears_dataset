# การตั้งค่า full_dataset แบบ Standalone

เมื่อแยก `full_dataset` ออกมาใช้เอง (ไม่ปะปนกับ pipeline หลัก) ต้องเตรียมข้อมูลดังนี้:

---

## 1. โครงสร้างโฟลเดอร์ที่ต้องมี

```
full_dataset/
├── data/
│   ├── vocab/                              # ← ต้อง copy เอง
│   │   └── vocabulary_SNOMED_MEDDRA_RxNorm_ATC/
│   │       ├── CONCEPT.csv
│   │       ├── CONCEPT_ANCESTOR.csv
│   │       ├── CONCEPT_RELATIONSHIP.csv
│   │       └── ...
│   ├── openFDA_drug_event/                 # จาก S01
│   ├── staging/                            # จาก S02–S09
│   ├── output/                             # จาก S10
│   ├── cache/
│   └── logs/
├── configs/
├── src/
└── scripts/
```

---

## 2. โฟลเดอร์ที่อ้างถึงจากนอก full_dataset (ต้องแก้)

| ข้อมูล | เดิม (อ้าง parent) | หลังแก้ (standalone) |
|--------|-------------------|----------------------|
| **vocab_root** | `../data/vocab` | `data/vocab` |
| **S02 staging** | hardcoded path ไป parent | ใช้ `data/staging/s02_entity_format` |
| **er_tables_path** | `../data/openFDA_drug_event/er_tables` | `data/er_tables` |

---

## 3. ขั้นตอนเตรียม Standalone

### 3.1 Copy OMOP Vocabulary

```bash
cd full_dataset
mkdir -p data/vocab
cp -r ../data/vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC data/vocab/
```

หรือถ้าย้ายจากที่อื่น:
```bash
# โหลด OMOP Vocabulary แล้ววางที่ data/vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC/
```

### 3.2 ข้อมูล OpenFDA (S01)

- รัน S01 ใน full_dataset → output ไปที่ `data/openFDA_drug_event/`
- หรือ copy จากที่เดิม:
```bash
cp -r /path/to/openFDA_drug_event full_dataset/data/
```

### 3.3 ตรวจสอบ Config

ไฟล์ `configs/config.local.yaml` ใช้ path แบบ standalone แล้ว:
- `vocab_root: data/vocab`
- `paths.root: .` (relative ต่อ full_dataset)

---

## 4. สรุปการแก้โค้ด (ไม่อ้าง parent)

| ไฟล์ | การแก้ |
|------|--------|
| `configs/config.local.yaml` | vocab_root → data/vocab, er_tables_path → data/er_tables |
| `src/stages/s03_join_partition_age.py` | ลบ hardcoded path ไป parent, ใช้เฉพาะ local staging |
| `src/cli.py` | ลบ shared_s02_dir check, ใช้เฉพาะ local staging |

---

## 5. การย้าย full_dataset ไปเครื่องอื่น

1. Copy โฟลเดอร์ `full_dataset/` ทั้งหมด
2. ตรวจสอบว่ามี `data/vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC/` ครบ
3. ถ้ายังไม่รัน S01: รัน S01 ก่อน
4. รัน pipeline ตาม [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)

### หมายเหตุ: S07b (LLM)

สคริปต์ `step4_s07b_llm.sh` ใช้ `MODEL_PATH` สำหรับ Qwen model (default: `/share/galaxy/thanyathon/models`). ถ้าย้ายเครื่อง ให้ตั้ง env ก่อนรัน:
```bash
export MODEL_PATH=/path/to/your/qwen-model
sbatch scripts/step4_s07b_llm.sh
```
