# ชุดข้อมูล FEARS — วิธีการ Pipeline และภาพรวมชุดข้อมูล

> **อัปเดตล่าสุด:** 2026-03-31  
> **แหล่งข้อมูล:** FDA Adverse Event Reporting System (FAERS) ผ่าน openFDA API  
> **ช่วงเวลา:** ไตรมาส 1 ปี 2557 – ไตรมาส 4 ปี 2568 (Q1 2014 – Q4 2025)  
> **กลุ่มประชากร (Cohort):** ผู้ใหญ่ (อายุ > 21 ปี) · เด็กและวัยรุ่น (อายุ 0–21 ปี, แบ่งตาม NICHD)

---

## สารบัญ

1. [ภาพรวม Pipeline](#1-ภาพรวม-pipeline)
2. [วิธีการแต่ละขั้นตอน (S01–S10)](#2-วิธีการแต่ละขั้นตอน-s01s10)
   - [S01 – ดึงข้อมูลจาก openFDA](#s01--ดึงข้อมูลจาก-openfda)
   - [S02 – จัดรูปแบบ Entity](#s02--จัดรูปแบบ-entity)
   - [S03 – Join และแบ่งกลุ่มอายุ](#s03--join-และแบ่งกลุ่มอายุ)
   - [S05 – แยกตาราง ADR](#s05--แยกตาราง-adr)
   - [S06 / S06b – จับคู่ MedDRA](#s06--s06b--จับคู่-meddra)
   - [S07 – รวมรายชื่อยา](#s07--รวมรายชื่อยา)
   - [S07b – LLM แยกส่วนประกอบยา](#s07b--llm-แยกส่วนประกอบยา)
   - [S08 – เพิ่มข้อมูล RxNorm](#s08--เพิ่มข้อมูล-rxnorm)
   - [S09 – รวมข้อมูลสุดท้ายและลบซ้ำ](#s09--รวมข้อมูลสุดท้ายและลบซ้ำ)
   - [S10 – แพ็กเกจไฟล์ส่งมอบ](#s10--แพ็กเกจไฟล์ส่งมอบ)
3. [สรุปตัวกรองคุณภาพข้อมูล](#3-สรุปตัวกรองคุณภาพข้อมูล)
4. [ภาพรวมชุดข้อมูล](#4-ภาพรวมชุดข้อมูล)
5. [โครงสร้างไฟล์เอาต์พุต](#5-โครงสร้างไฟล์เอาต์พุต)
6. [ข้อจำกัดและข้อสังเกต](#6-ข้อจำกัดและข้อสังเกต)

---

## 1. ภาพรวม Pipeline

```
openFDA API
    │
    ▼
S01  ดึงข้อมูล JSON รายงาน ADR รายไตรมาส → แปลงเป็น CSV แบบ Flat
    │
    ▼
S02  จัดรูปแบบตาราง Entity → Parquet (patient, report, drug, reaction, reporter)
    │
    ▼
S03  Inner Join ทุกตาราง · กรองเฉพาะยา Suspect + ผู้รายงานที่ผ่านเกณฑ์
     · แบ่งกลุ่มตามอายุ → adult_events / pediatric_events (2557–2568)
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
S05  ดึงแถว ADR ที่ไม่ซ้ำ            S07  รวมเป็นหนึ่งแถวต่อชื่อยา
     (safetyreportid, PT, outcome)          พร้อม context columns แบบ list
    │                                      │
    ▼                                      ▼
S06/S06b  จับคู่ PT → SOC ผ่าน      S07b  LLM (Qwen) แยกโครงสร้างยา:
          MedDRA OMOP vocab               ingredients · strength · dosage_form
          → pt_soc_dictionary             ing_source = faers | llm | bracket
                                           │
                                           ▼
                                    S08  เพิ่มข้อมูล RxNorm (7 ขั้นตอนต่อเนื่อง):
                                          basename → ingredient → LocalCID
                                          → ตัดคำต่อท้าย → ChEMBL → approx → KEGG
                                          + เติม ing_source (rxnav_basename ฯลฯ)
                                          + แยกแถวที่ map ไม่ได้ / น่าสงสัย → quarantine
    │                                      │
    └──────────────┬───────────────────────┘
                   ▼
S09  Inner Join สามทาง: เหตุการณ์ × ข้อมูลยา × พจนานุกรม PT-SOC
     ลบซ้ำตาม (safetyreportid, medicinal_product, reaction_meddrapt)
     → patient_report_reporter_drug_reaction_full_data.parquet
                   │
                   ▼
S10  แพ็กเกจตาราง Dimension:
     drug_full_data · adr_full_data · standard_reaction_full_data
```

> **หมายเหตุ:** S04 ถูกรวมเข้ากับ S03 แล้ว ไม่มี S04 แยกต่างหาก

---

## 2. วิธีการแต่ละขั้นตอน S01–S10

### S01 – ดึงข้อมูลจาก openFDA

**Script:** `src/stages/s01_fetch_openfda.py`

ดาวน์โหลดไฟล์ ZIP รายไตรมาสของ FAERS จาก openFDA API และแปลง JSON ซ้อนชั้นให้เป็น CSV แบบ Flat การดำเนินการหลัก:

- ส่ง HTTP request พร้อม `OPENFDA_API_KEY` โดยจำกัด rate และทำ exponential backoff เมื่อเกิดข้อผิดพลาด
- แยกแต่ละไฟล์ไปยัง `patient_drug/*.csv.gzip`, `patient/*.csv.gzip` เป็นต้น
- ถอดรหัส Enum ต่างๆ (drug characterization, เส้นทางให้ยา, ผลลัพธ์) โดยใช้ FAERS codebook
- แยก `activesubstance` จาก JSON ซ้อนชั้น และแปลงเป็น `activesubstance_name`
- เขียน `patient_drug_openfda/*.csv.gzip` แบบ Long Format (หนึ่งแถวต่อหนึ่ง key-value ของ openfda ต่อยาหนึ่งรายการ)
- ตรวจสอบซ้ำตาม `(safetyreportid, entry)` ในแต่ละไฟล์

**เอาต์พุต:** `data/raw/patient_drug/`, `data/raw/patient/`, `data/raw/reporter/`, `data/raw/reactions/` เป็นต้น

---

### S02 – จัดรูปแบบ Entity

**Script:** `src/stages/s02_entity_format_stream.py`

อ่าน CSV shard จาก S01 และสร้างตาราง Parquet แบบ Normalized ER โดยใช้ Polars Streaming เพื่อหลีกเลี่ยงการโหลดข้อมูลขนาดใหญ่เข้า RAM ทั้งหมดในครั้งเดียว

| ตาราง | คีย์ | คำอธิบาย |
|-------|------|-----------|
| `patient.parquet` | `safetyreportid` | อายุ, เพศ, น้ำหนัก; `patient_custom_master_age` เป็นปีเต็ม |
| `report.parquet` | `safetyreportid` | วันที่รับรายงาน / อัปเดต |
| `report_serious.parquet` | `safetyreportid` | Flag ความรุนแรง (เสียชีวิต, นอนโรงพยาบาล, …) |
| `reporter.parquet` | `safetyreportid` | คุณสมบัติผู้รายงาน, ประเทศ, บริษัท |
| `reactions.parquet` | `safetyreportid` | `reaction_meddrapt`, `reaction_outcome` |
| `drugcharacteristics_extended.parquet` | `(safetyreportid, entry)` | ทุก field ระดับยา + active substance ที่แยกแล้ว |
| `drug_openfda_wide.parquet` | `(safetyreportid, entry)` | คอลัมน์ openfda_* แบบ list (rxcui, ชื่อ generic/brand, …) |
| `drug_mapping_input.parquet` | `(safetyreportid, entry)` | LEFT JOIN ระหว่าง extended + openfda_wide |

อายุถูกแปลงจากหน่วย FAERS (ปี/เดือน/วัน/สัปดาห์) เป็นปีเต็ม (`np.floor`)  
รองรับโหมดใช้ RAM สูงผ่าน `S02_OPENFDA_HIGH_MEMORY=1`

---

### S03 – Join และแบ่งกลุ่มอายุ

**Script:** `src/stages/s03_join_partition_age.py`

รวมตารางทั้งหมดด้วย Inner Join บน `safetyreportid` ใช้ตัวกรองคุณภาพ แล้วแบ่งเป็น cohort ตามอายุ

**ตัวกรองคุณภาพที่ใช้ใน S03:**

| ตัวกรอง | เกณฑ์ |
|---------|-------|
| ช่วงวันที่ | `2014-01-01 ≤ timeline_key ≤ 2025-12-31` (ลำดับ: mostrecent → lastupdate → receive) |
| ID ถูกต้อง | `safetyreportid` ต้องเป็นตัวเลขล้วน (`^\d+$`) ไม่รวม composite key เช่น `4816137-9` |
| ปฏิกิริยาไม่ null | `reaction_meddrapt IS NOT NULL` |
| ชื่อยาไม่ null | `medicinal_product IS NOT NULL AND len > 0` |
| เฉพาะยา Suspect | `drug_characterization` ขึ้นต้นด้วย `"Suspect"` — ตัด Concomitant ออก |
| ผู้รายงานผ่านเกณฑ์ | ตัด: `Unknown`, `Lawyer`, `Consumer or non-health professional` |

**การแบ่งกลุ่มตามอายุ:**

| Cohort | ช่วงอายุ | คอลัมน์เพิ่มเติม |
|--------|----------|------------------|
| เด็ก/วัยรุ่น (Pediatric) | 0–21 ปี | `nichd` (กลุ่มพัฒนาการ NICHD) |
| ผู้ใหญ่ (Adult) | 22–120 ปี | — |

**การแบ่งกลุ่ม NICHD:**

| กลุ่ม | ช่วงอายุ |
|-------|----------|
| infancy (ทารก) | < 1 ปี (รวมแรกเกิด) |
| toddler (เด็กเล็ก) | 1 ปี |
| early_childhood (วัยเด็กตอนต้น) | 2–5 ปี |
| middle_childhood (วัยเด็กตอนกลาง) | 6–11 ปี |
| early_adolescence (วัยรุ่นตอนต้น) | 12–17 ปี |
| late_adolescence (วัยรุ่นตอนปลาย) | 18–21 ปี |

**เอาต์พุต:** `data/staging/s03_join_partition_age/{cohort}_events_full_data.parquet`

---

### S05 – แยกตาราง ADR

**Script:** `src/stages/s05_split_adr.py`

ดึง tuple ที่ไม่ซ้ำของ `(safetyreportid, reaction_meddrapt, reaction_outcome)` จากข้อมูล S03 ใช้ `.drop_nulls(["reaction_meddrapt"]).unique()` เพื่อป้องกันแถวปฏิกิริยาซ้ำก่อนจับคู่กับ MedDRA

**เอาต์พุต:** `data/staging/s05_split_adr/{cohort}_adr_full_data.parquet`

---

### S06 / S06b – จับคู่ MedDRA

**Scripts:** `src/stages/s06_map_omop_meddra.py` · `src/stages/s06b_map_omop_meddra_full_hierarchy.py`

จับคู่แต่ละ `reaction_meddrapt` (Preferred Term, PT) กับลำดับชั้น MedDRA โดยใช้ OMOP vocabulary (`vocabulary_SNOMED_MEDDRA_RxNorm_ATC`)

**S06:** ใช้ `CONCEPT_ANCESTOR` เพื่อ map PT→SOC แบบย่อ สร้าง `pt_soc_dictionary_full_data.parquet` (ใช้ใน S09)

**S06b:** ใช้ `CONCEPT_RELATIONSHIP` เพื่อสร้างลำดับชั้นเต็ม PT→HLT→HLGT→SOC สร้าง:
- `pt_hierarchy_dictionary_full_data.parquet` — ข้อมูล PT พร้อม list รวมทุกระดับ
- `pt_hierarchy_paths_full_data.parquet` — ขยายออกเป็นหนึ่งแถวต่อ path

**SOC ที่ตัดออก** (ไม่เกี่ยวกับผลข้างเคียงจากยาโดยตรง):
- Surgical and medical procedures (หัตถการทางการแพทย์)
- Social circumstances (สภาพสังคม)
- Product issues (ปัญหาของผลิตภัณฑ์)

การจับคู่ข้อความใช้ Title Case normalization ตามมาตรฐาน MedDRA

**เอาต์พุต:** `data/staging/s06_map_omop_meddra/{cohort}/pt_soc_dictionary_full_data.parquet`

---

### S07 – รวมรายชื่อยา

**Script:** `src/stages/s07_split_drug.py`

ยุบตารางเหตุการณ์ให้เหลือหนึ่งแถวต่อ `medicinal_product` ที่ไม่ซ้ำ โดยรวม context columns เป็น list ที่ไม่ซ้ำและเรียงลำดับ (ผ่าน `implode().list.unique().list.sort()`):

- `active_substance_faers` — รายชื่อ active substance จาก FAERS (list)
- `drug_dosage_form` — list รูปแบบยา
- `drug_authorization_number` — รหัสทะเบียนยา
- `action_drug` — การดำเนินการกับยา

ชื่อยาถูก Normalize ด้วย `normalize_faers_drug_name()` ก่อน Aggregation เพื่อลดความซ้ำซ้อนของชื่อที่ใกล้เคียงกัน

**เอาต์พุต:** `data/staging/s07_split_drug/{cohort}_drugs_full_data.parquet`

---

### S07b – LLM แยกส่วนประกอบยา

**Script:** `src/stages/s07b_llm_clean.py` (รันใน process) · `scripts/s07_openai_run.py` (batch OpenAI)

แยกโครงสร้างข้อมูลยาจากชื่อยาดิบโดยใช้ Large Language Model (ค่าเริ่มต้น: `Qwen/Qwen2.5-32B-Instruct` หรือ OpenAI GPT-4)

**ลำดับความสำคัญในการดึงข้อมูล:**

1. **`faers`** — `active_substance_faers` มีชื่อส่วนประกอบยาอยู่แล้ว → ใช้ตรงๆ (ไม่ต้องใช้ LLM)
2. **`llm`** — LLM ดึง ingredients, strength, dosage_form, qualifier จากข้อความชื่อยา
3. **`bracket`** — Fallback เมื่อทั้ง FAERS และ LLM ไม่ให้ข้อมูล: ดึงจากรูปแบบวงเล็บในชื่อยา

แหล่งที่มาของส่วนประกอบยาบันทึกใน **`ing_source`** (`faers` | `llm` | `bracket` | `null`)  
ค่า `null` หมายถึง LLM ไม่ได้ส่งคืน ingredient list ที่ใช้งานได้ และ FAERS ก็ไม่มีข้อมูล active substance

**ฟิลด์ที่ผลิตต่อแถว:**

| คอลัมน์ | ประเภท | แหล่งที่มา |
|---------|--------|------------|
| `medicinal_product` | str | ชื่อยาดิบ (key) |
| `basename` | str | ชื่อยาหลักหลังตัดรูปแบบออก (regex + LLM) |
| `ingredients` | str/list | รายชื่อส่วนประกอบยาที่ดึงได้ |
| `salt` | list | รูปแบบเกลือ / counter-ion ที่ตรวจพบ |
| `strength` | str | ความแรงของยา เช่น `"10MG"` |
| `dosage_form` | str | รูปแบบยา (tablet / capsule / injection / …) |
| `qualifier` | str | คำขยายที่ไม่ใช่ชื่อยา (ประเทศ, brand variant) |
| `qualifier_type` | str | `COUNTRY` / `BRAND` / … |
| `ing_source` | str | `faers` / `llm` / `bracket` / `null` |

**เอาต์พุต:** `data/staging/s07b_llm_clean/{cohort}_drugs_llm_cleaned.parquet`

---

### S08 – เพิ่มข้อมูล RxNorm

**Script:** `src/stages/s08_enrich_drug_identifiers_local.py`

แปลง `basename` ของแต่ละยา (จาก S07b) ให้เป็น **RxCUI** มาตรฐาน (RxNorm Concept Identifier) ผ่านการค้นหา 7 ขั้นตอนต่อเนื่อง จากนั้นเพิ่มรายชื่อ `rxnorm_ingredients`

**ลำดับการค้นหา (เรียงตามความสำคัญ):**

| ขั้นที่ | วิธีการ | ค่า `lookup_hit` |
|--------|---------|-----------------|
| 1 | RxNav API จับคู่ตรงกับ `basename` | `basename` |
| 2 | RxNav API จับคู่ตรงกับแต่ละ `ingredient` | `ingredients` |
| 3 | LocalCID (SQLite ออฟไลน์) → ชื่อมาตรฐาน → RxNav | ชื่อมาตรฐาน |
| 4 | ตัดคำต่อท้ายทางเภสัชกรรม (SULFATE, TABLET, TEVA ฯลฯ) → RxNav ใหม่ | `suffix_strip:<ชื่อที่ตัดแล้ว>` |
| 5 | ChEMBL ค้นหา brand → `pref_name` → RxNav (จับคู่ exact เท่านั้น) | ชื่อ pref_name |
| 6 | RxNav approximate match (คะแนน ≥ 8.0, ความคล้าย ≥ 0.70, อักษรแรกต้องตรงกรณีคำเดียว) | `approx:<ชื่อที่ match>` |
| 7 | KEGG Drug (ยานอก US) → INN → RxNav | `kegg:<id>:<inn>` |

หลังค้นหาครบ **`ing_source` ถูกเติมให้** สำหรับแถวที่ยังเป็น `null`:
- `rxnav_basename` — map ได้ผ่าน basename exact match
- `rxnav_ingredients` — map ได้ผ่าน ingredient exact match
- `rxnorm_enriched` — map ได้ผ่าน approx / KEGG / CID / ChEMBL / suffix-strip

#### การแยก Quarantine

หลัง enrichment แถวถูกแยกเป็น:

| ปลายทาง | เงื่อนไข | `s08_quarantine_reason` |
|---------|---------|------------------------|
| **main** `*_drugs_enriched.parquet` | มี `rxcui` และไม่น่าสงสัย | — |
| **quarantine** `quarantine/*_drugs_quarantine.parquet` | `rxcui IS NULL` | `no_rxcui` |
| | ชื่อยาน่าสงสัย เช่น `(UNKNOWN)`, `CHINESE HERBAL MEDICINES`, `DIET AID`, `DRUG UNKNOWN` | `suspicious_name` |
| | ทั้งสองเงื่อนไข | `no_rxcui_and_suspicious` |

ตั้ง `S08_QUARANTINE_ONLY_UNMAPPED=1` เพื่อส่ง quarantine เฉพาะแถวที่ map ไม่ได้เท่านั้น

**เอาต์พุต:**
- `data/staging/s08_enrich_drug_identifiers/{cohort}_drugs_enriched.parquet`
- `data/staging/s08_enrich_drug_identifiers/quarantine/{cohort}_drugs_quarantine.parquet`

---

### S09 – รวมข้อมูลสุดท้ายและลบซ้ำ

**Script:** `src/stages/s09_finalize_merge_and_report.py`

ทำ Inner Join สามทางเพื่อสร้างตาราง Fact พร้อมใช้วิเคราะห์:

```
ข้อมูลเหตุการณ์ (S03)
    × ข้อมูลยา enriched (S08)  — join บน medicinal_product; 1 แถวต่อยา (ป้องกัน Cartesian)
    × พจนานุกรม PT-SOC (S06)   — join บน reaction_meddrapt (normalize เป็น Title Case)
```

**การรับประกันความถูกต้องของ Join:** ตาราง enriched drug ถูก pre-dedup ด้วย `.unique(subset=["medicinal_product"], keep="first")` ก่อน join เพื่อป้องกันการ fan-out จากหลาย RxCUI ต่อชื่อยา

**การประมวลผลหลัง Join:**
- `reaction_meddrapt` Normalize เป็น Title Case ก่อน join กับ dictionary
- รวม Flag ความรุนแรง: `GREATEST(death, hospitalization, …)` → `serious`
- คอลัมน์วันที่: เก็บ `receive_date`, `mostrecent_receive_date`, `lastupdate_date` ไว้ทั้ง 3 คอลัมน์
- เติม "Unknown" สำหรับ: `patient_sex`, `reaction_outcome`, `drug_administration`, `drug_indication`, `reporter_country`, `reporter_company`, `reporter_qualification`

**การลบซ้ำ:** ใช้ `.unique(subset=["safetyreportid", "medicinal_product", "reaction_meddrapt"], keep="first")` เพื่อให้แต่ละ (รายงาน, ยา, ปฏิกิริยา) ปรากฏเพียงครั้งเดียว

ประมวลผลด้วย Polars batched streaming เพื่อควบคุมการใช้ RAM ในชุดข้อมูลผู้ใหญ่ขนาดใหญ่ (~100 ล้านแถว input → ~18 ล้านแถว output)

**เอาต์พุต:** `data/output/{Adult,Pediatric}/patient_report_reporter_drug_reaction_full_data.parquet`

---

### S10 – แพ็กเกจไฟล์ส่งมอบ

**Script:** `src/stages/s10_package_deliverables.py`

สร้างตาราง Dimension เพิ่มเติมอีกสามตารางจาก Fact table ของ S09 (ไม่มี Join ใหม่):

| ตาราง | คีย์ | Logic |
|-------|------|-------|
| `drug_full_data.parquet` | `ingredient` | Group by `ingredient`, ดึง `.first()` สำหรับ `medicinal_product`, `rxcui`, `mapping_method` |
| `adr_full_data.parquet` | `(safetyreportid, reaction_meddrapt)` | คู่ที่ไม่ซ้ำพร้อม MedDRA code และ SOC list |
| `standard_reaction_full_data.parquet` | `(safetyreportid, reaction_meddrapt)` | เหมือนกับ `adr_full_data` (ชื่อทางเลือก) |

ทุกตารางบีบอัดด้วย ZSTD และบันทึกไว้ที่ `data/output/{Adult,Pediatric}/`

---

## 3. สรุปตัวกรองคุณภาพข้อมูล

| ตัวกรอง | Stage | เกณฑ์ |
|---------|-------|-------|
| ช่วงวันที่ | S03 | 2014-01-01 ถึง 2025-12-31 |
| safetyreportid ถูกต้อง | S03 | ตัวเลขล้วน (`^\d+$`) |
| ปฏิกิริยาไม่ null | S03 | `reaction_meddrapt IS NOT NULL` |
| เฉพาะยา Suspect | S03 | `drug_characterization` ขึ้นต้นด้วย `"Suspect"` |
| ผู้รายงานผ่านเกณฑ์ | S03 | ตัด Unknown / Lawyer / Consumer |
| อายุถูกต้อง | S03 | อายุ 0–120; แถวที่อายุ null ถูกตัดออก |
| ชื่อยาถูกต้อง | S03 | `medicinal_product IS NOT NULL AND len > 0` |
| ครอบคลุม MedDRA | S09 | Inner join → เฉพาะ PT ที่อยู่ใน dictionary |
| ครอบคลุม RxNorm | S09 | Inner join → เฉพาะยาที่ resolve `rxcui` ได้ |
| ลบซ้ำ | S09 | Unique บน `(safetyreportid, medicinal_product, reaction_meddrapt)` |
| Quarantine ยา map ไม่ได้ | S08 | `rxcui IS NULL` หรือชื่อน่าสงสัย → แยกไฟล์ |

---

## 4. ภาพรวมชุดข้อมูล

ตัวเลขทั้งหมดมาจากการรัน Pipeline **20260329T223533** (เสร็จ 2026-03-30)

### จำนวนแถว

| ตาราง | ผู้ใหญ่ | เด็ก/วัยรุ่น |
|-------|-------:|-------------:|
| Fact: `patient_report_reporter_drug_reaction_full_data` | **17,979,420** | **966,168** |
| ADR dimension: `adr_full_data` | 5,410,155 | 453,039 |
| Reaction dimension: `standard_reaction_full_data` | 5,410,155 | 453,039 |
| Drug dimension: `drug_full_data` | 4,944 | 2,881 |
| S08 enriched drugs (main) | 85,267 | 22,437 |
| S08 quarantine drugs | 6,221 | 1,747 |

### ค่าไม่ซ้ำในชุดข้อมูลผู้ใหญ่

| ข้อมูล | จำนวน |
|--------|------:|
| `safetyreportid` (รายงาน) ไม่ซ้ำ | ~2,539,839 |
| `medicinal_product` (ชื่อยา) ไม่ซ้ำ | ~67,193 |
| `ingredient` (ส่วนประกอบยา) ไม่ซ้ำ | ~4,944 |
| `reaction_meddrapt` (MedDRA PT) ไม่ซ้ำ | ~1,640 |
| RxCUI ไม่ซ้ำ | ~8,007 |
| เฉลี่ยแถวต่อรายงาน | ~7.06 |
| เฉลี่ยแถวต่อ (รายงาน, ยา) | ~3.68 |

### การกระจายตัวอายุ

| Cohort | อายุต่ำสุด | อายุสูงสุด | Null |
|--------|----------:|----------:|-----:|
| ผู้ใหญ่ | 22 ปี | 120 ปี | 0 |
| เด็ก/วัยรุ่น | 0 ปี | 21 ปี | 0 |

### กลุ่ม NICHD (เด็กและวัยรุ่น)

| กลุ่ม | ช่วงอายุ |
|-------|----------|
| infancy (ทารก) | < 1 ปี |
| toddler (เด็กเล็ก) | 1 ปี |
| early_childhood (วัยเด็กตอนต้น) | 2–5 ปี |
| middle_childhood (วัยเด็กตอนกลาง) | 6–11 ปี |
| early_adolescence (วัยรุ่นตอนต้น) | 12–17 ปี |
| late_adolescence (วัยรุ่นตอนปลาย) | 18–21 ปี |

### ลักษณะยา

ทุกแถวใน Fact table มีค่า `drug_characterization = "Suspect (the drug was considered by the reporter to have caused or contributed to the event)"` ไม่มีแถวยา Concomitant

### คุณสมบัติผู้รายงาน (ผู้ใหญ่)

| คุณสมบัติ | แถว | % |
|-----------|----:|--:|
| Other health professional (วิชาชีพสุขภาพอื่น) | 11,041,092 | 61.4% |
| Physician (แพทย์) | 5,864,404 | 32.6% |
| Pharmacist (เภสัชกร) | 1,073,924 | 6.0% |

### วิธีการ Mapping RxNorm (ผู้ใหญ่)

| วิธี | แถว | % |
|------|----:|--:|
| `basename` (จับคู่ชื่อยาโดยตรง) | 15,926,893 | 88.6% |
| `ingredients` (จับคู่ส่วนประกอบ) | ~1,765,000 | ~9.8% |
| `approx:…` / `kegg:…` / fallback อื่น | ส่วนที่เหลือ | ~1.6% |

### การกระจาย `ing_source` (S08 enriched)

| ค่า `ing_source` | ผู้ใหญ่ | เด็ก/วัยรุ่น | ความหมาย |
|-----------------|-------:|-------------:|-----------|
| `faers` | 81,052 | 21,547 | ดึงจาก FAERS active substance |
| `llm` | 3,058 | 607 | ดึงด้วย LLM |
| `rxnorm_enriched` | 478 | 136 | เติมจาก RxNorm (approx/KEGG/CID/ChEMBL) |
| `rxnav_ingredients` | 354 | 65 | เติมจาก RxNav ผ่าน ingredient |
| `rxnav_basename` | 325 | 82 | เติมจาก RxNav ผ่าน basename |
| **null** | **0** | **0** | ไม่พบ (ทุกแถวมีค่า) |

### ข้อมูล Quarantine จาก S08

| สาเหตุ | ผู้ใหญ่ | เด็ก/วัยรุ่น |
|--------|-------:|-------------:|
| `no_rxcui` (map ไม่ได้) | 4,576 | 1,141 |
| `suspicious_name` (ชื่อน่าสงสัย) | 1,575 | 594 |
| `no_rxcui_and_suspicious` (ทั้งสอง) | 70 | 12 |

---

## 5. โครงสร้างไฟล์เอาต์พุต

### `patient_report_reporter_drug_reaction_full_data.parquet`

หนึ่งแถว = หนึ่งชุด **(รายงาน × ยา × MedDRA PT)** ที่ไม่ซ้ำ

| คอลัมน์ | ประเภท | หมายเหตุ |
|---------|--------|---------|
| `safetyreportid` | str | รหัสรายงาน FAERS (ตัวเลขล้วน) |
| `age_years` | i32 | อายุเป็นปีเต็ม |
| `patient_sex` | str | Male / Female / Unknown |
| `nichd` | str | เฉพาะกลุ่มเด็ก: กลุ่ม NICHD |
| `receive_date` | Date | วันที่รับรายงานครั้งแรก |
| `mostrecent_receive_date` | Date | วันที่รับรายงานล่าสุด |
| `lastupdate_date` | Date | วันที่อัปเดตล่าสุด |
| `serious` | int | 1 ถ้า Flag ความรุนแรงใดๆ = 1 |
| `congenital_anomali` | float | Flag ความพิการแต่กำเนิด |
| `death` | float | Flag เสียชีวิต |
| `disabling` | float | Flag พิการ |
| `hospitalization` | float | Flag นอนโรงพยาบาล |
| `life_threatening` | float | Flag เป็นอันตรายต่อชีวิต |
| `other` | float | Flag ความรุนแรงอื่นๆ |
| `reporter_country` | str | รหัสประเทศ ISO |
| `reporter_company` | str | บริษัทที่รายงาน |
| `reporter_qualification` | str | Physician / Pharmacist / Other health professional |
| `medicinal_product` | str | ชื่อยาดิบจาก FAERS (ตัวพิมพ์ใหญ่) |
| `rxcui` | str | RxNorm Concept Identifier |
| `mapping_method` | str | วิธีที่ resolve RxCUI ได้ (basename / ingredients / approx:… / kegg:…) |
| `ingredient` | str | ชื่อส่วนประกอบยามาตรฐานจาก RxNorm; หลายตัวคั่นด้วย ` / ` |
| `drug_characterization` | str | เสมอ: "Suspect…" |
| `drug_administration` | str | เส้นทางการให้ยา |
| `drug_indication` | str | ข้อบ่งชี้ที่รายงาน |
| `reaction_meddrapt` | str | MedDRA Preferred Term (Title Case) |
| `reaction_outcome` | str | Recovered / Fatal / Not recovered / Unknown |
| `meddra_concept_id` | i64 | OMOP concept ID สำหรับ PT |
| `meddra_concept_code` | str | MedDRA PT code |
| `meddra_soc_codes` | list[str] | รหัส System Organ Class (อาจมีหลายค่า) |
| `meddra_soc_names` | list[str] | ชื่อ System Organ Class (อาจมีหลายค่า) |

### `drug_full_data.parquet`

หนึ่งแถว = หนึ่ง **ingredient** ที่ไม่ซ้ำ

| คอลัมน์ | ประเภท |
|---------|--------|
| `ingredient` | str |
| `medicinal_product` | str |
| `rxcui` | str |
| `mapping_method` | str |

### `adr_full_data.parquet` / `standard_reaction_full_data.parquet`

หนึ่งแถว = หนึ่งชุด **(safetyreportid, reaction_meddrapt)** ที่ไม่ซ้ำ

| คอลัมน์ | ประเภท |
|---------|--------|
| `safetyreportid` | str |
| `reaction_meddrapt` | str |
| `reaction_outcome` | str |
| `meddra_concept_id` | i64 |
| `meddra_concept_code` | str |
| `meddra_soc_names` | list[str] |
| `meddra_soc_codes` | list[str] |

---

## 6. ข้อจำกัดและข้อสังเกต

1. **รายงานที่มียาหลายรายการ:** รายงาน FAERS บางฉบับอาจมียาและปฏิกิริยาหลายสิบรายการ Fact table เก็บหนึ่งแถวต่อ (ยา, ปฏิกิริยา) ต่อรายงาน — รายงานที่มียา n ตัว และ PT m ตัว จะผลิต `n × m` แถว นี่เป็นพฤติกรรมที่ถูกต้องตามโครงสร้าง FAERS ไม่ใช่ข้อมูลซ้ำ

2. **`ingredient` หลายส่วนประกอบ:** เมื่อฟิลด์ `ingredient` มีชื่อหลายตัวคั่นด้วย ` / ` สะท้อนชุดส่วนประกอบที่ดีที่สุดของชื่อ Brand นั้นจาก RxNorm ซึ่งผลิตโดย logic MIN-combined name ใน S09

3. **`mapping_method` มีค่าหลากหลาย:** S08 ผลิต `mapping_method` มากกว่า 2,000 ค่าที่แตกต่างกัน (ส่วนใหญ่เป็น `approx:<ชื่อ>` และ `kegg:<id>:<inn>`) ผู้ใช้สามารถจัดกลุ่มตาม prefix (basename, ingredients, approx, kegg ฯลฯ) เพื่อแบ่งชั้นคุณภาพ mapping

4. **ยาใน Quarantine:** ประมาณ 6,200 แถวผู้ใหญ่ และ 1,750 แถวเด็ก ถูกส่งไป quarantine ใน S08 (map RxNorm ไม่ได้ หรือระบุว่าเป็นชื่อ Placeholder) บันทึกไว้ที่ `data/staging/s08_enrich_drug_identifiers/quarantine/` สำหรับ review ภายหลัง ไม่รวมในไฟล์ส่งมอบ

5. **ความครอบคลุม MedDRA:** ทุกแถวใน Fact table ถูกจับคู่กับ MedDRA PT (บังคับด้วย inner join ใน S09) มีเพียง 1,640 PT ที่ไม่ซ้ำใน cohort ผู้ใหญ่ และ 453,039 คู่ (รายงาน, PT) ที่ไม่ซ้ำ

6. **Provenance ของ `ing_source`:** คอลัมน์ `ing_source` ใน staging S08 บอกว่าฟิลด์ `ingredients` ได้มาอย่างไร (`faers` = จาก FAERS active_substance, `llm` = LLM, `bracket` = วงเล็บ Fallback, `rxnav_*`/`rxnorm_enriched` = เติมจาก RxNorm path ใน S08) คอลัมน์นี้ไม่ได้ถ่ายทอดไปยังไฟล์เอาต์พุตสุดท้าย (S09/S10)
