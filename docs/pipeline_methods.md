# FEARS Dataset — Pipeline Methods & Dataset Overview

> **Last updated:** 2026-03-31  
> **Data source:** FDA Adverse Event Reporting System (FAERS) via openFDA API  
> **Coverage:** Q1 2014 – Q4 2025  
> **Cohorts:** Adult (age > 21) · Pediatric (age 0–21, NICHD bands)

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Stage-by-Stage Methods](#2-stage-by-stage-methods)
   - [S01 – Fetch openFDA](#s01--fetch-openfda)
   - [S02 – Entity Formatting](#s02--entity-formatting)
   - [S03 – Join & Age Partition](#s03--join--age-partition)
   - [S05 – ADR Split](#s05--adr-split)
   - [S06 / S05b – MedDRA Mapping](#s06--s06b--meddra-mapping)
   - [S07 – Drug List Collapse](#s07--drug-list-collapse)
   - [S07b – LLM Drug Decomposition](#s07b--llm-drug-decomposition)
   - [S08 – RxNorm Enrichment](#s08--rxnorm-enrichment)
   - [S09 – Final Merge & Dedup](#s09--final-merge--dedup)
   - [S10 – Package Deliverables](#s10--package-deliverables)
3. [Quality Filters Summary](#3-quality-filters-summary)
4. [Dataset Overview](#4-dataset-overview)
5. [Output Schema](#5-output-schema)
6. [Known Limitations & Notes](#6-known-limitations--notes)

---

## 1. Pipeline Overview

```
openFDA API
    │
    ▼
S01  Fetch raw drug-event JSON → flatten → CSV shards
    │
    ▼
S02  Normalize entity tables → Parquet (patient, report, drug, reaction, reporter)
    │
    ▼
S03  Inner join all entities · filter Suspect-only + qualified reporters
     · partition by age → adult_events / pediatric_events (2014–2025)
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
S04  Extract unique ADR rows         S06  Collapse to unique medicinal_product rows
     (safetyreportid, PT, outcome)         with list-valued context columns
    │                                      │
    ▼                                      ▼
S06/S05b  Map PT → SOC via        S06b  LLM (Qwen) decomposition:
          MedDRA OMOP vocab               ingredients · strength · dosage_form
          → pt_soc_dictionary             ing_source = faers | llm | bracket
                                           │
                                           ▼
                                    S07  RxNorm enrichment (7-step cascade):
                                          basename → ingredient → LocalCID
                                          → suffix-strip → ChEMBL → approx → KEGG
                                          + coalesce ing_source (rxnav_basename etc.)
                                          + quarantine unmapped / suspicious rows
    │                                      │
    └──────────────┬───────────────────────┘
                   ▼
S08  Three-way inner join: clean events × enriched drug × PT-SOC dictionary
     dedup on (safetyreportid, medicinal_product, reaction_meddrapt)
     → patient_report_reporter_drug_reaction_full_data.parquet
                   │
                   ▼
S09  Package dimension tables:
     drug_full_data · adr_full_data · standard_reaction_full_data
```

> **Note:** S04 was merged into S03. There is no separate S04 stage.

---

## 2. Stage-by-Stage Methods

### S01 – Fetch openFDA

**Script:** `src/stages/s01_fetch_openfda.py`

Downloads FAERS quarterly drug-event ZIP files from the openFDA API and flattens nested JSON into CSV shards. Key operations:

- Authenticated requests with `OPENFDA_API_KEY` (rate-limited, exponential backoff)
- Per-file extraction to `patient_drug/*.csv.gzip`, `patient/*.csv.gzip`, etc.
- Enum-code decoding (drug characterization, routes, outcomes) using FAERS codebooks
- `activesubstance` nested JSON parsed and flattened to `activesubstance_name`
- `patient_drug_openfda/*.csv.gzip` written in long format (one row per openfda key-value per drug)
- Duplicate check on `(safetyreportid, entry)` per shard file

**Outputs:** `data/raw/patient_drug/`, `data/raw/patient/`, `data/raw/reporter/`, `data/raw/reactions/`, etc.

---

### S02 – Entity Formatting

**Script:** `src/stages/s02_entity_format_stream.py`

Reads S01 CSV shards and produces normalized ER-style Parquet tables using Polars streaming to avoid large in-memory pandas concatenations. Key tables produced:

| Table | Key | Description |
|-------|-----|-------------|
| `patient.parquet` | `safetyreportid` | Age, sex, weight; `patient_custom_master_age` in whole years |
| `report.parquet` | `safetyreportid` | Receive/update dates |
| `report_serious.parquet` | `safetyreportid` | Seriousness flags (death, hospitalization, …) |
| `reporter.parquet` | `safetyreportid` | Qualification, country, company |
| `reactions.parquet` | `safetyreportid` | `reaction_meddrapt`, `reaction_outcome` |
| `drugcharacteristics_extended.parquet` | `(safetyreportid, entry)` | All drug-level fields + parsed active substance |
| `drug_openfda_wide.parquet` | `(safetyreportid, entry)` | openfda_* list columns (rxcui, generic/brand names, …) |
| `drug_mapping_input.parquet` | `(safetyreportid, entry)` | LEFT JOIN of extended + openfda_wide |

Age is converted from FAERS raw units (years/months/days/weeks) to whole years (`np.floor`).  
High-memory path available via `S02_OPENFDA_HIGH_MEMORY=1` env variable.

---

### S03 – Join & Age Partition

**Script:** `src/stages/s03_join_partition_age.py`

Assembles the master event table by inner-joining all entity tables on `safetyreportid`, applies early quality filters, then splits into cohorts.

**Quality filters applied here:**

| Filter | Criteria |
|--------|----------|
| Date range | `2014-01-01 ≤ timeline_key ≤ 2025-12-31` (waterfall: mostrecent → lastupdate → receive) |
| Valid IDs | `safetyreportid` matches `^\d+$` (exclude composite keys like `4816137-9`) |
| Non-null reaction | `reaction_meddrapt IS NOT NULL` |
| Non-null drug | `medicinal_product IS NOT NULL AND len > 0` |
| Suspect drugs only | `drug_characterization` starts with `"Suspect"` — excludes Concomitant |
| Qualified reporters | Exclude: `Unknown`, `Lawyer`, `Consumer or non-health professional` |

**Age partition:**

| Cohort | `age_years` range | Extra columns |
|--------|-------------------|---------------|
| Pediatric | 0–21 | `nichd` band (infancy/toddler/early_childhood/middle_childhood/early_adolescence/late_adolescence) |
| Adult | 22–120 | — |

NICHD band mapping:
- infancy: 0 (< 1 yr)
- toddler: 1 yr
- early_childhood: 2–5 yr
- middle_childhood: 6–11 yr
- early_adolescence: 12–17 yr
- late_adolescence: 18–21 yr

**Outputs:** `data/staging/s03_join_partition_age/{cohort}_events_full_data.parquet`

---

### S04 – ADR Split

**Script:** `src/stages/s04_split_adr.py`

Extracts unique `(safetyreportid, reaction_meddrapt, reaction_outcome)` tuples from S03 events. Applies `.drop_nulls(["reaction_meddrapt"]).unique()` to prevent duplicate reaction lines before MedDRA mapping.

**Outputs:** `data/staging/s04_split_adr/{cohort}_adr_full_data.parquet`

---

### S05 / S05b – MedDRA Mapping

**Scripts:** `src/stages/s05_map_omop_meddra.py` · `src/stages/s05b_map_omop_meddra_full_hierarchy.py`

Maps each unique `reaction_meddrapt` (Preferred Term, PT) to the MedDRA hierarchy using the OMOP vocabulary (`vocabulary_SNOMED_MEDDRA_RxNorm_ATC`).

**S06:** Uses `CONCEPT_ANCESTOR` for PT→SOC shortcut mapping. Produces `pt_soc_dictionary_full_data.parquet` (used by S09).

**S05b:** Uses `CONCEPT_RELATIONSHIP` to build the full PT→HLT→HLGT→SOC hierarchy. Produces:
- `pt_hierarchy_dictionary_full_data.parquet` — PT-centric with aggregated level lists
- `pt_hierarchy_paths_full_data.parquet` — expanded one-row-per-path format

**Excluded SOCs** (not related to direct drug effects):
- Surgical and medical procedures
- Social circumstances
- Product issues

Text matching uses Title Case normalization for compatibility with MedDRA standards.

**Outputs:** `data/staging/s05_map_omop_meddra/{cohort}/pt_soc_dictionary_full_data.parquet`

---

### S06 – Drug List Collapse

**Script:** `src/stages/s06_split_drug.py`

Collapses the event table to one row per unique `medicinal_product`, collecting context columns as sorted, deduplicated lists (via `implode().list.unique().list.sort()`):

- `active_substance_faers` — raw FAERS active substance names (list)
- `drug_dosage_form` — list of dosage forms seen
- `drug_authorization_number` — regulatory IDs
- `action_drug` — action taken on drug

Drug names are normalized by `normalize_faers_drug_name()` before aggregation to reduce near-duplicate variants.

**Outputs:** `data/staging/s06_split_drug/{cohort}_drugs_full_data.parquet`

---

### S06b – LLM Drug Decomposition

**Script:** `src/stages/s06b_llm_clean.py` (in-process) · `scripts/s07_openai_run.py` (batch OpenAI)

Decomposes each drug name string into structured pharmaceutical fields using a large language model (default: `Qwen/Qwen2.5-32B-Instruct` or OpenAI GPT-4 equivalent).

**Extraction priority:**

1. **`faers`** — `active_substance_faers` already contains ingredient names → use directly (no LLM needed)
2. **`llm`** — LLM extracts ingredients, strength, dosage_form, qualifier from drug name text
3. **`bracket`** — fallback when neither FAERS nor LLM yields ingredients: extract from parenthetical patterns in the drug name string

The source of ingredients is recorded in **`ing_source`** (`faers` | `llm` | `bracket` | `null`).  
When `ing_source` is `null`, it means the LLM returned no parseable ingredient list and FAERS substance was absent.

Fields produced per row:

| Column | Type | Source |
|--------|------|--------|
| `medicinal_product` | str | raw key |
| `basename` | str | regex-cleaned + LLM-derived core name |
| `ingredients` | str/list | extracted ingredient names |
| `salt` | list | detected salt/counter-ion forms |
| `strength` | str | dosage strength (e.g., `"10MG"`) |
| `dosage_form` | str | tablet / capsule / injection / … |
| `qualifier` | str | non-drug qualifier (country, brand variant) |
| `qualifier_type` | str | `COUNTRY` / `BRAND` / … |
| `ing_source` | str | `faers` / `llm` / `bracket` / `null` |

**Outputs:** `data/staging/s06b_llm_clean/{cohort}_drugs_llm_cleaned.parquet`

---

### S07 – RxNorm Enrichment

**Script:** `src/stages/s07_enrich_drug_identifiers_local.py`

Resolves each unique drug `basename` (from S07b) to a standard **RxCUI** (RxNorm concept identifier) using a 7-step cascade lookup, then enriches with `rxnorm_ingredients` list.

**Lookup cascade (by priority):**

| Step | Method | `lookup_hit` value |
|------|--------|-------------------|
| 1 | RxNav API exact match on `basename` | `basename` |
| 2 | RxNav API exact match on each `ingredient` | `ingredients` |
| 3 | LocalCID offline (SQLite) → canonical title → RxNav | canonical title string |
| 4 | Pharmaceutical suffix stripping (remove noise: SULFATE, TABLET, TEVA, etc.) → RxNav retry | `suffix_strip:<stripped>` |
| 5 | ChEMBL brand lookup → `pref_name` → RxNav (strict exact-synonym match only) | pref_name string |
| 6 | RxNav approximate match (score ≥ 8.0, similarity ≥ 0.70, first-letter guard for single-word) | `approx:<matched>` |
| 7 | KEGG Drug (non-US brands) → INN → RxNav | `kegg:<id>:<inn>` |

After all lookups, **`ing_source` is coalesced** for rows that were still `null`:
- `rxnav_basename` — mapped via basename exact match
- `rxnav_ingredients` — mapped via ingredient exact match
- `rxnorm_enriched` — mapped via approx / KEGG / CID / ChEMBL / suffix-strip

#### Quarantine split

After enrichment, rows are split into:

| Destination | Condition | `s07_quarantine_reason` |
|-------------|-----------|------------------------|
| **main** `*_drugs_enriched.parquet` | has `rxcui` and not suspicious | — |
| **quarantine** `quarantine/*_drugs_quarantine.parquet` | `rxcui IS NULL` | `no_rxcui` |
| | suspicious name heuristic (e.g., `(UNKNOWN)`, `CHINESE HERBAL MEDICINES`, `DIET AID`, `DRUG UNKNOWN`) | `suspicious_name` |
| | both | `no_rxcui_and_suspicious` |

Set `S08_QUARANTINE_ONLY_UNMAPPED=1` to restrict quarantine to unmapped-only.

**Outputs:**
- `data/staging/s07_enrich_drug_identifiers/{cohort}_drugs_enriched.parquet`
- `data/staging/s07_enrich_drug_identifiers/quarantine/{cohort}_drugs_quarantine.parquet`

---

### S08 – Final Merge & Dedup

**Script:** `src/stages/s08_finalize_merge_and_report.py`

Performs the three-way inner join to produce the final analysis-ready fact table:

```
clean events (S03)
    × enriched drug lookup (S08)   — join on medicinal_product; 1 row per drug (prevents Cartesian explosion)
    × PT-SOC dictionary (S06)      — join on reaction_meddrapt (Title Case normalized)
```

**Join guarantee:** The enriched drug table is pre-deduplicated to `.unique(subset=["medicinal_product"], keep="first")` before the join, ensuring no fan-out from multiple RxCUI candidates per drug name.

**Post-join processing:**
- `reaction_meddrapt` normalized to Title Case before dictionary join
- Seriousness flags consolidated: `GREATEST(death, hospitalization, …)` → `serious`
- Date columns: `receive_date`, `mostrecent_receive_date`, `lastupdate_date` retained as-is (three separate columns)
- Unknown-fill for: `patient_sex`, `reaction_outcome`, `drug_administration`, `drug_indication`, `reporter_country`, `reporter_company`, `reporter_qualification`

**Deduplication:** Final `.unique(subset=["safetyreportid", "medicinal_product", "reaction_meddrapt"], keep="first")` ensures each (report, drug, reaction) combination appears exactly once.

Processing uses Polars batched streaming to stay within memory limits on large adult cohort (~100M input rows → ~18M output rows).

**Outputs:** `data/output/{Adult,Pediatric}/patient_report_reporter_drug_reaction_full_data.parquet`

---

### S09 – Package Deliverables

**Script:** `src/stages/s09_package_deliverables.py`

Derives three additional deliverable tables from the S08 fact table (no new joins):

| Table | Key | Logic |
|-------|-----|-------|
| `drug_full_data.parquet` | `ingredient` | Group by `ingredient`, take `.first()` for `medicinal_product`, `rxcui`, `mapping_method` |
| `adr_full_data.parquet` | `(safetyreportid, reaction_meddrapt)` | Unique combinations with MedDRA codes and SOC lists |
| `standard_reaction_full_data.parquet` | `(safetyreportid, reaction_meddrapt)` | Same as `adr_full_data` (alternative naming) |

All tables are written with ZSTD compression to `data/output/{Adult,Pediatric}/`.

---

## 3. Quality Filters Summary

| Filter | Stage | Criteria |
|--------|-------|----------|
| Date range | S03 | 2014-01-01 to 2025-12-31 |
| Valid safetyreportid | S03 | Digits-only (`^\d+$`) |
| Non-null reaction | S03 | `reaction_meddrapt IS NOT NULL` |
| Suspect drugs only | S03 | `drug_characterization` starts with `"Suspect"` |
| Qualified reporter | S03 | Exclude Unknown / Lawyer / Consumer |
| Valid age | S03 | Age 0–120; null ages dropped |
| Valid drug name | S03 | `medicinal_product IS NOT NULL AND len > 0` |
| MedDRA coverage | S08 | Inner join → only PT terms present in MedDRA dictionary |
| RxNorm coverage | S08 | Inner join → only drugs with resolved `rxcui` |
| Deduplication | S08 | Unique on `(safetyreportid, medicinal_product, reaction_meddrapt)` |
| Quarantine unmapped | S07 | Rows with `rxcui IS NULL` or suspicious name → separate file |

---

## 4. Dataset Overview

All figures from pipeline run **20260329T223533** (completed 2026-03-30).

### Row counts

| Table | Adult | Pediatric |
|-------|------:|----------:|
| Fact: `patient_report_reporter_drug_reaction_full_data` | **17,979,420** | **966,168** |
| ADR dimension: `adr_full_data` | 5,410,155 | 453,039 |
| Reaction dimension: `standard_reaction_full_data` | 5,410,155 | 453,039 |
| Drug dimension: `drug_full_data` | 4,944 | 2,881 |
| S07 enriched drugs (main) | 85,267 | 22,437 |
| S07 quarantine drugs | 6,221 | 1,747 |

### Unique entities (Adult fact)

| Entity | Count |
|--------|------:|
| Unique `safetyreportid` | ~2,539,839 |
| Unique `medicinal_product` | ~67,193 |
| Unique `ingredient` | ~4,944 |
| Unique `reaction_meddrapt` (PT) | ~1,640 |
| Unique RxCUI | ~8,007 |

### Age distribution

| Cohort | Min age | Max age | Null age |
|--------|--------:|--------:|---------:|
| Adult | 22 yr | 120 yr | 0 |
| Pediatric | 0 yr | 21 yr | 0 |

### NICHD bands (Pediatric)

| Band | Age range |
|------|-----------|
| infancy | < 1 yr |
| toddler | 1 yr |
| early_childhood | 2–5 yr |
| middle_childhood | 6–11 yr |
| early_adolescence | 12–17 yr |
| late_adolescence | 18–21 yr |

### Drug characterization (all rows in fact)

All rows carry `drug_characterization = "Suspect (the drug was considered by the reporter to have caused or contributed to the event)"`. No Concomitant rows present.

### Reporter qualification (Adult)

| Qualification | Rows | % |
|---------------|-----:|--:|
| Other health professional | 11,041,092 | 61.4% |
| Physician | 5,864,404 | 32.6% |
| Pharmacist | 1,073,924 | 6.0% |

### RxNorm mapping method (Adult fact)

| Method | Rows | % |
|--------|-----:|--:|
| `basename` | 15,926,893 | 88.6% |
| `ingredients` | ~1,765,000 | ~9.8% |
| `approx:…` / `kegg:…` / other fallback | remainder | ~1.6% |

### ing_source distribution (S08 enriched)

| `ing_source` | Adult | Pediatric |
|--------------|------:|----------:|
| `faers` | 81,052 | 21,547 |
| `llm` | 3,058 | 607 |
| `rxnorm_enriched` | 478 | 136 |
| `rxnav_ingredients` | 354 | 65 |
| `rxnav_basename` | 325 | 82 |
| **null** | **0** | **0** |

### Quarantine breakdown (S08)

| Reason | Adult | Pediatric |
|--------|------:|----------:|
| `no_rxcui` | 4,576 | 1,141 |
| `suspicious_name` | 1,575 | 594 |
| `no_rxcui_and_suspicious` | 70 | 12 |

---

## 5. Output Schema

### `patient_report_reporter_drug_reaction_full_data.parquet`

One row = one unique **(report × drug × reaction PT)** combination.

| Column | Type | Notes |
|--------|------|-------|
| `safetyreportid` | str | FAERS report ID (digits only) |
| `age_years` | i32 | Whole years |
| `patient_sex` | str | Male / Female / Unknown |
| `nichd` | str | Pediatric only: NICHD developmental band |
| `receive_date` | Date | Initial receipt date |
| `mostrecent_receive_date` | Date | Most recent receipt |
| `lastupdate_date` | Date | Last update |
| `serious` | int | 1 if any seriousness flag = 1 |
| `congenital_anomali` | float | Seriousness sub-flag |
| `death` | float | Seriousness sub-flag |
| `disabling` | float | Seriousness sub-flag |
| `hospitalization` | float | Seriousness sub-flag |
| `life_threatening` | float | Seriousness sub-flag |
| `other` | float | Seriousness sub-flag |
| `reporter_country` | str | ISO country code |
| `reporter_company` | str | Reporting company |
| `reporter_qualification` | str | Physician / Pharmacist / Other health professional |
| `medicinal_product` | str | Raw FAERS drug name (uppercase) |
| `rxcui` | str | RxNorm CUI |
| `mapping_method` | str | How RxCUI was resolved (basename / ingredients / approx:… / kegg:… etc.) |
| `ingredient` | str | Standard ingredient name(s) from RxNorm; multi-ingredient joined with ` / ` |
| `drug_characterization` | str | Always: "Suspect…" |
| `drug_administration` | str | Route of administration |
| `drug_indication` | str | Reported indication |
| `reaction_meddrapt` | str | MedDRA Preferred Term (Title Case) |
| `reaction_outcome` | str | Recovered / Fatal / Not recovered / Unknown |
| `meddra_concept_id` | i64 | OMOP concept ID for PT |
| `meddra_concept_code` | str | MedDRA PT code |
| `meddra_soc_codes` | list[str] | System Organ Class code(s) |
| `meddra_soc_names` | list[str] | System Organ Class name(s) |

### `drug_full_data.parquet`

One row = one unique **ingredient**.

| Column | Type |
|--------|------|
| `ingredient` | str |
| `medicinal_product` | str |
| `rxcui` | str |
| `mapping_method` | str |

### `adr_full_data.parquet` / `standard_reaction_full_data.parquet`

One row = one unique **(safetyreportid, reaction_meddrapt)**.

| Column | Type |
|--------|------|
| `safetyreportid` | str |
| `reaction_meddrapt` | str |
| `reaction_outcome` | str |
| `meddra_concept_id` | i64 |
| `meddra_concept_code` | str |
| `meddra_soc_names` | list[str] |
| `meddra_soc_codes` | list[str] |

---

## 6. Known Limitations & Notes

1. **Many-drug reports:** A single FAERS report may list dozens of drugs and dozens of reactions. The fact table stores one row per (drug, reaction) pair within a report — reports with many drugs and many reactions produce `n_drug × n_reaction` rows. This is expected behavior from the FAERS data model, not duplication.

2. **Multi-ingredient `ingredient` strings:** When a drug's `ingredient` contains multiple substances (joined by ` / `), it reflects the best-fit RxNorm ingredient set for that brand name, produced by the MIN-combined name selection logic in S09.

3. **`mapping_method` diversity:** The S07 cascade produces ~2,000+ distinct `mapping_method` values (mostly `approx:<name>` and `kegg:<id>:<inn>` variants). Users can group by prefix (`basename`, `ingredients`, `approx`, `kegg`, etc.) for quality stratification.

4. **Quarantine drugs:** ~6,200 adult and ~1,750 pediatric drug name rows were quarantined in S07 (no RxCUI resolved or identified as non-drug placeholder). These are saved under `data/staging/s07_enrich_drug_identifiers/quarantine/` for manual review and are excluded from the deliverable files.

5. **MedDRA coverage:** 100% of rows in the output fact table are matched to a MedDRA PT (enforced by S08 inner join). Only 1,640 distinct PTs appear in the adult cohort and 453,039 unique (report, PT) ADR records.

6. **ing_source provenance:** The `ing_source` column in S07 staging traces how the `ingredients` field was populated (`faers` = from FAERS active_substance, `llm` = LLM extraction, `bracket` = parenthetical fallback, `rxnav_*`/`rxnorm_enriched` = backfilled by S07 RxNorm path). This column does not propagate to the final output files (S09/S10).
