# Drug-Ready Structured Data Schema (S01–S03)

สรุป outputs ใหม่และ validation สำหรับ drug-level mapping pipeline

---

## 1. S01 Outputs (unchanged, with additions)

### patient_drug/*.csv.gzip
- **Key**: `(safetyreportid, entry)` — 1 row per drug per report
- **New column**: `activesubstance` — raw JSON string of nested activesubstance (for S02 parsing)
- **Existing**: `activesubstance_name` — flattened substance name
- **Validation**: Duplicate check of `(safetyreportid, entry)` per file; warning if duplicates found

### patient_drug_openfda/*.csv.gzip
- **Key**: `(safetyreportid, entry, key)` — long format; multiple rows per drug expected
- **Validation**: Schema preserved; `(safetyreportid, entry)` correctly links to patient_drug

---

## 2. S02 New Outputs

### drugcharacteristics_extended.parquet
- **Key**: `(safetyreportid, entry)`
- **Columns**: medicinal_product, drug_characterization, drug_administration, drug_indication, drug_dosage_form, drug_authorization_number, drug_batch_number, drug_structured_dosage_*, drug_treatment_duration*, action_drug, **activesubstance_raw**, **active_substance_name**
- **Note**: activesubstance_raw = raw from S01; active_substance_name = parsed (coalesce S01 activesubstance_name or regex from activesubstance_raw)

### drug_openfda_wide.parquet
- **Key**: `(safetyreportid, entry)`
- **Columns**: safetyreportid, entry, openfda_rxcui, openfda_generic_name, openfda_brand_name, openfda_substance_name, openfda_product_ndc, openfda_package_ndc, openfda_application_number, openfda_manufacturer_name, openfda_route, openfda_spl_set_id, openfda_nui, openfda_pharm_class_*, openfda_product_type, openfda_unii
- **Type**: List[Utf8] for openfda_* columns (multiple values per key)

### drug_mapping_input.parquet
- **Key**: `(safetyreportid, entry)`
- **Merge**: drugcharacteristics_extended LEFT JOIN drug_openfda_wide
- **Columns**: All from extended + all openfda_* columns

### drug_mapping_input_unique.parquet
- **Key**: `mapping_record_id` (1-based sequential)
- **Dedup key**: medicinal_product, drug_administration, drug_dosage_form, active_substance_name, drug_authorization_number, openfda_generic_name (first), openfda_brand_name (first), openfda_substance_name (first), openfda_application_number (first)
- **Extra**: source_record_count = number of (safetyreportid, entry) rows collapsed

---

## 3. S03 New Outputs

### adult_drug_mapping_input.parquet
- **Key**: `(safetyreportid, entry)`
- **Filter**: Reports with patient_custom_master_age > 21
- **Enrichment**: age_years, reporter_qualification from adult_events_full_data

### pediatric_drug_mapping_input.parquet
- **Key**: `(safetyreportid, entry)`
- **Filter**: Reports with 0 ≤ patient_custom_master_age ≤ 21
- **Enrichment**: age_years, nichd, reporter_qualification from pediatric_events_full_data

---

## 4. Validation

### S01
- Duplicate `(safetyreportid, entry)` in patient_drug per file → warning

### S02
- **Row count** per output (in manifest)
- **Duplicate check**: `(safetyreportid, entry)` in drugcharacteristics_extended, drug_openfda_wide, drug_mapping_input
- **Non-null coverage %** for: medicinal_product, active_substance_name, openfda_rxcui, openfda_generic_name, openfda_substance_name

### S03
- Same validation for adult_drug_mapping_input, pediatric_drug_mapping_input
- Results in manifest `validation` key

---

## 5. Downstream Considerations

1. **openfda_* columns are List[Utf8]**: Use `.list.first()` or `.list.get(0)` for single-value mapping; iterate for multi-value.
2. **drug_mapping_input_unique**: Use for mapping jobs (LLM/RxNav) to avoid re-processing identical strings; join back to drug_mapping_input via dedup key if needed.
3. **active_substance_name**: May be null if activesubstance_raw parse fails; fallback to medicinal_product for mapping.
4. **Cohort outputs**: adult/pediatric drug_mapping_input are filtered by age; ensure S03 has run after S02 for these to exist.
