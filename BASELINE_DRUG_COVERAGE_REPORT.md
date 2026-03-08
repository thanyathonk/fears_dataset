# Baseline Drug Coverage Report — full_dataset Pipeline

**Purpose:** Document baseline drug mapping coverage for comparison with Aioli.  
**Data source:** Actual files in project (no assumptions).  
**Date:** 2025-03-07

---

## 1. Files Used

| File | Path | Status | Purpose |
|------|------|--------|---------|
| adult_events_full_data.parquet | `data/staging/s03_join_partition_age/` | ✅ Present | Record-level drug rows (Adult) |
| pediatric_events_full_data.parquet | `data/staging/s03_join_partition_age/` | ✅ Present | Record-level drug rows (Pediatric) |
| adult_drugs_clean_full_data.parquet | `data/staging/s07b_llm_clean/` | ✅ Present | Unique mapping candidates (Adult) |
| pediatric_drugs_clean_full_data.parquet | `data/staging/s07b_llm_clean/` | ✅ Present | Unique mapping candidates (Pediatric) |
| adult_drugs_enriched_final_full_data.parquet | `data/staging/s08_enrich_drug_identifiers/` | ✅ Present | Final mapping with rxcui/ingredients (Adult) |
| pediatric_drugs_enriched_final_full_data.parquet | `data/staging/s08_enrich_drug_identifiers/` | ✅ Present | Final mapping with rxcui/ingredients (Pediatric) |

### Files Not Present (Pipeline Design)

| File | Expected Path | Note |
|------|---------------|------|
| drug_mapping_input.parquet | `data/staging/s02_entity_format/` | Requires drug_openfda_wide; not built in current run |
| drug_mapping_input_unique.parquet | `data/staging/s02_entity_format/` | Depends on drug_mapping_input |
| adult_drug_mapping_input.parquet | `data/staging/s03_join_partition_age/` | Depends on drug_mapping_input |
| pediatric_drug_mapping_input.parquet | `data/staging/s03_join_partition_age/` | Depends on drug_mapping_input |

**Current flow:** S03 events → S07 unique drugs → S07b clean → S08 enrich. Coverage is computed from events + enriched outputs.

---

## 2. Coverage Definitions

| Level | Definition | Numerator | Denominator |
|-------|-------------|------------|-------------|
| **Record-level** | Drug rows (report × drug × reaction) | Rows with valid `medicinal_product` | Total drug rows |
| **Record-level (mapped)** | Drug rows that map to RxCUI | Rows whose `medicinal_product` has rxcui in enriched | Total drug rows |
| **Unique mapping record-level** | Distinct drug names sent to mapping | Unique `medicinal_product` in S07b | Same as unique medicinal_product |
| **Unique medicinal_product-level** | Distinct drug names | Unique `medicinal_product` in events | — |
| **Input field coverage** | Records with non-empty medicinal_product | Rows with non-null, non-empty `medicinal_product` | Total rows |
| **Final mapping coverage** | Unique drugs with RxCUI | Enriched rows with valid `rxcui` | Total unique drugs in enriched |

---

## 3. Results Tables

### 3.1 Input Field Coverage

| Cohort | Total Records | With medicinal_product | Input Coverage |
|--------|---------------|-------------------------|----------------|
| Adult | 114,144,307 | 114,144,307 | **100%** |
| Pediatric | 6,348,035 | 6,348,035 | **100%** |

### 3.2 Record-Level Mapping Coverage

| Cohort | Total Records | Records Mapped (rxcui) | Record-Level Coverage |
|--------|---------------|------------------------|-----------------------|
| Adult | 114,144,307 | 76,166,249 | **66.73%** |
| Pediatric | 6,348,035 | 4,041,412 | **63.66%** |

### 3.3 Unique Mapping Record / Medicinal Product Coverage

| Cohort | Unique medicinal_products | With RxCUI | Unique-Level Coverage |
|--------|---------------------------|------------|------------------------|
| Adult | 305,838 | 54,878 | **17.94%** |
| Pediatric | 47,739 | 17,689 | **37.05%** |

### 3.4 Ingredient Coverage (Final Mapping)

| Cohort | Total Unique | With ingredient_count > 0 | With RxCUI |
|--------|--------------|---------------------------|------------|
| Adult | 305,838 | 54,825 | 54,878 |
| Pediatric | 47,739 | 17,670 | 17,689 |

---

## 4. Top Unmapped medicinal_product (by Record Count)

### Adult — Top 30

| Record Count | medicinal_product |
|--------------|-------------------|
| 507,505 | PHTHALYLSULFATHIAZOLE |
| 318,248 | ERELZI |
| 110,898 | ADVAIR HFA |
| 107,663 | ZOFRAN |
| 100,396 | ACETAMINOPHEN AND CODEINE |
| 59,318 | MYOCHRYSINE |
| 58,511 | KARDEGIC |
| 58,393 | DUODOPA |
| 57,672 | PANTOPRAZOLE MAGNESIUM |
| 50,286 | SIRUKUMAB |
| 46,793 | MOVICOL |
| 45,518 | BREO ELLIPTA |
| 40,132 | MARVELON |
| 39,453 | MABTHERA |
| 39,211 | VITAMINS |
| 38,573 | UNSPECIFIED INGREDIENT |
| 36,128 | COTRIMOXAZOLE |
| 36,021 | ZENHALE |
| 35,220 | DESOGESTREL AND ETHINYL ESTRADIOL |
| 35,065 | MULTIVITAMIN |
| 34,475 | INSULIN NOS |
| 33,213 | DIETARY SUPPLEMENT |
| 32,268 | PIPERACILLIN AND TAZOBACTAM |
| 28,842 | VITAMINS NOS |
| 28,685 | HYDROXYCHLOROQUINE DIPHOSPHATE |
| 26,914 | TRELEGY ELLIPTA |
| 26,104 | FLUMETHASONE |
| 25,642 | COCODAMOL |
| 24,530 | SERETIDE |
| 24,366 | TORASEMID |

### Pediatric — Top 30

| Record Count | medicinal_product |
|--------------|-------------------|
| 9,927 | COTRIMOXAZOLE |
| 9,451 | ZOFRAN |
| 6,171 | THYMOCYTE IMMUNE GLOBULIN NOS |
| 4,984 | PIPERACILLIN AND TAZOBACTAM |
| 4,664 | TRIKAFTA |
| 4,351 | ERELZI |
| 3,745 | ADVAIR HFA |
| 3,194 | VITAMINS |
| 2,953 | INSULIN NOS |
| 2,194 | IMMUNE GLOBULIN NOS |
| 2,107 | UNSPECIFIED INGREDIENT |
| 2,011 | EMLA |
| 1,973 | MYOZYME |
| 1,869 | SERETIDE |
| 1,861 | VITAMINS NOS |
| 1,831 | GAMMAGARD LIQUID |
| 1,778 | TECELEUKIN |
| 1,773 | YAZ |
| 1,713 | MABTHERA |
| 1,676 | MOVICOL |
| 1,652 | GRANULOCYTE COLONY STIMULATING FACTOR |
| 1,633 | CANNABIS |
| 1,532 | BAKTAR |
| 1,464 | SEPTRA |
| 1,449 | MULTIVITAMIN |
| 1,441 | SYMDEKO |
| 1,380 | COCODAMOL |
| 1,359 | ZENHALE |
| 1,311 | ALBUREX |
| 1,251 | ONCOVIN |

---

## 5. Main Gaps

1. **Unique medicinal_product coverage low (Adult 17.9%)**  
   - 250,960 adult unique drug names unmapped  
   - 30,050 pediatric unique drug names unmapped  

2. **High-impact unmapped products**
   - Brand names: ERELZI, ADVAIR HFA, ZOFRAN, MABTHERA, SERETIDE, BREO ELLIPTA, TRELEGY ELLIPTA, ZENHALE  
   - Combination products: ACETAMINOPHEN AND CODEINE, PIPERACILLIN AND TAZOBACTAM, COTRIMOXAZOLE, DESOGESTREL AND ETHINYL ESTRADIOL  
   - Generics with salt/form: PANTOPRAZOLE MAGNESIUM, HYDROXYCHLOROQUINE DIPHOSPHATE  
   - NOS/unspecified: INSULIN NOS, VITAMINS, VITAMINS NOS, UNSPECIFIED INGREDIENT, DIETARY SUPPLEMENT  
   - Typos: SIRUKUMAB (likely SIRUKUMAB), SUNITINAB MALATE (SUNITINIB)  
   - Non-US / regional: KARDEGIC, DUODOPA, MOVICOL, COCODAMOL  

3. **Record-level vs unique-level**
   - Record-level ~66% (popular drugs map well)  
   - Unique-level ~18–37% (long tail of rare names unmapped)  

4. **Input field coverage**
   - 100%: all records have non-empty `medicinal_product`  

---

## 6. Ready-to-Present Summary

| Metric | Adult | Pediatric |
|--------|-------|-----------|
| Total drug records | 114.1M | 6.3M |
| Input field coverage | 100% | 100% |
| Record-level mapping coverage | **66.7%** | **63.7%** |
| Unique medicinal_products | 305,838 | 47,739 |
| Unique mapped (RxCUI) | 54,878 | 17,689 |
| Unique-level mapping coverage | **17.9%** | **37.1%** |
| Unmapped unique products | 250,960 | 30,050 |

**Takeaway:** Input is complete; mapping coverage is moderate at record level (~65%) but low at unique product level (18–37%). High-volume unmapped items include brand names, combinations, NOS terms, and non-US names. Aioli comparison should focus on improving unique-level coverage and these high-impact gaps.
