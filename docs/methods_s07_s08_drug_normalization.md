# Drug Name Normalization and Identifier Enrichment: Methods for Stages S07b and S08

## Abstract

Drug records in the FDA Adverse Event Reporting System (FAERS) exhibit severe heterogeneity: brand names, generic names, multi-language variants, misspellings, co-formulations with dosage information embedded in a single free-text string, and international trade names that are absent from US drug databases. This document describes the methodology developed for two pipeline stages—**S07b** (LLM-based drug name decomposition and normalization) and **S08** (multi-tier drug identifier enrichment)—designed to address these challenges. Through iterative experimentation, we achieved RxCUI identification coverage of **80.3%** for the pediatric cohort (30,804 unique drug names) and **70.6%** for the adult cohort (165,850 unique drug names).

---

## 1. Problem Statement

Raw `medicinal_product` strings from FAERS contain:

1. **Embedded metadata** — dosage strengths (`"AMOXICILLIN 500MG CAPSULE"`), routes (`"IV"`), dosage forms (`"TABLET"`), and manufacturer names (`"METFORMIN TEVA 500MG"`) concatenated with the drug name.
2. **Brand-to-generic ambiguity** — names such as `"KIOVIG"` or `"HUMULINE"` that map to standard generic names (`IMMUNOGLOBULIN`, `INSULIN HUMAN`) but do not directly appear in standard drug databases.
3. **Typographic variation** — misspellings (`"FLUCANOZOLE"` → `"FLUCONAZOLE"`), truncations, and non-English orthographic conventions.
4. **Non-US international brands** — European, Japanese, and other regional trade names that are registered in regional pharmacopoeias but absent from RxNorm.
5. **Multi-ingredient formulations** — free-text strings listing multiple active ingredients simultaneously without consistent delimiters.

The core challenge is to extract a canonical **basename** (the primary drug identifier), separate **ingredients**, and map both to a standardized **RxCUI** (RxNorm Concept Unique Identifier) suitable for downstream drug safety analysis.

---

## 2. Stage S07b — LLM-Based Drug Name Decomposition

### 2.1 Overview

Stage S07b transforms raw `medicinal_product` strings into structured fields using a three-phase pipeline: (1) rule-based regex preprocessing, (2) large language model (LLM) inference, and (3) rule-based post-processing. The output for each record is a normalized row containing: `basename`, `ingredients` (list), `salt`, `strength`, `dosage_form`, `qualifier`, and `qualifier_type`.

### 2.2 Phase 1: Regex Preprocessing

Before LLM inference, each raw string is preprocessed to produce a cleaner input and to derive a deterministic `basename` without LLM involvement:

1. **Bracket extraction** — parenthetical expressions are extracted and stored separately (e.g., `"DENOSINE (JAPAN)"` → qualifier `"JAPAN"` with `qualifier_type = "COUNTRY"`).
2. **Special character normalization** — non-alphanumeric characters (except hyphens and slashes) are replaced with whitespace.
3. **Numeric dose pattern removal** — tokens matching `\d+(\.\d+)?\s?(MG|ML|MCG|G|IU|%)` or ratio patterns (`\d+/\d+`) are stripped.
4. **Pharmaceutical token filtering** — a curated set of salt forms (`HYDROCHLORIDE`, `HCL`, `MALEATE`, …), route descriptors (`ORAL`, `IV`, `INTRAVENOUS`, …), dosage form tokens (`TABLET`, `CAPSULE`, `SOLUTION`, …), company names (`PFIZER`, `NOVARTIS`, …), and prepositions (`FOR`, `WITH`, `AND`, …) are removed to isolate the core drug token(s).

The cleaned text is passed to `derive_basename()`, which applies the same filter rules deterministically to produce a reproducible basename even when LLM inference is unavailable.

### 2.3 Phase 2: LLM Decomposition (Qwen 2.5-32B)

A **Qwen/Qwen2.5-32B-Instruct** model is used in batched inference mode (`batch_size = 64`) to parse each (preprocessed) drug string into a JSON object with five structured keys: `ingredients`, `strength`, `dosage_form`, `qualifier`, and `qualifier_type`.

The system prompt enforces **strict string-level extraction**:

```
You are an AI system for pharmaceutical text decomposition.
STRICT STRING-LEVEL EXTRACTION ONLY.
Do NOT infer, normalize, guess, or enrich.
- Extract ingredients ONLY if explicitly written as chemicals.
- Salts (e.g., HCL, MALEATE) are ingredients but will be post-processed.
- Country names, brand names, regions are NOT ingredients.
```

This constraint prevents hallucination: if a brand name like `"TAVOR"` (a trade name for lorazepam) is given, the model returns `{"ingredients": null, …}` rather than guessing the generic.

**Few-shot examples** embedded in the user prompt include:
- `"TAVOR"` → `{ingredients: null, …}` (brand name, no explicit ingredient)
- `"DENOSINE (JAPAN)"` → `{qualifier: "JAPAN", qualifier_type: "COUNTRY", …}`
- `"PROPRANOLOL HCL 10MG TABLET"` → `{ingredients: ["PROPRANOLOL","HCL"], strength: "10MG", dosage_form: "TABLET", …}`

### 2.4 Phase 3: Post-Processing

After LLM inference, the raw JSON output is validated and cleaned:

1. **Salt separation** — the `split_ingredient_and_salt()` function separates active ingredients from pharmaceutical salt forms using a curated `SALTS` set (`HYDROCHLORIDE`, `SODIUM`, `PHOSPHATE`, etc.), which are stored in a separate `salt` column.
2. **Non-ingredient filtering** — tokens belonging to the `NON_INGREDIENT` set (`HUMAN`, `NORMAL`, `STERILE`, `WATER`, etc.) are removed from the ingredient list.
3. **Token cleanup** — FAERS-specific notation (`".R."`, numeric stereo descriptors like `"2S"`, `"4R"`) is stripped via `_clean_ingredient_token()`.
4. **LLM–regex fallback logic** — if the LLM returns a non-null `basename` it takes precedence; otherwise, the regex-derived `basename` is used as a reliable fallback.

### 2.5 Output Schema

| Column | Type | Description |
|---|---|---|
| `medicinal_product` | String | Original raw input (preserved, never modified) |
| `basename` | String | Primary drug name after normalization |
| `ingredients` | List[String] | Active ingredient(s) explicitly stated |
| `salt` | String | Salt/counter-ion form |
| `strength` | String | Dosage strength |
| `dosage_form` | String | Route/form of administration |
| `qualifier` | String | Additional qualifier (country, brand suffix, etc.) |
| `qualifier_type` | String | Type of qualifier (e.g., `"COUNTRY"`) |

---

## 3. Stage S08 — Multi-Tier Drug Identifier Enrichment

### 3.1 Overview

Stage S08 maps each unique `basename` to a **RxCUI** via a cascading seven-step lookup strategy. Steps are applied in order; once a RxCUI is found, the remaining steps are skipped. The final step (Step 8) fetches ingredient information for any resolved RxCUI. The enrichment is implemented as an asynchronous pipeline using `asyncio` and `aiohttp` to maximize throughput while respecting API rate limits.

### 3.2 Data Sources

| Source | Type | Purpose |
|---|---|---|
| **RxNav API** (NLM) | Remote REST API | Primary drug name ↔ RxCUI lookup (Steps 1–2, 6–7) |
| **CID-Synonym-filtered.db** | Local SQLite (offline) | Drug synonym → PubChem CID lookup (Step 3) |
| **CID-Title** | Local flat file (offline) | PubChem CID → canonical title (Step 3) |
| **ChEMBL API** (EBI) | Remote REST API | Verified brand name → generic INN lookup (Step 5) |
| **KEGG Drug API** (Kanehisa Lab) | Remote REST API | Non-US trade names → INN lookup (Step 7) |

The local CID files (`CID-Synonym-filtered.db`, `CID-Title`) are pre-downloaded PubChem datasets providing fully offline fallback, eliminating dependency on the PubChem REST API during enrichment.

### 3.3 Pharmaceutical Noise Word Lexicon

A key preprocessing step used across multiple lookup steps is `_strip_pharma_noise()`, which removes non-drug tokens from a `basename` before retrying lookups. The lexicon (`_PHARMA_NOISE`) was developed iteratively and contains over 200 terms organized into the following categories:

- **Salt/counter-ion forms**: `HYDROCHLORIDE`, `HCL`, `SULFATE`, `ACETATE`, `TARTRATE`, `PHOSPHATE`, etc.
- **Dosage form tokens**: `TABLET`, `CAPSULE`, `INJECTION`, `SOLUTION`, `SUPPOSITORY`, etc.
- **Multilingual dosage forms**: French (`BUVABLE`, `GELULE`, `SIROP`), German (`TABLETTEN`, `LOSUNG`, `SAFT`), Spanish (`COMPRIMIDO`, `SOLUCION`), Italian (`GOCCE`, `SOLUZIONE`)
- **Release modifier tokens**: `EXTENDED`, `RELEASE`, `ER`, `XR`, `SR`, `RETARD`, `DEPOT`
- **Manufacturer/brand suffix tokens**: `TEVA`, `MYLAN`, `SANDOZ`, `PFIZER`, `NOVARTIS`, etc.
- **Vaccine qualifiers**: `TETRA`, `PENTA`, `MONOVALENT`, `BIVALENT`, `ATTENUATED`, `ADJUVANTED`
- **Common English prepositions in drug phrase contexts**: `FOR`, `OF`, `WITH`, `AND`

**Design decisions to prevent false removal:**
- Standalone minerals (`CALCIUM`, `MAGNESIUM`, `ZINC`, `POTASSIUM`) are **excluded** because they appear as primary drug names (e.g., `"MAGNESIUM SUPPLEMENTATION"` → basename `"MAGNESIUM"`).
- `OXIDE` and `HYDROXIDE` are **excluded** because they are part of real drug names (`ZINC OXIDE`, `MAGNESIUM HYDROXIDE`, `NITRIC OXIDE`).

### 3.4 Seven-Step Cascade Lookup

#### Step 1: RxNav Exact Match on Basename

The normalized `basename` (from S07b) is sent to the RxNav `rxcui.json` endpoint for an exact lookup:

```
GET https://rxnav.nlm.nih.gov/REST/rxcui.json?name={basename}
```

This is the most direct and highest-precision step. A valid numeric `idType=RXNORM` response is accepted.

#### Step 2: RxNav Exact Match on Ingredients

If Step 1 fails (common for brand names with no direct RxNorm entry), each ingredient extracted by S07b is individually queried against RxNav. The first successful match is used. This step exploits the S07b LLM output to find the generic drug when only the brand name was submitted.

> **Example:** `"HUMULINE"` (brand) → ingredients `["INSULIN", "HUMAN"]` → `"INSULIN HUMAN"` → RxCUI found.

#### Step 3: Offline LocalCID Fallback

If neither basename nor ingredients match directly, the system performs an offline lookup using local PubChem CID files:

1. **CID-Synonym-filtered.db** (SQLite) — a pre-computed index mapping drug synonyms to PubChem CIDs, searched via case-insensitive SQL.
2. **CID-Title** (flat file) — maps each CID to its IUPAC canonical title, which is then submitted to RxNav.

Both `basename` and each `ingredient` are tried as lookup candidates. This step recovers drugs that appear in PubChem under a synonym but not directly in RxNorm by their reported name.

#### Step 4: Pharmaceutical Noise Stripping → Retry RxNav

If Step 3 fails, `_strip_pharma_noise()` is applied to the `basename` to remove all pharmaceutical noise tokens (see §3.3), and the stripped name is retried against RxNav. This handles cases such as:

- `"MORPHIN SULFATE"` → strip `SULFATE` → `"MORPHIN"` → found
- `"URSODIOL CAP"` → strip `CAP` → `"URSODIOL"` → found
- `"MONTELUKAST CHEWABLE TABLETS"` → strip `CHEWABLE TABLETS` → `"MONTELUKAST"` → found

#### Step 5: ChEMBL Verified Brand Lookup

For names remaining unresolved after stripping, a ChEMBL molecule search is performed. The search is strict: a ChEMBL result is **accepted only** if the original search term appears as an exact (case-insensitive) match in the molecule's `cross_references.xref_name` or `molecule_synonyms.molecule_synonym` fields. If accepted, the ChEMBL `pref_name` (preferred generic name) is then submitted to RxNav.

This strict verification prevents false positives encountered in pilot experiments (e.g., `"HUMULINE"` matched `"Humulin"` via substring containment `n in name_lower`, which was rejected by requiring exact synonym match).

#### Step 6: RxNav Approximate Match (Typo Correction)

A fuzzy RxNav lookup via the `approximateTerm.json` endpoint is used to recover drug names with typos, transpositions, or minor truncations:

```
GET https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={name}&maxEntries=3
```

A candidate match is **accepted only when all three criteria are satisfied:**

| Criterion | Threshold | Rationale |
|---|---|---|
| RxNav approximate score | ≥ 8.0 | Eliminates very low-confidence fuzzy matches |
| `difflib.SequenceMatcher` similarity | ≥ 0.70 | Ensures the input and match are genuinely similar strings |
| First-letter match (single-word inputs only) | Exact | Prevents swaps like `SOLON → COLON`, `FULCONAZOLE → SULCONAZOLE` |

> **Example:** `"FLUCANOZOLE"` → score 9.2, similarity 0.87, first letter `F = F` → accepted as `"FLUCONAZOLE"`.

The `lookup_hit` field is set to `"approx:<matched_name>"` to allow post-hoc analysis of typo corrections.

**Pilot experiment on approximate matching:** An initial analysis of 500 unresolved samples found that naive fuzzy matching (e.g., `approximateTerm` without filtering) produced high false-positive rates. Examples of rejections enforced by the strict criteria:

- `"ANTIRETROVIRAL TREATMENT"` → matched `"Acne Treatment"` (similarity 0.47 → **rejected**)
- `"SOLON"` → matched `"COLON"` (first-letter `S ≠ C` → **rejected**)
- `"FULCONAZOLE"` → matched `"SULCONAZOLE"` (first-letter `F ≠ S` → **rejected**)

#### Step 7: KEGG Drug Lookup → INN → RxNav (Non-US Brand Names)

For drugs that are regional trade names (European, Japanese, Thai) not listed in RxNorm under any variant, a two-phase KEGG Drug lookup is performed:

**Phase A — KEGG Drug search:**
```
GET https://rest.kegg.jp/find/drug/{name}
```
The first matching KEGG drug entry ID (e.g., `D01234`) is retrieved.

**Phase B — INN extraction:**
```
GET https://rest.kegg.jp/get/{kegg_id}
```
The `NAME` field of the KEGG entry is parsed to extract the International Nonproprietary Name (INN). The INN is the first name listed before a semicolon in the `NAME` block, with parenthetical annotations (e.g., `(USP/INN)`, `(TN)`) removed.

The extracted INN is then submitted to RxNav (exact match), and if not found, also to the approximate matcher (Step 6 logic). The `lookup_hit` is set to `"kegg:{kegg_id}:{inn}"`.

**False positive prevention:** A minimum length guard (`len(basename) >= 5`) is applied before KEGG lookup. Pilot testing revealed that KEGG's substring-based search would match short strings to unrelated drugs (e.g., `"AN"` → nadide, `"G"` → oxygen). After implementing this guard, 62 false-positive KEGG hits in the pediatric cohort and 1,310 in the adult cohort (from earlier runs without the guard) were identified and retroactively corrected by post-processing the output files.

#### Step 8: Fetch RxNorm Ingredient Components

For any RxCUI resolved in Steps 1–7, the RxNorm ingredient decomposition is retrieved:

```
GET https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allrelated.json
```

Active ingredient names and their own RxCUIs are stored as `rxnorm_ingredients` for downstream pharmacological analysis.

### 3.5 Rate Limiting

To comply with API rate limits and prevent service degradation, the following concurrency controls are applied via `asyncio.Semaphore`:

| API | Concurrent Requests |
|---|---|
| RxNav | 5 |
| ChEMBL (EBI) | 5 |
| KEGG Drug | 3 |

---

## 4. Experiments and Iterative Refinement

### 4.1 Version History

The enrichment pipeline was developed through 8 iterative versions (`v1`–`v8`) driven by analysis of unresolved drug names at each stage. Key experiments and resulting changes are summarized below.

| Version | Key Change | Motivation |
|---|---|---|
| v1–v3 | Initial cascade: RxNav exact → PubChem API | Baseline implementation |
| v4 | Replaced PubChem API with local CID files | PubChem API instability (HTTP 503 errors); offline files provided identical coverage |
| v5 | Preserve raw `medicinal_product`; use `basename` as lookup key | User feedback: original name was being overwritten; data integrity requirement |
| v6 | Add ChEMBL brand lookup with strict synonym verification | Pilot analysis found ~5–7% brand names not in RxNorm but resolvable via ChEMBL |
| v7 | Expand `_PHARMA_NOISE` with multilingual tokens; fix ingredients bug | Analysis of not-found drugs revealed French/German/Spanish suffix noise; ingredients from some records were not being propagated correctly due to first-occurrence precedence |
| v8 | Add RxNav approximate match (Step 6) and KEGG Drug lookup (Step 7) | Analysis of remaining not-found drugs identified two major categories: typos/misspellings and non-US brand names |

### 4.2 Analysis of Unresolved Drug Names

Iterative analysis of the 500-sample unresolved drug set (after v6) identified the following categories of failure:

| Category | Estimated Share | Resolution |
|---|---|---|
| Typographic errors / misspellings | ~25% | Step 6: Approximate match |
| Non-US brand names (EU/JP/TH) | ~8% | Step 7: KEGG Drug |
| Multi-ingredient complex formulations | ~18% | Partial: ingredient lookup |
| Highly ambiguous / single-letter codes | ~12% | Not resolvable (excluded) |
| Institutional/procedure codes | ~10% | Not drug names; intentionally left unresolved |
| Genuinely unknown/unlicensed drugs | ~27% | Not resolvable |

### 4.3 Approximate Matching Threshold Tuning

The thresholds for Step 6 (`score ≥ 8.0`, `similarity ≥ 0.70`, first-letter match) were selected empirically. Lowering `similarity` to 0.60 increased coverage by ~2% but introduced false positives at an unacceptable rate (~15% false positive rate in manually reviewed samples). The final thresholds represent a conservative precision-oriented choice.

### 4.4 KEGG False Positive Analysis

After the initial S08 run with KEGG lookup enabled (before the minimum-length guard was in place), manual inspection found:

- **Pediatric cohort:** 62 basenames of length < 5 received KEGG hits. Example: `"AN"` → KEGG D01234 (nadide). These were set to `not_found` by post-processing.
- **Adult cohort:** 1,310 such false-positive KEGG hits were identified and corrected similarly.

The minimum length guard (`len(basename) >= 5`) was then added to the code and verified to eliminate all such cases on re-analysis.

---

## 5. Results

### 5.1 Coverage by Lookup Method

**Pediatric cohort** (30,804 unique drug names):

| Lookup Method | Drug Names Resolved | Coverage (%) |
|---|---|---|
| Step 1: RxNav exact (basename) | 5,886 | 19.1% |
| Step 2: RxNav exact (ingredients) | 15,066 | 48.9% |
| Steps 3–5: LocalCID + Suffix + ChEMBL | 1,635 | 5.3% |
| Step 6: Approximate match (typo) | 1,993 | 6.5% |
| Step 7: KEGG Drug (non-US brands) | 142 | 0.5% |
| **Total found** | **24,722** | **80.3%** |
| Not found | 6,082 | 19.7% |

**Adult cohort** (165,850 unique drug names):

| Lookup Method | Drug Names Resolved | Coverage (%) |
|---|---|---|
| Step 1: RxNav exact (basename) | 10,208 | 6.2% |
| Step 2: RxNav exact (ingredients) | 78,958 | 47.6% |
| Steps 3–5: LocalCID + Suffix + ChEMBL | 11,656 | 7.0% |
| Step 6: Approximate match (typo) | 16,006 | 9.7% |
| Step 7: KEGG Drug (non-US brands) | 344 | 0.2% |
| **Total found** | **117,172** | **70.6%** |
| Not found | 48,678 | 29.4% |

### 5.2 Observations

- **Ingredient-based lookup (Step 2) dominates** both cohorts (~47–49%), reflecting the prevalence of brand names in FAERS that lack direct RxNorm entries. The S07b LLM extraction of explicit ingredient tokens is therefore the single most important preprocessing step.
- **Approximate matching (Step 6)** contributed meaningfully: 6.5% in pediatrics and 9.7% in adults, demonstrating that typographic errors are a substantial source of failures. The adult cohort's higher approximate-match rate aligns with its greater diversity of international reporters.
- **KEGG non-US lookup (Step 7)** contributed modestly (0.2–0.5%) but is qualitatively important for European and Japanese clinical trial data.
- The **adult cohort's lower overall coverage (70.6% vs. 80.3%)** is primarily attributable to a higher proportion of multi-word complex formulations, hospital compounding codes, and procedure names misreported as drug names in FAERS.

---

## 6. Output Schema (S08)

| Column | Description |
|---|---|
| `medicinal_product` | Original raw drug name (unchanged) |
| `basename` | Normalized primary drug name from S07b |
| `ingredients` | Semicolon-separated ingredient string |
| `rxcui` | RxNorm Concept Unique Identifier (null if not found) |
| `lookup_hit` | Which step resolved the RxCUI (e.g., `"basename"`, `"ingredients"`, `"approx:FLUCONAZOLE"`, `"kegg:D01234:foscarnet"`, `"not_found"`) |
| `rxnorm_ingredients` | RxNorm ingredient decomposition for the resolved RxCUI |

---

*Pipeline version: S07b (Qwen 2.5-32B, batch inference) · S08 v8.0 (7-step cascade + ingredient fetch)*
