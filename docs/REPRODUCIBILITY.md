# Reproducibility checklist

To obtain **the same (or nearly the same) outputs** as another run, all of the following must match.

## 1. Same code revision

```bash
git checkout <commit-hash-or-tag>
```

## 2. Same configuration

- Copy `configs/config.example.yaml` → `configs/config.local.yaml` and align **stage toggles**, **paths**, **cohort age_cutoff**, and **S02 memory mode** with the reference run.
- Same **environment variables** (see `.env.example`): at minimum the same `OPENFDA_API_KEY` behaviour (key vs no key changes rate limits, not necessarily byte-identical downloads if the upstream bulk files changed).

## 3. Same inputs

| Input | Notes |
|--------|--------|
| **OpenFDA FAERS bulk** | S01 downloads a manifest-dated snapshot. Different run dates → different raw CSVs → downstream tables differ. Prefer documenting **manifest date** or **checksums** of `data/openFDA_drug_event/`. |
| **OMOP / MedDRA vocabulary** | `data/vocab/` must be the same version (see `SETUP_STANDALONE.md`). |
| **Local CID DB (S08)** | If used: same `CID-Synonym-filtered.db` / `CID-Title` files under `data/`. |

## 4. Same environment

- Python and **locked dependencies** (`requirements.txt` / conda env name).
- **RAM / S02 mode**: `s02_openfda_high_memory` vs disk-shard path can change ordering or performance; outputs should still match if deterministic, but resource failures can cause partial runs.
- **LLM steps (S07b)**: If `llm.enabled` or optional scripts use an LLM, use the **same model, temperature, and endpoint**; otherwise outputs can diverge.

## 5. Full pipeline order

Dependencies follow:

`S01 → S02 → S03 → S05 → S06/S06b → S07 → S07b (optional) → S08 → S09 → S10`

Starting from **S01** with the **same bulk data** is required for full reproducibility. Joining mid-pipeline only works if you copy **compatible intermediate artifacts** from the reference run (same schema/version).

## 6. API keys (`.env`, not committed)

Copy `.env.example` → `.env` in the repo root. Python loads it automatically when `src.settings` is imported (CLI, stages that pull in `src.utils.io`, etc.). Shell scripts such as `scripts/step1_s01_fetch.sh` also `source .env` if present.

| Variable | Stage | Required? |
|----------|--------|-----------|
| `OPENFDA_API_KEY` | S01 | Optional but recommended (higher API limits for manifest / requests). |
| `NCBI_API_KEY` | S08 | Optional; improves PubChem/NCBI rate limits. |
| `OPENAI_API_KEY` / `S07_*` | S07b scripts | Required when calling an OpenAI-compatible server. |
| `HF_TOKEN` | S10 HF upload | Only if uploading to Hugging Face. |

Each collaborator uses **their own keys**; reproducibility means **same tier of access + same external data**, not sharing secrets in git.

## 7. What this repo does *not* pin

- **Live APIs** (RxNav, PubChem, ChEMBL) can change responses slightly over time.
- **OpenFDA bulk** content updates when FDA republishes quarters.

For publication-grade reproducibility, archive **input file checksums** and **config + commit hash** alongside results.
