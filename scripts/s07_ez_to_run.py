import src.settings  # noqa: F401 — loads .env before os.environ usage

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from tqdm import tqdm
import logging
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
import re

# =========================
# Setup
# =========================
warnings.filterwarnings("ignore")
os.environ["TOKENIZERS_PARALLELISM"] = "false"

logging.basicConfig(
    format="%(asctime)s -%(levelname)s- %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# Load dataset
# =========================
logger.info("Loading dataset")
ds = pd.read_parquet(
    "/ist/users/thanyathonk/thanyathonk_bak/fears_dataset/data/staging/s06_split_drug/pediatric_drugs_full_data.parquet"
)

assert "medicinal_product" in ds.columns

# =========================
# Load model
# =========================
device = "cuda" if torch.cuda.is_available() else "cpu"
model_name = "Qwen/Qwen2.5-32B-Instruct"
cache_dir = "/ist-project/scads/jenta/model"

logger.info("Loading model")
tokenizer = AutoTokenizer.from_pretrained(model_name, cache_dir=cache_dir)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16,
    device_map=device,
    cache_dir=cache_dir,
)

tokenizer.pad_token = tokenizer.eos_token
tokenizer.padding_side = "left"
logger.info("Model loaded")

# =========================
# Constants
# =========================
EXPECTED_KEYS = [
    "ingredients",
    "strength",
    "dosage_form",
    "qualifier",
    "qualifier_type",
]

SALTS = {
    "HCL", "HYDROCHLORIDE",
    "SODIUM", "POTASSIUM",
    "MALEATE", "SUCCINATE",
    "PHOSPHATE", "TARTRATE",
    "BESYLATE", "MESYLATE",
    "ACETATE", "FUMARATE",
    "CITRATE", "CALCIUM", "BROMIDE"
}

DOSAGE_FORMS = {
    "TABLET","TAB","TABS","CAPSULE","CAP","CAPS","CREAM","GEL",
    "SOLUTION","SUSPENSION","SYRUP","SPRAY","POWDER",
    "INJ","SOL","SOLN","AMP","SUSP","OINT","OINTMENT",
    "LOTION","PATCH","DROP","DROPS","LOZENGE","ELIXIR",
    "EMULSION","GRANULE","GRANULES","VIAL","AMPOULE",
}

ROUTES = {
    "ORAL","IV","INTRAVENOUS","IM","INTRAMUSCULAR",
    "SC","SUBCUTANEOUS","INJECTION","INFUSION",
    "TOPICAL","INHALATION","OPHTHALMIC","NASAL","PO",
}

BASENAME_NOISE = {
    "FOR","WITH","AND","THE","USP","NF","BP","EP",
    "PRN","NTE","VIA","PER","USE","NOT","NEB","NEBS",
}

COMMON_COMPANY = {
    "PFIZER","NOVARTIS","ROCHE","SANOFI","MERCK",
    "BAYER","ASTRAZENECA","GSK","MYLAN","TEVA","SANDOZ",
    "BRISTOL","SQUIBB","ABBVIE"
}

# Words that should never appear as ingredient names
NON_INGREDIENT = {
    # descriptors — not chemical entities
    "HUMAN","HUMANA","NORMAL","STERILE","PURIFIED",
    "UNKNOWN","UNSPECIFIED","OTHER","AQUEOUS","CONCENTRATE",
    # FAERS codes / generic labels
    "NOS","INGREDIENT","INGREDIENTS","ACTIVE","INACTIVE",
    # solvents / vehicles
    "WATER","SALINE",
    # prepositions (French/Spanish de, von, van)
    "DE","VON","VAN","DU","EL","LA","LAS","LOS",
}

# =========================
# Prompt
# =========================
SYSTEM_PROMPT = (
    "You are an AI system for pharmaceutical text decomposition.\n\n"
    "Priority rules:\n"
    "1. If 'Active substance' is provided, use it as the PRIMARY source for ingredients.\n"
    "   Do NOT override it with text from the drug name.\n"
    "2. If 'Active substance' is empty, extract ingredients from the drug name only if "
    "explicitly written as chemicals.\n"
    "3. Extract strength, dosage_form, qualifier from the drug name regardless.\n\n"
    "General rules:\n"
    "- Salts (e.g., HCL, MALEATE) are part of ingredients list.\n"
    "- Country names, brand names, regions are NOT ingredients — put in qualifier.\n"
    "- Preserve original casing of ingredient names.\n"
)

USER_PROMPT = """
Drug name:
{name}

Active substance (from FAERS — use as PRIMARY source for ingredients if provided):
{substance}

Return VALID JSON ONLY with keys:
{keys}

Rules:
- Missing fields → null
- ingredients must be list or null

Examples:
Input name: TAVOR  substance: (empty)
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}

Input name: HUMALOG  substance: INSULIN LISPRO
Output: {{"ingredients":["INSULIN LISPRO"],"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}

Input name: DENOSINE (JAPAN)  substance: (empty)
Output: {{"ingredients":null,"strength":null,"dosage_form":null,"qualifier":"JAPAN","qualifier_type":"COUNTRY"}}

Input name: PROPRANOLOL HCL 10MG TABLET  substance: (empty)
Output: {{"ingredients":["PROPRANOLOL","HCL"],"strength":"10MG","dosage_form":"TABLET","qualifier":null,"qualifier_type":null}}

Input name: VALCYTE 450MG TABLET  substance: VALGANCICLOVIR HYDROCHLORIDE
Output: {{"ingredients":["VALGANCICLOVIR","HYDROCHLORIDE"],"strength":"450MG","dosage_form":"TABLET","qualifier":null,"qualifier_type":null}}

Input name: AMOXICILINA + ACIDO CLAVULANICO MEPHA  substance: AMOXICILLIN\\CLAVULANIC ACID
Output: {{"ingredients":["AMOXICILLIN","CLAVULANIC ACID"],"strength":null,"dosage_form":null,"qualifier":null,"qualifier_type":null}}
"""

# =========================
# Helpers
# =========================
def _pick_name(row):
    """norm priority: medicinal_product_norm first, else raw medicinal_product."""
    if "medicinal_product_norm" in row.index:
        norm = row.get("medicinal_product_norm")
        if norm is not None and not (isinstance(norm, float) and pd.isna(norm)):
            ns = str(norm).strip()
            if ns:
                return ns
    raw = row.get("medicinal_product")
    return str(raw).strip() if raw is not None else ""

def _pick_substance(row):
    """Join active_substance_faers list to a single string, or empty."""
    if "active_substance_faers" not in row.index:
        return ""
    val = row.get("active_substance_faers")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, list):
        parts = [str(x).strip() for x in val if x is not None and str(x).strip()]
        return " | ".join(parts)
    return str(val).strip()

def derive_basename(text):
    text = text.upper()
    # extract bracket content before removing, use as fallback pool
    bracket_content = re.findall(r"\(([^)]+)\)", text)
    text_no_brackets = re.sub(r"\(.*?\)", " ", text)
    text_no_brackets = re.sub(r"[^A-Z ]", " ", text_no_brackets)
    def _bn_keep(tok):
        return (len(tok) > 2
                and tok not in SALTS
                and tok not in DOSAGE_FORMS
                and tok not in ROUTES
                and tok not in BASENAME_NOISE
                and tok not in COMMON_COMPANY)

    tokens = [t for t in text_no_brackets.split() if _bn_keep(t)]
    if tokens:
        return " ".join(tokens)
    # fallback: try tokens from bracket content
    for bc in bracket_content:
        bc_clean = re.sub(r"[^A-Z ]", " ", bc.upper())
        fb_tokens = [t for t in bc_clean.split() if _bn_keep(t)]
        if fb_tokens:
            return " ".join(fb_tokens)
    # last resort
    raw_clean = re.sub(r"\s+", " ", re.sub(r"[^A-Z ]", " ", text.upper())).strip()
    last_tokens = [t for t in raw_clean.split() if _bn_keep(t)]
    return " ".join(last_tokens) if last_tokens else raw_clean

# =========================
# Inference
# =========================
@torch.no_grad()
def inference_batch(prompts_data):
    """prompts_data: list of (name, substance) tuples."""
    messages = []
    for name, substance in prompts_data:
        messages.append([
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": USER_PROMPT.format(
                    name=name,
                    substance=substance if substance else "(empty)",
                    keys=EXPECTED_KEYS,
                ),
            },
        ])

    prompts = [
        tokenizer.apply_chat_template(
            m,
            add_generation_prompt=True,
            tokenize=False,
        )
        for m in messages
    ]

    tokens = tokenizer(
        prompts,
        return_tensors="pt",
        padding=True,
        truncation=True,
    ).to(model.device)

    outputs = model.generate(
        **tokens,
        max_new_tokens=512,
        do_sample=False,
        temperature=0.4,
        eos_token_id=tokenizer.eos_token_id,
    )

    decoded = []
    for i, out in enumerate(outputs):
        decoded.append(
            tokenizer.decode(
                out[len(tokens["input_ids"][i]):],
                skip_special_tokens=True,
            ).strip()
        )

    return decoded

# =========================
# Post-processing
# =========================
def _clean_ingredient_token(raw: str) -> str:
    """Remove FAERS dot-notation, symbols, and numeric position/stereo tokens."""
    t = raw.strip().upper()
    # replace .WORD. patterns (Greek letter encoding) with just the word
    t = re.sub(r"\.([A-Z]+)\.", r"\1", t)
    # strip remaining non-alphanumeric except spaces
    t = re.sub(r"[^A-Z0-9 ]", " ", t)
    # remove standalone numeric/stereo tokens: "9", "6S", "5R", "1", "3"
    tokens = [
        tk for tk in t.split()
        if len(tk) > 1                                 # single char: D, E, Z
        and not re.fullmatch(r"\d+", tk)               # pure digit: 1, 5, 9
        and not re.fullmatch(r"\d+[A-Z]{1,2}", tk)    # stereo: 6S, 5R, 4E
        and not re.fullmatch(r"[A-Z]{1,2}\d+", tk)    # e.g. D9
    ]
    return " ".join(tokens).strip()


def split_ingredient_and_salt(parsed_ingredients):
    if not isinstance(parsed_ingredients, list):
        return None, None

    ing = []
    salt = []

    for x in parsed_ingredients:
        if not isinstance(x, str):
            continue
        t = _clean_ingredient_token(x)
        if not t:
            continue
        if t in NON_INGREDIENT:
            continue
        if t in SALTS:
            salt.append(t)
        else:
            ing.append(t)

    return (
        ing if ing else None,
        salt if salt else None,
    )

# =========================
# Run pipeline
# =========================
batch_size = 64
records = []

logger.info("Start inference")

for i in tqdm(range(0, len(ds), batch_size)):
    batch = ds.iloc[i:i + batch_size]

    raw_keys = []
    prompts_data = []
    names_for_basename = []

    for _, row in batch.iterrows():
        raw_keys.append(str(row["medicinal_product"]) if pd.notna(row["medicinal_product"]) else "")
        name = _pick_name(row)
        substance = _pick_substance(row)
        prompts_data.append((name, substance))
        names_for_basename.append(name)

    outputs = inference_batch(prompts_data)

    for raw_key, name, out in zip(raw_keys, names_for_basename, outputs):

        try:
            parsed = json.loads(out)
        except Exception:
            parsed = {}

        if isinstance(parsed, list):
            parsed = parsed[0] if parsed and isinstance(parsed[0], dict) else {}

        if not isinstance(parsed, dict):
            parsed = {}

        ingredient_clean, salt = split_ingredient_and_salt(
            parsed.get("ingredients")
        )

        def _str_or_none(v):
            if v is None: return None
            s = str(v).strip()
            return None if s.lower() in ("none", "null", "") else s

        record = {
            "medicinal_product": raw_key,
            "basename": derive_basename(name),
            "ingredients": ingredient_clean,
            "salt": salt,
            "strength": _str_or_none(parsed.get("strength")),
            "dosage_form": _str_or_none(parsed.get("dosage_form")),
            "qualifier": _str_or_none(parsed.get("qualifier")),
            "qualifier_type": _str_or_none(parsed.get("qualifier_type")),
        }

        records.append(record)

# =========================
# Save output
# =========================
df = pd.DataFrame(records)
df = df[
    [
        "medicinal_product",
        "basename",
        "ingredients",
        "salt",
        "strength",
        "dosage_form",
        "qualifier",
        "qualifier_type",
    ]
]

_SCHEMA = pa.schema([
    pa.field("medicinal_product", pa.string()),
    pa.field("basename", pa.string()),
    pa.field("ingredients", pa.list_(pa.string())),
    pa.field("salt", pa.list_(pa.string())),
    pa.field("strength", pa.string()),
    pa.field("dosage_form", pa.string()),
    pa.field("qualifier", pa.string()),
    pa.field("qualifier_type", pa.string()),
])

out_path = "/ist-project/scads/jenta/pediatric_drugs_llm_cleaned_full_data.parquet"
table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
pq.write_table(table, out_path)
logger.info(f"Saved output → {out_path}")
