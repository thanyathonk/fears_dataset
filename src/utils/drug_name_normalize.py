from __future__ import annotations

"""FAERS / MedDRA-style drug string normalization before LLM or RxNav lookup."""

import re
import unicodedata
from typing import Optional

# Greek letter tokens often appear as ".ALPHA." etc. in legacy FAERS exports.
_GREEK_DOT_PATTERN = re.compile(
    r"\.(ALPHA|BETA|GAMMA|DELTA|EPSILON|THETA|LAMBDA|MU|OMEGA)\.",
    re.IGNORECASE,
)
_GREEK_MAP = {
    "alpha": "alpha-",
    "beta": "beta-",
    "gamma": "gamma-",
    "delta": "delta-",
    "epsilon": "epsilon-",
    "theta": "theta-",
    "lambda": "lambda-",
    "mu": "mu-",
    "omega": "omega-",
}

# Encoding / OCR glitches: lone "?" between letters (e.g. FILM?COATED, CO?AMOXICILLIN)
_MOJIBAKE_Q = re.compile(r"(?<=[A-Za-z0-9])\?(?=[A-Za-z0-9])")

# Trailing sentence-style dots only (keep dots inside strengths like "1.4 MG")
_TRAILING_DOT = re.compile(r"(?<=\S)\.+(?=\s*$)")


def _greek_replacer(m: re.Match[str]) -> str:
    key = m.group(1).lower()
    return _GREEK_MAP.get(key, f"{key}-")


def normalize_faers_drug_name(raw: Optional[str]) -> str:
    """
    Normalize a single drug display string for downstream LLM / vocabulary lookup.

    Heuristics (tuned on staging ``*_drugs_full_data`` samples):

    - Unicode NFKC (full-width Latin, compatibility forms)
    - MedDRA ``^`` → space
    - ``.ALPHA.``-style tokens → ASCII ``alpha-``, etc.
    - ``?`` sandwiched between alphanumerics → hyphen (common mojibake for ``-``)
    - Smart quotes → ASCII apostrophe
    - Collapse repeated hyphens and whitespace
    - Strip trailing ``.`` clutter (e.g. ``CLONAZEPAM.``) without removing internal dots
    - If the result is empty, returns the original stripped string (fallback).
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    original = s

    s = unicodedata.normalize("NFKC", s)
    s = (
        s.replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )
    s = s.replace("^", " ")
    s = _GREEK_DOT_PATTERN.sub(_greek_replacer, s)
    s = _MOJIBAKE_Q.sub("-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _TRAILING_DOT.sub("", s).strip()
    return s if s else original


__all__ = ["normalize_faers_drug_name"]
