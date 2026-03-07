from __future__ import annotations

"""Stage registry for the can-drug pipeline."""

from importlib import import_module
from typing import Callable, Dict, List

from src.utils.io import PipelineContext

StageFunction = Callable[[PipelineContext], None]

_STAGE_MODULES: Dict[str, str] = {
    "s01_fetch_openfda": "src.stages.s01_fetch_openfda",
    "s02_entity_format": "src.stages.s02_entity_format_stream",
    "s03_join_partition_age": "src.stages.s03_join_partition_age",
    "s05_split_adr": "src.stages.s05_split_adr",
    "s06_map_omop_meddra": "src.stages.s06_map_omop_meddra",
    "s06b_map_omop_meddra_full_hierarchy": "src.stages.s06b_map_omop_meddra_full_hierarchy",
    "s07_split_drug": "src.stages.s07_split_drug",
    "s07b_llm_clean": "src.stages.s07b_llm_clean",
    "s08_enrich_drug_identifiers": "src.stages.s08_enrich_drug_identifiers_local",
    "s09_finalize_merge_and_report": "src.stages.s09_finalize_merge_and_report",
    "s10_package_deliverables": "src.stages.s10_package_deliverables",
}

ORDERED_STAGES: List[str] = list(_STAGE_MODULES.keys())


def get_stage_callable(key: str) -> StageFunction:
    try:
        module_path = _STAGE_MODULES[key]
    except KeyError as exc:  # pragma: no cover - config error path
        raise ValueError(f"Unknown stage key: {key}") from exc

    module = import_module(module_path)
    try:
        run_callable = getattr(module, "run")
    except AttributeError as exc:  # pragma: no cover - developer error path
        raise ValueError(f"Stage '{key}' is missing a run(ctx) implementation") from exc
    if not callable(run_callable):  # pragma: no cover - sanity check
        raise TypeError(f"Stage '{key}' run attribute is not callable")
    return run_callable  # type: ignore[return-value]


__all__ = ["ORDERED_STAGES", "get_stage_callable"]
