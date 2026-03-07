from __future__ import annotations

"""Settings and configuration loading for the can-drug pipeline."""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class OpenFDAConfig(BaseModel):
    download_manifest_url: str = "https://api.fda.gov/download.json"
    partition_key: str = "drug/event"
    api_token_env: str = "OPENFDA_API_KEY"
    partitions: list[str] = Field(default_factory=list)
    quarters: list[str] = Field(default_factory=list)
    max_partitions: int = 0
    parallel_downloads: int = 4
    chunk_size: int = 4_194_304
    retry_attempts: int = 5
    retry_backoff_seconds: float = 2.5


class CohortConfig(BaseModel):
    age_cutoff: int = 21
    age_column: str = "patient_custom_master_age"
    pediatric_comparator: str = "<="
    adult_comparator: str = ">"


class RxNormConfig(BaseModel):
    base_url: str = "https://rxnav.nlm.nih.gov/REST"
    concurrency: int = 10
    throttle_seconds: float = 0.05
    retry_attempts: int = 5
    retry_backoff_seconds: float = 2.0


class ChemicalConfig(BaseModel):
    pubchem_base_url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    chembl_base_url: str = "https://www.ebi.ac.uk/chembl/api/data"
    unichem_base_url: str = "https://www.ebi.ac.uk/unichem/rest"
    include_drugbank: bool = False
    smiles_sources: list[str] = Field(default_factory=lambda: ["pubchem", "chembl", "unichem"])


class LLMConfig(BaseModel):
    enabled: bool = False
    model: str = "gpt-5-nano"
    chunk_size: int = 250
    cache_results: bool = True
    openai_api_key_env: str = "OPENAI_API_KEY"


class HuggingFaceConfig(BaseModel):
    repo_id: str = ""
    branch: str = "main"
    private: bool = True
    token_env: str = "HF_TOKEN"


class StageToggles(BaseModel):
    s01_fetch_openfda: bool = True
    s02_entity_format: bool = True
    s02_openfda_high_memory: bool = False  # True when RAM >= 80GB for faster S02 openfda
    s03_join_partition_age: bool = True
    s05_split_adr: bool = True
    s06_map_omop_meddra: bool = True
    s06b_map_omop_meddra_full_hierarchy: bool = True
    s07_split_drug: bool = True
    s08_enrich_drug_identifiers: bool = True
    s09_finalize_merge_and_report: bool = True
    s10_package_deliverables: bool = True


class PathsConfig(BaseModel):
    root: Path = Path(".")
    data_root: Path = Path("data")
    staging_root: Path = Path("data/staging")
    raw_root: Path = Path("data/raw")
    reference_root: Path = Path("data/reference")
    vocab_root: Path = Path("data/vocab")
    output_root: Path = Path("data/output")
    cache_root: Path = Path("cache")
    logs_root: Path = Path("logs")

    @field_validator("root", mode="before")
    @classmethod
    def _coerce_root(cls, value: Any) -> Path:
        return Path(value).resolve() if value else Path(".").resolve()

    @model_validator(mode="after")
    def _resolve_paths(self) -> "PathsConfig":
        for field_name in (
            "data_root",
            "staging_root",
            "raw_root",
            "reference_root",
            "vocab_root",
            "output_root",
            "cache_root",
            "logs_root",
        ):
            value = getattr(self, field_name)
            setattr(self, field_name, (self.root / value).resolve())
        return self


class PipelineConfig(BaseModel):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    openfda: OpenFDAConfig = Field(default_factory=OpenFDAConfig)
    cohorts: CohortConfig = Field(default_factory=CohortConfig)
    rxnorm: RxNormConfig = Field(default_factory=RxNormConfig)
    chemistry: ChemicalConfig = Field(default_factory=ChemicalConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    huggingface: HuggingFaceConfig = Field(default_factory=HuggingFaceConfig)
    stages: StageToggles = Field(default_factory=StageToggles)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PIPELINE_",
        extra="ignore",
    )

    config_path: Path = Path("configs/config.local.yaml")


def load_settings(config_path: Optional[Path | str] = None) -> PipelineConfig:
    """Load pipeline configuration from YAML, overlaying environment defaults."""
    env_settings = EnvSettings()
    path = Path(config_path or env_settings.config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data: Dict[str, Any] = yaml.safe_load(handle) or {}

    return PipelineConfig(**data)


__all__ = ["PipelineConfig", "load_settings", "EnvSettings"]
