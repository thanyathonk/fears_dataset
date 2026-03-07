from __future__ import annotations

"""Hugging Face Hub publishing utilities."""

import os
from pathlib import Path

from huggingface_hub import HfApi, create_repo, upload_folder
from loguru import logger

from src.utils.io import PipelineContext


def publish_dataset(ctx: PipelineContext) -> None:
    cfg = ctx.config.huggingface
    if not cfg.repo_id:
        raise ValueError("Hugging Face repo_id is not configured")
    token = os.getenv(cfg.token_env)
    if not token:
        raise ValueError(f"Hugging Face token environment variable '{cfg.token_env}' not set")

    dataset_root = ctx.config.paths.output_root
    if not dataset_root.exists():
        raise FileNotFoundError(f"Output root not found: {dataset_root}")

    api = HfApi(token=token)
    create_repo(repo_id=cfg.repo_id, private=cfg.private, repo_type="dataset", token=token, exist_ok=True)

    logger.info("Uploading dataset folder %s to Hugging Face (%s)", dataset_root, cfg.repo_id)
    upload_folder(
        repo_id=cfg.repo_id,
        folder_path=str(dataset_root),
        repo_type="dataset",
        revision=cfg.branch,
        token=token,
    )
    logger.success("Dataset uploaded to Hugging Face: %s", cfg.repo_id)


__all__ = ["publish_dataset"]
