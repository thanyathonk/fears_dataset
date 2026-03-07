from __future__ import annotations

"""ChEMBL adapter for fetching SMILES by identifier."""

from typing import Optional

from src.utils.http import CachedAsyncHTTPClient, DiskCache
from src.utils.io import PipelineContext


class ChemblClient:
    def __init__(self, ctx: PipelineContext) -> None:
        self.ctx = ctx
        self.base_url = ctx.config.chemistry.chembl_base_url.rstrip("/")
        cache = DiskCache(ctx.cache_dir, namespace="chembl")
        self.client = CachedAsyncHTTPClient(cache, timeout=60)

    async def fetch_molecule(self, chembl_id: str) -> Optional[dict]:
        url = f"{self.base_url}/molecule/{chembl_id}.json"
        payload = await self.client.request_json("GET", url)
        return payload.get("molecule")

    async def fetch_smiles(self, chembl_id: str) -> Optional[str]:
        molecule = await self.fetch_molecule(chembl_id)
        if not molecule:
            return None
        props = molecule.get("molecule_structures", {})
        return props.get("canonical_smiles")

    async def close(self) -> None:
        await self.client.aclose()


__all__ = ["ChemblClient"]
