from __future__ import annotations

"""PubChem adapter for chemical descriptor lookups."""

from typing import Dict, Optional
from urllib.parse import quote

from src.utils.http import CachedAsyncHTTPClient, DiskCache
from src.utils.io import PipelineContext


class PubChemClient:
    def __init__(self, ctx: PipelineContext) -> None:
        self.ctx = ctx
        self.base_url = ctx.config.chemistry.pubchem_base_url.rstrip("/")
        cache = DiskCache(ctx.cache_dir, namespace="pubchem")
        self.client = CachedAsyncHTTPClient(cache, timeout=60)

    async def fetch_smiles(self, identifier: str, namespace: str = "name") -> Optional[str]:
        safe_identifier = quote(identifier, safe="")
        url = f"{self.base_url}/compound/{namespace}/{safe_identifier}/property/CanonicalSMILES/JSON"
        payload = await self.client.request_json("GET", url)
        props = payload.get("PropertyTable", {}).get("Properties", [])
        if props:
            entry = props[0]
            # Prefer CanonicalSMILES; fallback to IsomericSMILES or ConnectivitySMILES
            return (
                entry.get("CanonicalSMILES")
                or entry.get("IsomericSMILES")
                or entry.get("ConnectivitySMILES")
            )
        return None

    async def close(self) -> None:
        await self.client.aclose()


__all__ = ["PubChemClient"]
