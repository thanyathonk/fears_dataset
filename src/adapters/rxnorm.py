from __future__ import annotations

"""RxNorm adapter providing cached lookup utilities."""

import aiohttp
from dataclasses import dataclass
from typing import Dict, List, Optional

from loguru import logger

from src.settings import PipelineConfig
from src.utils.http import CachedAsyncHTTPClient, DiskCache
from src.utils.io import PipelineContext


@dataclass
class IngredientInfo:
    """Holds ingredient metadata from RxNav related endpoint."""
    rxcui: str
    name: str
    tty: str  # 'IN' or 'MIN'


class RxNormClient:
    def __init__(self, ctx: PipelineContext, session: Optional[aiohttp.ClientSession] = None) -> None:
        self.ctx = ctx
        self.session = session  # Allow external session for direct/proxy control
        cache = DiskCache(ctx.cache_dir, namespace="rxnorm")
        self.client = CachedAsyncHTTPClient(cache, timeout=60)
        self.base_url = ctx.config.rxnorm.base_url.rstrip("/")

    async def _get_json(self, url: str, params: Optional[Dict] = None) -> dict:
        """Unified HTTP GET that uses the external session if available, otherwise the cached client."""
        if self.session:
            async with self.session.get(url, params=params) as resp:
                resp.raise_for_status()
                return await resp.json()
        return await self.client.request_json("GET", url, params=params)

    async def lookup_exact(self, name: str) -> List[str]:
        """Exact match lookup - returns list of RxCUI."""
        url = f"{self.base_url}/rxcui.json"
        full_url = f"{url}?name={name}"

        try:
            payload = await self._get_json(url, params={"name": name})

            ids = payload.get("idGroup", {}).get("rxnormId", [])
            if isinstance(ids, str):
                ids = [ids]

            return ids or []
        except Exception as e:
            logger.error(f"[RxNormClient] Failed to lookup '{name}': {e} (URL: {full_url})")
            raise

    async def select_best_rxcui(self, candidates: List[str], name: str) -> str:
        """
        Select the best RxCUI from multiple candidates.

        Priority order:
        1. TTY = 'IN' (Ingredient) - preferred for drug ingredients
        2. TTY = 'MIN' (Multiple Ingredient)
        3. Any other TTY
        4. First candidate if all else fails

        Returns the best RxCUI or empty string if none found.
        """
        if not candidates:
            return ""

        if len(candidates) == 1:
            return candidates[0]

        # Log warning for multiple candidates
        logger.warning(f"[RxNormClient] Multiple RxCUI candidates for '{name}': {candidates}")

        # Get TTY for each candidate
        tty_map = {}
        for rxcui in candidates:
            try:
                tty = await self.get_rxcui_tty(rxcui)
                tty_map[rxcui] = tty
            except Exception as e:
                logger.warning(f"[RxNormClient] Could not get TTY for RxCUI {rxcui}: {e}")
                tty_map[rxcui] = "UNKNOWN"

        # Select by priority
        priority_order = ['IN', 'MIN']  # Ingredient types preferred

        for preferred_tty in priority_order:
            for rxcui, tty in tty_map.items():
                if tty == preferred_tty:
                    logger.info(f"[RxNormClient] Selected RxCUI {rxcui} (TTY: {tty}) for '{name}' from {len(candidates)} candidates")
                    return rxcui

        # Fallback: select first candidate
        selected = candidates[0]
        tty = tty_map.get(selected, "UNKNOWN")
        logger.info(f"[RxNormClient] Selected first RxCUI {selected} (TTY: {tty}) for '{name}' from {len(candidates)} candidates")
        return selected

    async def get_rxcui_tty(self, rxcui: str) -> str:
        """Get TTY (Term Type) for an RxCUI."""
        url = f"{self.base_url}/rxcui/{rxcui}/properties.json"

        try:
            payload = await self._get_json(url)
            properties = payload.get("properties", {})
            return properties.get("tty", "UNKNOWN")
        except Exception as e:
            logger.warning(f"[RxNormClient] Failed to get TTY for RxCUI {rxcui}: {e}")
            return "UNKNOWN"

    async def lookup_approximate(self, name: str) -> List[str]:
        """Approximate match lookup - returns list of RxCUI (deprecated, use lookup_exact with PubChem fallback)."""
        url = f"{self.base_url}/approximateTerm.json"  # Fixed: base_url already includes /REST
        payload = await self.client.request_json(
            "GET",
            url,
            params={"term": name, "maxEntries": 5},
        )
        group = payload.get("approximateGroup", {}).get("candidate", [])
        if isinstance(group, dict):
            group = [group]
        return [candidate.get("rxcui") for candidate in group if candidate.get("rxcui")]

    async def get_related_ingredients(self, rxcui: str) -> List[IngredientInfo]:
        """
        Get related ingredients for an RxCUI using the allrelated.json endpoint.

        API: /REST/rxcui/{rxcui}/allrelated.json

        Returns list of IngredientInfo with rxcui, name, and tty (IN, MIN).
        Filters for IN (Ingredient) and MIN (Multiple Ingredient) TTYs.
        """
        url = f"{self.base_url}/rxcui/{rxcui}/allrelated.json"
        payload = await self._get_json(url)

        ingredients: List[IngredientInfo] = []
        concept_groups = payload.get("allRelatedGroup", {}).get("conceptGroup", [])

        for group in concept_groups:
            tty = group.get("tty", "")
            if tty not in {"IN", "MIN"}:  # Focus on Ingredient and Multiple Ingredient
                continue

            concept_props = group.get("conceptProperties", []) or []
            for prop in concept_props:
                ing_rxcui = prop.get("rxcui", "")
                ing_name = prop.get("name", "")
                if ing_rxcui and ing_name:
                    ingredients.append(IngredientInfo(
                        rxcui=ing_rxcui,
                        name=ing_name.lower(),
                        tty=tty,
                    ))

        return ingredients

    async def ingredients_for_rxcui(self, rxcui: str) -> List[str]:
        """Legacy method - returns just ingredient names using allrelated endpoint."""
        url = f"{self.base_url}/rxcui/{rxcui}/allrelated.json"  # Fixed: base_url already includes /REST
        payload = await self.client.request_json("GET", url)
        group = payload.get("allRelatedGroup", {}).get("conceptGroup", [])
        ingredients: List[str] = []
        for entry in group:
            if entry.get("tty") in {"IN", "PIN"}:
                for prop in entry.get("conceptProperties", []) or []:
                    name = prop.get("name")
                    if name:
                        ingredients.append(name.lower())
        return sorted(set(ingredients))

    async def close(self) -> None:
        await self.client.aclose()


__all__ = ["RxNormClient", "IngredientInfo"]
