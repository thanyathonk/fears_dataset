from __future__ import annotations

"""HTTP utilities with simple filesystem caching and retry helpers."""

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from tenacity import AsyncRetrying, RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class DiskCache:
    def __init__(self, root: Path, namespace: str = "api") -> None:
        self.root = root / namespace
        self.root.mkdir(parents=True, exist_ok=True)

    def _key(self, method: str, url: str, params: Optional[Dict[str, Any]], data: Optional[Any]) -> Path:
        raw = json.dumps({"m": method, "u": url, "p": params, "d": data}, sort_keys=True, default=str)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return self.root / f"{digest}.json"

    def get(self, method: str, url: str, params: Optional[Dict[str, Any]], data: Optional[Any]) -> Optional[Dict[str, Any]]:
        path = self._key(method, url, params, data)
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        return None

    def set(self, method: str, url: str, params: Optional[Dict[str, Any]], data: Optional[Any], payload: Dict[str, Any]) -> None:
        path = self._key(method, url, params, data)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)


class CachedHTTPClient:
    def __init__(self, cache: DiskCache, timeout: int = 60) -> None:
        self.cache = cache
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((httpx.HTTPError,)),
    )
    def request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        if use_cache:
            cached = self.cache.get(method, url, params, data)
            if cached is not None:
                return cached
        response = self._client.request(method, url, params=params, json=data)
        response.raise_for_status()
        payload = response.json()
        if use_cache:
            self.cache.set(method, url, params, data, payload)
        return payload


class CachedAsyncHTTPClient:
    def __init__(self, cache: DiskCache, timeout: int = 60) -> None:
        self.cache = cache
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
        self._client = httpx.AsyncClient(timeout=timeout, limits=limits, headers={"User-Agent": "can-drug-pipeline/0.1"})

    async def request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        if use_cache:
            cached = self.cache.get(method, url, params, data)
            if cached is not None:
                return cached

        async for attempt in AsyncRetrying(
            reraise=True,
            wait=wait_exponential(multiplier=1, min=1, max=30),
            stop=stop_after_attempt(5),
            retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError)),
        ):
            with attempt:
                response = await self._client.request(method, url, params=params, json=data)
                response.raise_for_status()
                payload = response.json()
                if use_cache:
                    self.cache.set(method, url, params, data, payload)
                return payload
        raise RetryError("Unreachable")

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["DiskCache", "CachedHTTPClient", "CachedAsyncHTTPClient"]
