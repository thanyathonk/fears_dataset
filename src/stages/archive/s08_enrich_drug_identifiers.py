from __future__ import annotations

"""Stage S08 – Smart Bandwidth Optimization with Error Circuit Breaker.

New Logic (v3):
1. Frequency sort drug names (process popular drugs first)
2. RxNav Exact Match (direct session, free)
3. PubChem Fallback (proxy session with circuit breaker)
4. Circuit breaker: auto-disable proxy when quota exceeded
5. Preserve all data (no filtering)
"""

import asyncio
import os
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional

import aiohttp
import polars as pl
from loguru import logger
from tqdm import tqdm

from src.adapters.rxnorm import RxNormClient, IngredientInfo
from src.utils.io import PipelineContext, stage_output_path, write_manifest

# Residential Proxy Configuration & Circuit Breaker
PROXY_URL = "http://spqc76uvna:c6wY~3QoAQjl7z5dzc@gate.decodo.com:10001"  # TODO: Replace with actual credentials

# Circuit Breaker State
PROXY_DEAD = False
consecutive_proxy_errors = 0
MAX_CONSECUTIVE_ERRORS = 20  # If 20 consecutive errors, assume proxy dead/quota exceeded

# Real User-Agents for Header Rotation (bypassing PubChem bot detection)
REAL_USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",

    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",

    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",

    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",

    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",

    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",

    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",

    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36",

    # Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
]

def get_random_headers():
    """Generate random headers to bypass PubChem bot detection."""
    import random

    user_agent = random.choice(REAL_USER_AGENTS)

    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Referer": "https://pubchem.ncbi.nlm.nih.gov/",
        "From": "tttccc4589@gmail.com",  # NCBI policy compliant contact email
    }

    return headers


class PubChemRateLimiter:
    """Dynamic rate limiter for PubChem API based on X-Throttling-Control headers."""

    def __init__(self, base_delay: float = 0.01):  # Reduced base delay for proxy
        self.current_delay = base_delay
        self.base_delay = base_delay

    def update_from_headers(self, headers: dict):
        """Update delay based on X-Throttling-Control header."""
        throttling_control = headers.get('X-Throttling-Control', '').upper()

        if 'BLACK' in throttling_control:
            self.current_delay = 2.0  # Reduced delay (proxy can rotate IP)
            logger.warning("[S07] PubChem BLACK: Server heavily overloaded, setting delay to 2s")
        elif 'RED' in throttling_control:
            self.current_delay = 1.0  # Reduced delay (proxy can rotate IP)
            logger.warning("[S07] PubChem RED: High load, setting delay to 1s")
        elif 'YELLOW' in throttling_control:
            self.current_delay = 0.5  # Moderate throttling
            logger.info("[S07] PubChem YELLOW: Moderate load, setting delay to 0.5s")
        elif 'GREEN' in throttling_control:
            self.current_delay = self.base_delay  # Normal operation
            logger.debug("[S07] PubChem GREEN: Normal operation, using base delay")
        else:
            # If no header, keep current delay but log
            logger.debug(f"[S07] PubChem header not found, keeping current delay: {self.current_delay}s")

    async def wait(self):
        """Wait for the current delay period."""
        if self.current_delay > 0:
            await asyncio.sleep(self.current_delay)


@dataclass
class EnrichmentResult:
    """Holds complete enrichment result for a drug name."""
    rxcui: Optional[str]
    rxcui_tty: Optional[str]  # TTY of the main concept (if found)
    ingredients: List[str]  # List of ingredient names
    ingredient_count: int
    source: str  # 'rxnav_exact', 'pubchem_fallback', or 'not_found'


# Global cache type for enrichment results
EnrichmentCache = Dict[str, EnrichmentResult]


async def _lookup_pubchem_title(
    session: aiohttp.ClientSession,
    name: str,
    limiter: PubChemRateLimiter,
    max_retries: int = 2,  # Reduced for bandwidth optimization
) -> Optional[str]:
    """
    Query PubChem REST API to get the standardized compound Title with circuit breaker.

    URL: https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{name}/property/Title/JSON

    Includes circuit breaker: if proxy appears dead/quota exceeded, returns None immediately.
    Returns the Title if found, None otherwise.
    """
    global PROXY_DEAD, consecutive_proxy_errors

    # Circuit Breaker: If proxy is dead, don't even try
    if PROXY_DEAD:
        return None

    encoded_name = urllib.parse.quote(name, safe="")
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{encoded_name}/property/Title/JSON"

    for attempt in range(max_retries):
        try:
            # Wait according to rate limiter before making request
            await limiter.wait()

            # Get fresh headers for each attempt (bypass bot detection)
            headers = get_random_headers()

            async with session.get(url, proxy=PROXY_URL, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                # Update rate limiter with response headers
                limiter.update_from_headers(dict(resp.headers))

                if resp.status == 404:
                    return None
                elif resp.status in (503, 429):  # Server busy or rate limited
                    # With proxy and Connection: close, retry immediately (fresh IP each time)
                    if attempt < max_retries - 1:
                        logger.debug(f"[S07] ⚠️  RATE LIMIT: PubChem {resp.status} for '{name}' (attempt {attempt + 1}/{max_retries}), retrying with fresh IP")
                    else:
                        logger.warning(f"[S07] ⚠️  RATE LIMIT: PubChem {resp.status} for '{name}' failed after {max_retries} retries")
                    continue
                elif resp.status >= 500:  # Other server errors
                    logger.warning("[S07] PubChem server error %d for %s", resp.status, name)
                    return None

                resp.raise_for_status()
                data = await resp.json()

                # Extract Title from response
                properties = data.get("PropertyTable", {}).get("Properties", [])
                if properties and len(properties) > 0:
                    title = properties[0].get("Title")
                    if title and title.lower() != name.lower():
                        # Success: Reset circuit breaker
                        global consecutive_proxy_errors
                        consecutive_proxy_errors = 0
                        return title
                return None

        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
                continue
            return None

        except aiohttp.ClientError as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
                continue
            return None

        except (aiohttp.ClientProxyConnectionError, aiohttp.ClientConnectorError,
                aiohttp.ClientError, asyncio.TimeoutError, Exception) as e:
            consecutive_proxy_errors += 1
            logger.warning("[S07] PubChem error #%d for '%s': %s", consecutive_proxy_errors, name, e)

            # Circuit Breaker: Too many consecutive errors = proxy dead/quota exceeded
            if consecutive_proxy_errors >= MAX_CONSECUTIVE_ERRORS:
                PROXY_DEAD = True
                logger.error("[S07] 🚨 CIRCUIT BREAKER: %d consecutive proxy errors! Proxy quota exceeded or dead. Switching to RxNav ONLY mode.", MAX_CONSECUTIVE_ERRORS)
                return None

            return None

        # No additional sleep needed - rate limiter handles timing

    logger.warning("[S07] PubChem failed after %d retries for: %s", max_retries, name)
    return None


async def _enrich_names(
    ctx: PipelineContext,
    names: List[str],
    cache: EnrichmentCache,
    *,
    label: Optional[str] = None,
) -> EnrichmentCache:
    """
    Enrich drug names with RxNorm data using:
    1. RxNav Exact Match
    2. PubChem Fallback (if exact match fails)
    3. Get ingredients with TTY metadata

    Hybrid approach: RxNav uses direct connection, PubChem uses proxy for bandwidth optimization.
    No approximate matching is used.
    """
    
    # Configuration - TURBO SPEED MODE: Decoupled semaphores for optimal performance
    # RxNav: Direct connection with conservative limits (avoid NLM blocking)
    # PubChem: Proxy with aggressive concurrency (IP rotation handles rate limits)

    rxnav_concurrency = 15      # Balanced for direct RxNav (avoid IP blocking)
    pubchem_concurrency = 100   # Aggressive for proxy PubChem (IP rotation)

    rxnav_sem = asyncio.Semaphore(rxnav_concurrency)
    pubchem_sem = asyncio.Semaphore(pubchem_concurrency)

    throttle = 0.01              # Minimal throttle for direct requests
    pubchem_throttle = 0         # No throttle for proxy (let IP rotation handle)
    results: EnrichmentCache = {}
    
    # Create PubChem rate limiter
    pubchem_limiter = PubChemRateLimiter(base_delay=0.1)

    # HYBRID SESSIONS: Direct for RxNav (free), Proxy for PubChem (with circuit breaker)

    # 1. Direct session for RxNav (FREE - no proxy bandwidth)
    direct_connector = aiohttp.TCPConnector(
        limit=200,  # High total limit to support decoupled semaphores
        limit_per_host=50,  # High per-host for RxNav performance
    )
    direct_timeout = aiohttp.ClientTimeout(total=6, connect=2)  # Very fast for direct

    # 2. Proxy session for PubChem (PAID - with circuit breaker)
    proxy_connector = aiohttp.TCPConnector(
        limit=200,  # High total limit for aggressive PubChem concurrency
        limit_per_host=100,  # Very high per-host for proxy performance
        force_close=True,  # Force close for IP rotation
        enable_cleanup_closed=True,
    )
    proxy_timeout = aiohttp.ClientTimeout(total=12, connect=4)  # Reasonable for proxy

    # Initialize circuit breaker state
    global PROXY_DEAD, consecutive_proxy_errors
    PROXY_DEAD = False
    consecutive_proxy_errors = 0

    async with aiohttp.ClientSession(connector=direct_connector, timeout=direct_timeout) as direct_session:
        async with aiohttp.ClientSession(connector=proxy_connector, timeout=proxy_timeout) as proxy_session:

            # RxNorm client uses DIRECT session (free bandwidth)
            client = RxNormClient(ctx, direct_session)

    async def handle(name: str) -> None:
                try:
                    rxcui: Optional[str] = None
                    source = "not_found"

                    # Log circuit breaker status for monitoring
                    global PROXY_DEAD, consecutive_proxy_errors
                    if PROXY_DEAD:
                        logger.info("[S07] Circuit breaker active: Proxy dead, RxNav only mode")

                    # Step 1: RxNav Exact Match (Direct Connection - Conservative Semaphore)
                    async with rxnav_sem:
                candidates = await client.lookup_exact(name)
                        if candidates:
                            rxcui = candidates[0]
                            source = "rxnav_exact"

                        # Step 2: PubChem Fallback (if exact match failed)
                        if not rxcui:
                            # PubChem API Call (Proxy Connection - Aggressive Semaphore)
                            async with pubchem_sem:
                                pubchem_title = await _lookup_pubchem_title(
                                    proxy_session, name, limiter=pubchem_limiter, max_retries=2
                                )
                            if pubchem_title:
                                # Retry exact match with PubChem title (RxNav Direct)
                                async with rxnav_sem:
                                    candidates = await client.lookup_exact(pubchem_title)
                                if candidates:
                rxcui = candidates[0]
                                    source = "pubchem_fallback"
                                    logger.info(f"[S07] ✓ PubChem fallback: '{name}' → '{pubchem_title}' → RxCUI {rxcui}")
                                else:
                                    logger.warning(f"[S07] PubChem fallback failed: '{pubchem_title}' not found in RxNav")
                                # No Title found - continue with not_found

                        # Step 3: Get ingredient metadata if we have valid RxCUI
                        if rxcui and rxcui.strip():
                            try:
                                # Get ingredients (RxNav Direct)
                                async with rxnav_sem:
                                    ingredient_infos = await client.get_related_ingredients(rxcui)

                                # Extract unique ingredient names
                                ingredient_names = list(set(
                                    info.name for info in ingredient_infos
                                ))

                                # Determine TTY (prefer IN over MIN if both present)
                                tty_set = set(info.tty for info in ingredient_infos)
                                rxcui_tty = "IN" if "IN" in tty_set else ("MIN" if "MIN" in tty_set else None)

                                results[name] = EnrichmentResult(
                                    rxcui=rxcui,
                                    rxcui_tty=rxcui_tty,
                                    ingredients=sorted(ingredient_names),
                                    ingredient_count=len(ingredient_names),
                                    source=source,
                                )
                            except Exception as e:
                                logger.warning("[S07] Failed to get ingredients for RxCUI %s (%s): %s", rxcui, name, e)
                                results[name] = EnrichmentResult(
                                    rxcui=rxcui,
                                    rxcui_tty=None,
                                    ingredients=[],
                                    ingredient_count=0,
                                    source="ingredients_error",
                                )
                        else:
                            results[name] = EnrichmentResult(
                                rxcui=None,
                                rxcui_tty=None,
                                ingredients=[],
                                ingredient_count=0,
                                source=source,
                            )

                except Exception as exc:
                        logger.warning("[S07] Enrichment failed for %s: %s", name, exc)
                        results[name] = EnrichmentResult(
                            rxcui=None,
                            rxcui_tty=None,
                            ingredients=[],
                            ingredient_count=0,
                            source="error",
                        )
            finally:
                await asyncio.sleep(throttle)
        
    # Progress over async tasks
    tasks = [asyncio.create_task(handle(name)) for name in names]
    desc = f"S08 lookups {label}" if label else "S08 lookups"
    for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=desc):
        await fut
    
    await client.close()
    cache.update(results)
    return results


def run(ctx: PipelineContext) -> None:
    """Run Stage S08: Drug identifier enrichment."""
    source_dir = stage_output_path(ctx, "s06b_llm_clean")
    output_dir = stage_output_path(ctx, "s07_enrich_drug_identifiers")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}
    global_cache: EnrichmentCache = {}

    # Check force flag
    force_enrichment = os.environ.get("FORCE_ENRICHMENT") == "1"
    if force_enrichment:
        logger.info("[S07] FORCE mode: Skipping cache loading, will re-run all enrichment")
        global_cache.clear()  # Clear any existing cache
    else:
        # Preload cache from previous enriched outputs if present
        for cohort in tqdm(("pediatric", "adult"), desc="S08 cache warm"):
        existing = output_dir / f"{cohort}_drugs_enriched.parquet"
        if existing.exists():
            try:
                cached_df = pl.read_parquet(existing)

                    # Identify the drug name column
                clean_col = None
                if "medicinal_product_llm_clean" in cached_df.columns:
                    clean_col = "medicinal_product_llm_clean"
                elif "medicinal_product_clean" in cached_df.columns:
                    clean_col = "medicinal_product_clean"
                    
                    # Check for new schema columns
                    has_new_schema = all(col in cached_df.columns for col in ["rxcui", "rxcui_tty", "ingredients", "ingredient_count"])

                    if clean_col and has_new_schema and cached_df.get_column(clean_col).dtype == pl.Utf8:
                        logger.info("[S07] Loading cached enrichment from %s (new schema)", existing)

                    tmp = cached_df.select(
                            pl.col(clean_col).cast(pl.Utf8).alias("name"),
                        pl.col("rxcui").cast(pl.Utf8, strict=False),
                            pl.col("rxcui_tty").cast(pl.Utf8, strict=False),
                        pl.col("ingredients").cast(pl.List(pl.Utf8), strict=False),
                            pl.col("ingredient_count").cast(pl.Int32, strict=False),
                        ).drop_nulls(["name"]).unique(subset=["name"])

                        for row in tmp.iter_rows(named=True):
                            name = row["name"]
                            if name:
                                global_cache[name] = EnrichmentResult(
                                    rxcui=row["rxcui"],
                                    rxcui_tty=row["rxcui_tty"],
                                    ingredients=row["ingredients"] or [],
                                    ingredient_count=row["ingredient_count"] or 0,
                                    source="cached",
                                )
                        logger.info("[S07] Loaded %d cached entries from %s", len(global_cache), cohort)
                else:
                        logger.info("[S07] Skip cache warm: incompatible schema in %s", existing)
            except Exception as e:
                    logger.info("[S07] Skip cache warm: error reading %s (%s)", existing, e)

    # Process each cohort (support selective cohort via environment variable)
    target_cohort = os.environ.get("TARGET_COHORT")
    if target_cohort:
        cohorts_to_process = (target_cohort,)
        logger.info("[S07] Processing selected cohort: %s", target_cohort)
    else:
        cohorts_to_process = ("pediatric", "adult")
        logger.info("[S07] Processing all cohorts: pediatric, adult")

    for cohort in cohorts_to_process:
        source = source_dir / f"{cohort}_drugs_clean.parquet"
        if not source.exists():
            raise FileNotFoundError(f"Missing LLM-cleaned drug dataset for cohort {cohort}")
        
        df = pl.read_parquet(source)
        
        # Clean schema: drop legacy columns
        legacy_cols = ["__index_level_0__", "rxcui", "rxcui_tty", "ingredients", "ingredient_count"]
        for col in legacy_cols:
            if col in df.columns:
                df = df.drop(col)

        # Filter rows with valid drug names
        df = df.filter(pl.col("medicinal_product_llm_clean").is_not_null())
        
        # Build list of names to enrich
        df_with_id = df.with_row_count("row_id")
        col_dtype = df_with_id.get_column("medicinal_product_llm_clean").dtype
        
        if col_dtype == pl.List(pl.Utf8):
            names_exploded = (
                df_with_id
                .select("row_id", pl.col("medicinal_product_llm_clean"))
                .explode("medicinal_product_llm_clean")
                .rename({"medicinal_product_llm_clean": "name"})
                .drop_nulls("name")
            )
        elif col_dtype == pl.Utf8:
            names_exploded = (
                df_with_id
                .select("row_id", pl.col("medicinal_product_llm_clean").alias("name"))
                .drop_nulls("name")
            )
        else:
            raise TypeError(f"Unsupported dtype for medicinal_product_llm_clean: {col_dtype}")

        # FREQUENCY SORTING: Process popular drugs first (before proxy dies)
        name_frequencies = names_exploded.group_by("name").agg(pl.len().alias("frequency"))
        sorted_names_df = name_frequencies.sort("frequency", descending=True)
        unique_names = sorted_names_df["name"].to_list()

        logger.info("[S07] Cohort %s: %d unique drug names to process (sorted by frequency desc)", cohort, len(unique_names))
        logger.info("[S07] Top 5 most frequent drugs: %s",
                   [f"{name}({freq})" for name, freq in zip(sorted_names_df["name"].head(5), sorted_names_df["frequency"].head(5))])

        # Find names not in cache
        missing = [name for name in unique_names if name not in global_cache]
        if missing:
            logger.info("[S07] Cohort %s: %d names need enrichment (cache hits: %d)", 
                       cohort, len(missing), len(unique_names) - len(missing))
            asyncio.run(_enrich_names(ctx, missing, global_cache, label=cohort))

        # Build mapping DataFrame from cache
        map_rows = []
        for name in unique_names:
            result = global_cache.get(name)
            if result:
                map_rows.append({
                    "name": name,
                    "rxcui": result.rxcui,
                    "rxcui_tty": result.rxcui_tty,
                    "ingredients": result.ingredients,
                    "ingredient_count": result.ingredient_count,
                })
            else:
                map_rows.append({
                    "name": name,
                    "rxcui": None,
                    "rxcui_tty": None,
                    "ingredients": [],
                    "ingredient_count": 0,
                })
        
        map_df = pl.DataFrame(map_rows).with_columns(
            pl.col("name").cast(pl.Utf8),
            pl.col("rxcui").cast(pl.Utf8, strict=False),
            pl.col("rxcui_tty").cast(pl.Utf8, strict=False),
            pl.col("ingredients").cast(pl.List(pl.Utf8), strict=False),
            pl.col("ingredient_count").cast(pl.Int32, strict=False),
        )

        # Join enrichment data back to original rows
        joined = names_exploded.join(map_df, on="name", how="left")

        # Aggregate per row_id (for List[Utf8] drug names, aggregate across all names)
        # Take first non-null rxcui and rxcui_tty, aggregate ingredients
        rx_grouped = (
            joined
            .group_by("row_id")
            .agg([
                pl.col("rxcui").drop_nulls().first().alias("rxcui"),
                pl.col("rxcui_tty").drop_nulls().first().alias("rxcui_tty"),
            ])
        )

        ing_grouped = (
            joined
            .select("row_id", "ingredients")
            .explode("ingredients")
            .drop_nulls()
            .group_by("row_id")
            .agg(pl.col("ingredients").unique().alias("ingredients"))
        )

        # Build final enriched DataFrame
        enriched = (
            df_with_id
            .join(rx_grouped, on="row_id", how="left")
            .join(ing_grouped, on="row_id", how="left")
            .with_columns([
                # Ensure ingredients is never null (use empty list)
                pl.when(pl.col("ingredients").is_null())
                .then(pl.lit([], dtype=pl.List(pl.Utf8)))
                .otherwise(pl.col("ingredients"))
                .alias("ingredients"),
            ])
            .with_columns([
                # Calculate ingredient_count
                pl.col("ingredients").list.len().cast(pl.Int32).alias("ingredient_count"),
            ])
            .drop("row_id")
        )

        # Write output
        out_path = output_dir / f"{cohort}_drugs_enriched.parquet"
        enriched.write_parquet(out_path, compression="zstd")

        # Calculate statistics
        total_rows = enriched.height
        mapped_rows = enriched.filter(pl.col("rxcui").is_not_null()).height
        coverage = 0.0 if total_rows == 0 else mapped_rows / total_rows * 100
        
        # Count by source
        sources_stats = {}
        for name in unique_names:
            result = global_cache.get(name)
            if result:
                sources_stats[result.source] = sources_stats.get(result.source, 0) + 1
        
        manifest_payload[cohort] = {
            "drug_rows": total_rows,
            "mapped_rows": mapped_rows,
            "coverage_pct": round(coverage, 2),
            "unique_names": len(unique_names),
            "source_breakdown": sources_stats,
            "output_path": str(out_path),
        }
        
        # Detailed coverage analysis
        rxnav_exact = sources_stats.get("rxnav_exact", 0)
        pubchem_fallback = sources_stats.get("pubchem_fallback", 0)
        not_found = sources_stats.get("not_found", 0)

        logger.info("[S07] Cohort %s: ✅ COVERAGE %.2f%% (%d/%d drugs enriched)",
                   cohort, coverage, mapped_rows, len(unique_names))
        logger.info("[S07] Cohort %s: 📊 RxNav direct: %d, PubChem fallback: %d, Not found: %d",
                   cohort, rxnav_exact, pubchem_fallback, not_found)

        # Quality assessment
        if coverage >= 90:
            logger.success("[S07] Cohort %s: 🌟 EXCELLENT coverage (%.1f%%) - Pipeline highly successful!", cohort, coverage)
        elif coverage >= 80:
            logger.success("[S07] Cohort %s: ✅ GOOD coverage (%.1f%%) - Ready for production!", cohort, coverage)
        elif coverage >= 70:
            logger.warning("[S07] Cohort %s: ⚠️  MODERATE coverage (%.1f%%) - Acceptable for research", cohort, coverage)
        else:
            logger.error("[S07] Cohort %s: ❌ LOW coverage (%.1f%%) - May need data quality improvements", cohort, coverage)

    # Overall statistics across all cohorts
    total_drugs_all = sum(stats["unique_names"] for stats in manifest_payload.values())
    total_enriched_all = sum(stats["mapped_rows"] for stats in manifest_payload.values())
    overall_coverage = total_enriched_all / total_drugs_all * 100 if total_drugs_all > 0 else 0

    logger.info("[S07] 📈 OVERALL SUMMARY:")
    logger.info("[S07] Total unique drugs processed: %d", total_drugs_all)
    logger.info("[S07] Total drugs enriched: %d", total_enriched_all)
    logger.info("[S07] Overall coverage: %.2f%%", overall_coverage)

    write_manifest(
        ctx,
        "s07_enrich_drug_identifiers",
        {
            "stage": "s07_enrich_drug_identifiers",
            "version": "3.0",
            "search_logic": "smart_bandwidth_optimization_with_circuit_breaker",
            "features": [
                "frequency_sorting_priority",
                "hybrid_sessions_direct_proxy",
                "circuit_breaker_error_detection",
                "bandwidth_optimization",
                "ip_rotation_force_close"
            ],
            "circuit_breaker_max_errors": MAX_CONSECUTIVE_ERRORS,
            "max_pubchem_retries": 2,
            "rxnav_concurrency": 30,
            "pubchem_concurrency": 100,
            "connector_limit": 200,
            "overall_coverage_pct": round(overall_coverage, 2),
            "total_drugs_processed": total_drugs_all,
            "total_drugs_enriched": total_enriched_all,
            "cohorts": manifest_payload,
        }
    )

    if overall_coverage >= 80:
        logger.success("[S07] 🎉 PIPELINE SUCCESS: %.1f%% overall coverage - Production ready!", overall_coverage)
    else:
        logger.warning("[S07] ⚠️  PIPELINE COMPLETE: %.1f%% overall coverage - Monitor and improve data quality", overall_coverage)
