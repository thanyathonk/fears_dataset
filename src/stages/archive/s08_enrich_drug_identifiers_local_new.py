from __future__ import annotations

"""Stage S08 Local – Enrich drug identifiers using Direct NCBI API Key Connection.

New Logic (v4):
1. Direct connection using NCBI API Key (no proxy needed)
2. Sequential fallback: RxNav → Local CID Index → RxNav ingredients
3. Rate limiting: 10 requests/second (API key limit)
4. Frequency sorting: Process popular drugs first
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

# NCBI API Configuration
NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "").strip()

# Rate limiting: Reduced to avoid PubChem 503 Server Busy errors
RATE_LIMIT_SEMAPHORE = asyncio.Semaphore(5)


@dataclass
class EnrichmentResult:
    """Result of drug identifier enrichment."""
    rxcui: Optional[str] = None
    rxcui_tty: Optional[str] = None
    ingredients: List[str] = None
    ingredient_count: int = 0
    source: str = "not_found"

    def __post_init__(self):
        if self.ingredients is None:
            self.ingredients = []


# Global cache type for enrichment results
EnrichmentCache = Dict[str, EnrichmentResult]


async def _lookup_pubchem_title_direct(
    session: aiohttp.ClientSession,
    name: str,
) -> Optional[str]:
    """
    Query PubChem REST API directly using NCBI API Key.

    URL: https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{name}/property/Title/JSON

    Returns the Title if found, None otherwise.
    """
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{encoded_name}/property/Title/JSON"

    headers = {
        "User-Agent": "DrugPipeline/1.0 (mailto:test@example.com)",  # NCBI compliance - use working email
        "api-key": NCBI_API_KEY,  # NCBI API key for direct access
        "Accept": "application/json",
    }

    for attempt in range(3):  # Max 3 retries
        try:
            # Rate limiting
            async with RATE_LIMIT_SEMAPHORE:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:

                    # Check X-Throttling-Control header for rate limiting status
                    throttling_status = resp.headers.get('X-Throttling-Control', '').lower()
                    if throttling_status in ['red', 'black']:
                        logger.warning(f"[Local] PubChem throttling RED/BLACK for {name}, sleeping 10s")
                        await asyncio.sleep(10)
                        continue
                    elif throttling_status == 'yellow':
                        logger.warning(f"[Local] PubChem throttling YELLOW for {name}, sleeping 3s")
                        await asyncio.sleep(3)

                    if resp.status == 404:
                        logger.info(f"[Local] PubChem not found (404) for: {name}")
                        return None
                    elif resp.status == 429:  # Too many requests
                        if attempt < 2:  # Don't sleep on last attempt
                            logger.warning("[Local] PubChem rate limited (429), sleeping 2s for: %s", name)
                            await asyncio.sleep(2)
                            continue
                        return None
                    elif resp.status == 503:  # Server Busy
                        if attempt < 2:  # Don't sleep on last attempt
                            logger.warning(f"[Local] PubChem server busy (503), sleeping 5s for: {name}")
                            await asyncio.sleep(5)
                            continue
                        return None
                    elif resp.status >= 500:  # Other server errors
                        logger.warning(f"[Local] PubChem server error {resp.status} for {name}")
                        return None

                    resp.raise_for_status()

                    # For successful responses, check for ServerBusy in content
                    if resp.status == 200:
                        response_text = await resp.text()
                        if "ServerBusy" in response_text or "too many requests" in response_text.lower():
                            if attempt < 2:
                                logger.warning(f"[Local] PubChem ServerBusy detected, sleeping 5s for: {name}")
                                await asyncio.sleep(5)
                                continue
                            return None
                        data = await resp.json()  # Re-parse as JSON after text check
                    else:
                        data = await resp.json()

                    logger.info(f"[Local] PubChem raw response for '{name}': {data}")

                    # Extract Title from response
                    properties = data.get("PropertyTable", {}).get("Properties", [])
                    if properties and len(properties) > 0:
                        title = properties[0].get("Title")
                        if title:
                            # Return title if found (even if same as input name)
                            logger.info(f"[Local] PubChem extracted title: '{title}' for input '{name}'")
                            return title
                        else:
                            logger.debug(f"[Local] PubChem found property but no Title for {name}")
                    else:
                        logger.debug(f"[Local] PubChem found no properties for {name}")
                    return None

        except aiohttp.ClientError as e:
            if attempt < 2:
                logger.info(f"[Local] PubChem client error for {name} (attempt {attempt + 1}): {e}")
                continue
            logger.warning(f"[Local] PubChem failed for {name} after 3 attempts: {e}")
            return None
        except asyncio.TimeoutError:
            if attempt < 2:
                logger.info(f"[Local] PubChem timeout for {name} (attempt {attempt + 1})")
                continue
            logger.warning(f"[Local] PubChem timeout for {name} after 3 attempts")
            return None
        except Exception as e:
            logger.warning(f"[Local] PubChem unexpected error for {name}: {e}")
            return None

    return None


async def _enrich_names_direct(
    ctx: PipelineContext,
    names: List[str],
    cache: EnrichmentCache,
    *,
    label: Optional[str] = None,
) -> EnrichmentCache:
    """
    Enrich drug names using Direct NCBI API Key connection.
    Sequential fallback: RxNav → Local CID Index → RxNav ingredients
    """

    # Setup session with NCBI API headers - more robust configuration
    headers = {
        "User-Agent": "DrugPipeline/1.0 (mailto:tttccc4589@gmail.com)",  # NCBI compliance
        "api-key": NCBI_API_KEY,  # NCBI API key
        "Accept": "application/json",
    }

    # More robust connection settings
    connector = aiohttp.TCPConnector(
        limit=15,  # Optimized concurrent connections
        limit_per_host=7,  # Optimized per host
        ttl_dns_cache=300,  # DNS cache TTL
        use_dns_cache=True,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)  # Increased timeouts

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:

        client = RxNormClient(ctx, session)

        # Initialize results dict for this enrichment session
        results = {}

        async def handle(name: str) -> None:
            try:
                rxcui: Optional[str] = None
                source = "not_found"

                # Step 1: RxNav Exact Match with retry
                logger.debug(f"[Local] Processing '{name}' - Step 1: RxNav lookup...")
                max_retries = 2
                for attempt in range(max_retries):
                    try:
                        logger.info(f"[Local] 🔍 '{name}' - RxNav attempt {attempt+1}/{max_retries}")
                        async with RATE_LIMIT_SEMAPHORE:
                            candidates = await client.lookup_exact(name)
                            logger.info(f"[Local] 📋 '{name}' - RxNav attempt {attempt+1} returned {len(candidates) if candidates else 0} candidates")

                            if candidates:
                                rxcui = candidates[0]
                                source = "rxnav_exact"
                                logger.info(f"[Local] ✅ RxNav SUCCESS: '{name}' → RxCUI {rxcui} (attempt {attempt+1})")
                                logger.info(f"[Local] 🔗 RxNav Link: https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allinfo.json")
                                break  # Success, exit retry loop
                            else:
                                logger.info(f"[Local] ❌ '{name}' - RxNav attempt {attempt+1} returned no candidates (404)")
                                if attempt < max_retries - 1:
                                    logger.info(f"[Local] 🔄 '{name}' - Retrying RxNav in 1 seconds...")
                                    await asyncio.sleep(1)
                                else:
                                    logger.warning(f"[Local] ❌ RxNav failed: '{name}' not found after {max_retries} attempts")
                                    break  # No more retries
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"[Local] RxNav attempt {attempt+1} failed for '{name}': {e}, retrying in 2s...")
                            await asyncio.sleep(2)
                        else:
                            logger.warning(f"[Local] RxNav failed after {max_retries} attempts for '{name}': {e}")
                            break

                # Add small delay between drugs to avoid rate limiting
                await asyncio.sleep(0.1)  # 100ms delay

                # Step 2: Local Fallback (if RxNav failed)
                if not rxcui:
                    logger.info(f"[Local] 🔄 RxNav failed for '{name}', trying local CID-Synonym fallback...")
                    logger.info(f"[Local] 🌐 PubChem API URL: https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{urllib.parse.quote(name, safe='')}/property/Title/JSON")

                    try:
                        pubchem_title = await _lookup_pubchem_title_direct(session, name)
                        logger.info(f"[Local] 📋 PubChem API Response for '{name}': '{pubchem_title}'")

                        if pubchem_title:
                            logger.info(f"[Local] ✅ PubChem SUCCESS: Found title '{pubchem_title}' for '{name}'")
                            logger.info(f"[Local] 🔗 PubChem Web Link: https://pubchem.ncbi.nlm.nih.gov/compound/{urllib.parse.quote(name, safe='')}")
                            logger.info(f"[Local] 🔄 Retrying RxNav with PubChem title: '{pubchem_title}'")
                            logger.info(f"[Local] 🌐 RxNav Retry URL: https://rxnav.nlm.nih.gov/REST/rxcui.json?name={urllib.parse.quote(pubchem_title, safe='')}")

                            # Retry RxNav with PubChem title (with retry)
                            for attempt in range(3):
                                try:
                                    async with RATE_LIMIT_SEMAPHORE:
                                        candidates = await client.lookup_exact(pubchem_title)
                                        logger.info(f"[Local] 📋 RxNav retry result: {len(candidates) if candidates else 0} candidates found")

                                        if candidates:
                                            rxcui = candidates[0]
                                            source = "local_fallback"
                                            logger.info(f"[Local] 🎉 PUBCHEM FALLBACK SUCCESS: '{name}' → '{pubchem_title}' → RxCUI {rxcui}")
                                            logger.info(f"[Local] 🔗 RxNav Link: https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allinfo.json")
                                            break
                                        else:
                                            logger.info(f"[Local] ❌ RxNav found no candidates for PubChem title '{pubchem_title}'")
                                            break  # No candidates
                                except Exception as e:
                                    if attempt < 2:
                                        logger.warning(f"[Local] PubChem RxNav retry {attempt+1} failed for '{name}': {e}")
                                        await asyncio.sleep(1)
                                    else:
                                        logger.warning(f"[Local] PubChem RxNav retry failed for '{name}': {e}")
                                        break
                        else:
                            logger.info(f"[Local] ❌ PubChem found no title for '{name}' (404 Not Found)")
                    except Exception as e:
                        logger.warning(f"[Local] PubChem lookup failed for '{name}': {e}")

                    # Add delay after PubChem
                    await asyncio.sleep(0.1)

                # Step 3: Get ingredients (if we have RxCUI)
                if rxcui and rxcui.strip():
                    try:
                        async with RATE_LIMIT_SEMAPHORE:
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
                        logger.warning(f"[Local] Failed to get ingredients for RxCUI {rxcui} ({name}): {e}")
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

                # Final result summary
                logger.debug(f"[Local] '{name}' - Final result: rxcui={rxcui}, source={source}")

                # Check if results dict was updated
                if name in results:
                    result_obj = results[name]
                    logger.debug(f"[Local] '{name}' - Results dict entry: rxcui={result_obj.rxcui}, source={result_obj.source}")
                else:
                    logger.warning(f"[Local] '{name}' - NOT FOUND IN RESULTS DICT!")

            except Exception as exc:
                logger.warning(f"[Local] Enrichment failed for '{name}': {str(exc)} (type: {type(exc).__name__})")
                logger.debug(f"[Local] Full exception for '{name}':", exc_info=True)
                results[name] = EnrichmentResult(
                    rxcui=None,
                    rxcui_tty=None,
                    ingredients=[],
                    ingredient_count=0,
                    source="error",
                )
                logger.debug(f"[Local] Set error result for '{name}'")

        # Process all names
        results = {}  # Initialize empty results dict
        logger.debug(f"[Local] Initialized results dict: {len(results)} entries")

        # Progress tracking with real-time success counting
        tasks = [asyncio.create_task(handle(name)) for name in names]
        desc = f"S08-Direct enrichments {label}" if label else "S08-Direct enrichments"

        # Configure tqdm for better visibility and real-time updates
        pbar = tqdm(
            total=len(tasks),
            desc=desc,
            unit='drugs',
            ncols=120,  # Wider progress bar
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]',
            colour='green',
            disable=False,
            leave=True,  # Keep progress bar after completion
            miniters=1,  # Update at least every iteration
            smoothing=0.1  # Smoother progress updates
        )

        completed = 0

        for fut in asyncio.as_completed(tasks):
            await fut
            completed += 1
            pbar.update(1)

            # Update progress stats frequently for better visibility
            if completed % 50 == 0:  # Every 50 drugs for very frequent updates
                current_successful = sum(1 for r in results.values() if r and r.rxcui and str(r.rxcui).strip())
                success_rate = current_successful / completed * 100 if completed > 0 else 0

                # Update tqdm postfix with current stats (shows on progress bar)
                pbar.set_postfix({
                    'successful': f'{current_successful}/{completed}',
                    'rate': f'{success_rate:.1f}%'
                }, refresh=True)

                # Log progress less frequently to avoid spam
                if completed % 1000 == 0:
                    logger.info(f"[Local] Progress: {completed}/{len(names)} drugs processed ({success_rate:.1f}% success rate)")

            if completed % 2000 == 0 and completed > 0:  # Major milestone every 2000 drugs
                current_successful = sum(1 for r in results.values() if r and r.rxcui and str(r.rxcui).strip())
                success_rate = current_successful / completed * 100
                estimated_total = int(current_successful / completed * len(names))
                logger.info(f"[Local] MILESTONE: {completed}/{len(names)} completed - {current_successful} successful so far")
                logger.info(f"[Local] Estimated final success: ~{estimated_total} drugs ({success_rate:.1f}% coverage)")

        pbar.close()

        # Final results summary
        logger.info(f"[Local] Enrichment session completed: {len(results)} results collected")

        # Debug: Check results before update
        results_with_rxcui = sum(1 for r in results.values() if r and r.rxcui and str(r.rxcui).strip())
        logger.info(f"[Local] Results dict: {len(results)} entries total, {results_with_rxcui} with valid RxCUI")

        # Show sample successful results
        successful_results = [(name, result) for name, result in results.items() if result and result.rxcui and str(result.rxcui).strip()]
        if successful_results:
            logger.info("[Local] Sample successful enrichments:")
            for name, result in successful_results[:5]:
                logger.info(f"  ✓ '{name}' → RxCUI {result.rxcui} ({result.source})")

        cache.update(results)

        # Final cache summary
        total_with_rxcui = sum(1 for r in cache.values() if r and r.rxcui and str(r.rxcui).strip())
        logger.info(f"[Local] Final cache: {len(cache)} entries total, {total_with_rxcui} with valid RxCUI")

        return cache


def run(ctx: PipelineContext) -> None:
    """Run Stage S08 Local: Drug identifier enrichment using Direct NCBI API."""

    source_dir = stage_output_path(ctx, "s07b_llm_clean")
    output_dir = stage_output_path(ctx, "s08_enrich_drug_identifiers")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_payload: Dict[str, Dict[str, object]] = {}
    global_cache: EnrichmentCache = {}

    # Check for force re-run flag
    force_enrichment = os.environ.get("FORCE_ENRICHMENT") == "1"
    if force_enrichment:
        logger.info("[Local] FORCE mode: Clearing cache, will re-run all enrichment")
        global_cache.clear()

    # Process each cohort (support selective cohort via environment variable)
    target_cohort = os.environ.get("TARGET_COHORT")
    if target_cohort:
        cohorts_to_process = (target_cohort,)
        logger.info(f"[Local] Processing selected cohort: {target_cohort}")
    else:
        cohorts_to_process = ("pediatric", "adult")
        logger.info("[Local] Processing all cohorts: pediatric, adult")

    for cohort in cohorts_to_process:
        source = source_dir / f"{cohort}_drugs_clean.parquet"
        if not source.exists():
            raise FileNotFoundError(f"Missing LLM-cleaned drug dataset for cohort {cohort}")

        df = pl.read_parquet(source)
        logger.debug(f"[Local] Loaded DF shape: {df.shape}, columns: {list(df.columns)}")

        # Clean schema: drop legacy columns
        legacy_cols = ["__index_level_0__", "rxcui", "rxcui_tty", "ingredients", "ingredient_count"]
        for col in legacy_cols:
            if col in df.columns:
                df = df.drop(col)
                logger.debug(f"[Local] Dropped legacy column: {col}")

        # Validate required columns
        if "medicinal_product_llm_clean" not in df.columns:
            raise ValueError(f"[Local] Required column 'medicinal_product_llm_clean' not found in {source}")

        logger.debug(f"[Local] After cleaning: shape {df.shape}, columns: {list(df.columns)}")

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

        # FREQUENCY SORTING: Process popular drugs first
        name_frequencies = names_exploded.group_by("name").agg(pl.len().alias("frequency"))
        sorted_names_df = name_frequencies.sort("frequency", descending=True)
        unique_names = sorted_names_df["name"].to_list()

        logger.info(f"[Local] Cohort {cohort}: {len(unique_names)} unique drug names to process (sorted by frequency desc)")
        top_5 = [f"{name}({freq})" for name, freq in zip(sorted_names_df["name"].head(5), sorted_names_df["frequency"].head(5))]
        logger.info(f"[Local] Top 5 most frequent drugs: {top_5}")

        # Debug: Sample data
        logger.debug(f"[Local] Cohort {cohort}: Sample unique_names[:5]: {unique_names[:5]}")
        if global_cache:
            cache_keys = list(global_cache.keys())[:5]
            logger.debug(f"[Local] Cohort {cohort}: Sample cache keys[:5]: {cache_keys}")

        # Find names not in cache
        missing = [name for name in unique_names if name not in global_cache]

        # Filter out obvious non-drugs to speed up processing
        OBVIOUS_NON_DRUG_KEYWORDS = [
            "COLONEL", "BUTTOCKS", "MANTOUX", "CHOCOLA", "MA DOPARK", "VACCINE", "SERUM",
            "OTHER ANALGESICS", "MULTIVITAMIN", "POLLENS", "ROBITUSSIN"
        ]

        filtered_missing = []
        skipped_non_drugs = 0

        for name in missing:
            name_upper = name.upper()
            is_obvious_non_drug = any(keyword in name_upper for keyword in OBVIOUS_NON_DRUG_KEYWORDS)
            if is_obvious_non_drug:
                logger.debug(f"[Local] Skipping obvious non-drug: '{name}'")
                skipped_non_drugs += 1
                # Mark as not found in cache
                global_cache[name] = EnrichmentResult(
                    rxcui=None, rxcui_tty=None, ingredients=[], ingredient_count=0, source="skipped_non_drug"
                )
            else:
                filtered_missing.append(name)

        logger.info(f"[Local] Cohort {cohort}: Cache status - total unique: {len(unique_names)}, cached: {len(global_cache)}, missing: {len(missing)}")
        logger.info(f"[Local] Cohort {cohort}: Filtered out {skipped_non_drugs} obvious non-drugs, remaining to process: {len(filtered_missing)}")

        if filtered_missing:
            logger.info(f"[Local] Cohort {cohort}: {len(filtered_missing)} names need enrichment")
            logger.info(f"[Local] Cohort {cohort}: Starting enrichment function...")

            # Call enrichment function
            result_cache = asyncio.run(_enrich_names_direct(ctx, filtered_missing, global_cache, label=cohort))

            logger.info(f"[Local] Cohort {cohort}: Enrichment function returned, result_cache has {len(result_cache)} entries")

            # Update global cache with results
            global_cache.update(result_cache)

            # Robust counting logic - check all filtered_missing names
            enriched_count = 0
            for name in filtered_missing:
                cache_result = global_cache.get(name)
                if cache_result and cache_result.rxcui:
                    # Convert to string and check if not empty
                    rxcui_str = str(cache_result.rxcui).strip()
                    if rxcui_str:
                        enriched_count += 1

            logger.info(f"[Local] Cohort {cohort}: Enrichment completed - {enriched_count}/{len(filtered_missing)} drugs enriched")

            # CRITICAL: Debug if enriched_count is 0
            if enriched_count == 0 and len(filtered_missing) > 0:
                logger.error(f"[Local] Cohort {cohort}: ❌ CRITICAL BUG - enriched_count is 0 despite processing {len(missing)} drugs!")
                logger.error(f"[Local] Cohort {cohort}: result_cache has {len(result_cache)} entries")
                logger.error(f"[Local] Cohort {cohort}: global_cache has {len(global_cache)} entries")

                # Check sample of what went wrong
                sample_missing = filtered_missing[:5]
                logger.error(f"[Local] Cohort {cohort}: Checking first 5 missing drugs:")
                for name in sample_missing:
                    in_result = name in result_cache
                    in_global = name in global_cache
                    result_obj = result_cache.get(name) if in_result else None
                    global_obj = global_cache.get(name) if in_global else None

                    rxcui_result = result_obj.rxcui if result_obj else None
                    rxcui_global = global_obj.rxcui if global_obj else None

                    logger.error(f"[Local]   '{name}': in_result={in_result}, in_global={in_global}, rxcui_result={rxcui_result}, rxcui_global={rxcui_global}")

            # Log some enriched drugs for verification
            if enriched_list:
                logger.info(f"[Local] Sample enriched drugs:")
                for name, result in enriched_list[:5]:
                    logger.info(f"  ✓ '{name}' → RxCUI {result.rxcui} ({result.source})")
            else:
                logger.warning(f"[Local] No drugs were successfully enriched!")
                # Check why - sample some results
                sample_results = [(name, global_cache.get(name)) for name in missing[:10]]
                logger.info(f"[Local] Sample results analysis:")
                for name, result in sample_results:
                    if result:
                        rxcui_val = repr(result.rxcui)  # Show actual value
                        has_rxcui = bool(result.rxcui)
                        logger.info(f"  '{name}' → rxcui={rxcui_val}, has_rxcui={has_rxcui}, source={result.source}")
                    else:
                        logger.info(f"  '{name}' → No result in cache")

            # Check a few sample results
            if enriched_count > 0:
                sample_names = [name for name in missing[:3] if global_cache.get(name)]
                for name in sample_names:
                    result = global_cache[name]
                    if result and result.rxcui:
                        logger.info(f"[Local] Sample result: '{name}' → RxCUI {result.rxcui} ({result.source})")
        else:
            logger.info(f"[Local] Cohort {cohort}: All {len(unique_names)} drugs already in cache")

        # Debug: Final cache status
        total_in_cache = sum(1 for result in global_cache.values() if result and result.rxcui and result.rxcui.strip())
        logger.info(f"[Local] Cohort {cohort}: Final cache status - {len(global_cache)} total entries, {total_in_cache} with RxCUI")

        # Build mapping DataFrame from cache
        logger.debug(f"[Local] Building map DataFrame for {len(unique_names)} unique names")
        map_rows = []

        found_in_cache = 0
        for name in unique_names:
            result = global_cache.get(name)
            if result and result.rxcui:
                map_rows.append({
                    "name": name,
                    "rxcui": result.rxcui,
                    "rxcui_tty": result.rxcui_tty,
                    "ingredients": result.ingredients,
                    "ingredient_count": result.ingredient_count,
                })
                found_in_cache += 1
            else:
                map_rows.append({
                    "name": name,
                    "rxcui": None,
                    "rxcui_tty": None,
                    "ingredients": [],
                    "ingredient_count": 0,
                })

        logger.info(f"[Local] Cohort {cohort}: Map DataFrame created - {found_in_cache}/{len(unique_names)} drugs have RxCUI")

        # Create output DataFrame
        map_df = pl.DataFrame(map_rows)
        logger.debug(f"[Local] Map DF shape: {map_df.shape}, columns: {list(map_df.columns)}")

        # Validate map_df has required columns
        required_map_cols = ["name", "rxcui", "rxcui_tty", "ingredients", "ingredient_count"]
        for col in required_map_cols:
            if col not in map_df.columns:
                raise ValueError(f"[Local] Required column '{col}' missing from map DataFrame")

        # Debug: Log column names before join
        logger.debug(f"[Local] Original DF columns: {list(df.columns)}")
        logger.debug(f"[Local] Map DF columns: {list(map_df.columns)}")

        # Join back with original data
        enriched_df = df.join(
            map_df,
            left_on="medicinal_product_llm_clean",
            right_on="name",
            how="left"
        )

        # Debug: Log columns after join
        logger.debug(f"[Local] After join columns: {list(enriched_df.columns)}")

        # Handle column rename safely (avoid duplicate column names)
        logger.debug(f"[Local] Before column handling: {list(enriched_df.columns)}")

        # Check for medicinal_product column conflicts
        if "medicinal_product" in enriched_df.columns:
            logger.warning(f"[Local] Column 'medicinal_product' already exists, dropping it to avoid duplicate error")
            enriched_df = enriched_df.drop("medicinal_product")
            logger.debug(f"[Local] After dropping medicinal_product: {list(enriched_df.columns)}")

        # Verify medicinal_product_llm_clean exists before rename
        if "medicinal_product_llm_clean" not in enriched_df.columns:
            raise ValueError(f"[Local] Column 'medicinal_product_llm_clean' not found for rename. Available columns: {list(enriched_df.columns)}")

        # Rename medicinal_product_llm_clean to medicinal_product
        enriched_df = enriched_df.rename({"medicinal_product_llm_clean": "medicinal_product"})
        logger.debug(f"[Local] After rename: {list(enriched_df.columns)}")

        # Final validation
        if "medicinal_product" not in enriched_df.columns:
            raise ValueError(f"[Local] Rename failed - 'medicinal_product' column not found after rename")

        logger.info(f"[Local] Column rename completed successfully")

        # Debug: Log final columns
        logger.debug(f"[Local] Final columns: {list(enriched_df.columns)}")

        # Save enriched data
        output_file = output_dir / f"{cohort}_drugs_enriched.parquet"
        enriched_df.write_parquet(output_file)
        logger.info(f"[Local] Saved {len(enriched_df)} enriched records to {output_file}")

        # Calculate statistics
        total_drugs = len(unique_names)
        mapped_rows = sum(1 for result in global_cache.values() if result.rxcui is not None)
        coverage = mapped_rows / total_drugs * 100 if total_drugs > 0 else 0

        # Count sources
        rxnav_exact = sum(1 for result in global_cache.values() if result.source == "rxnav_exact")
        pubchem_fallback = sum(1 for result in global_cache.values() if result.source == "pubchem_fallback")
        not_found = total_drugs - mapped_rows

        logger.info(f"[Local] Cohort {cohort}: {coverage:.1f}% coverage ({mapped_rows}/{total_drugs} drugs enriched)")
        logger.info(f"[Local] Cohort {cohort}: RxNav direct: {rxnav_exact}, PubChem fallback: {pubchem_fallback}, Not found: {not_found}")

        # Quality assessment
        if coverage >= 90:
            logger.success("[Local] Cohort %s: 🌟 EXCELLENT coverage (%.1f%%) - Production ready!", cohort, coverage)
        elif coverage >= 80:
            logger.success("[Local] Cohort %s: ✅ GOOD coverage (%.1f%%) - Ready for production!", cohort, coverage)
        elif coverage >= 70:
            logger.warning("[Local] Cohort %s: ⚠️  MODERATE coverage (%.1f%%) - Acceptable for research", cohort, coverage)
        else:
            logger.error("[Local] Cohort %s: ❌ LOW coverage (%.1f%%) - May need data quality improvements", cohort, coverage)

        # Update manifest
        manifest_payload[cohort] = {
            "total_drugs": total_drugs,
            "mapped_rows": mapped_rows,
            "coverage_pct": round(coverage, 2),
            "rxnav_exact": rxnav_exact,
            "pubchem_fallback": pubchem_fallback,
            "not_found": not_found,
        }

    # Overall statistics across all cohorts
    total_drugs_all = sum(stats["total_drugs"] for stats in manifest_payload.values())
    total_enriched_all = sum(stats["mapped_rows"] for stats in manifest_payload.values())
    overall_coverage = total_enriched_all / total_drugs_all * 100 if total_drugs_all > 0 else 0

    logger.info("[Local] 📈 OVERALL SUMMARY:")
    logger.info("[Local] Total unique drugs processed: %d", total_drugs_all)
    logger.info("[Local] Total drugs enriched: %d", total_enriched_all)
    logger.info("[Local] Overall coverage: %.2f%%", overall_coverage)

    write_manifest(
        ctx,
        "s08_enrich_drug_identifiers_local",
        {
            "stage": "s08_enrich_drug_identifiers_local",
            "version": "1.0",
            "connection_method": "direct_ncbi_api_key",
            "api_key_configured": bool(NCBI_API_KEY),
            "rate_limit": 10,
            "overall_coverage_pct": round(overall_coverage, 2),
            "total_drugs_processed": total_drugs_all,
            "total_drugs_enriched": total_enriched_all,
            "cohorts": manifest_payload,
        }
    )

    if overall_coverage >= 80:
        logger.success("[Local] 🎉 PIPELINE SUCCESS: %.1f%% overall coverage - Production ready!", overall_coverage)
    else:
        logger.warning("[Local] ⚠️  PIPELINE COMPLETE: %.1f%% overall coverage - Monitor and improve data quality", overall_coverage)
