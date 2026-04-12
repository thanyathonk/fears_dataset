from __future__ import annotations

"""Typer CLI entrypoint for the can-drug pipeline."""

import subprocess
from datetime import timedelta
from pathlib import Path
from time import perf_counter
from typing import Iterable, Optional

import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.settings import load_settings
from src.utils.io import PipelineContext, init_run_context
from src.stages import get_stage_callable, ORDERED_STAGES

app = typer.Typer(help="Reproducible ETL pipeline for FAERS/OpenFDA data.")
console = Console()

# Stages that require ER tables from S02
ER_DEPENDENT_STAGES = {
    "s03_join_partition_age",
    "s05_split_adr",
    "s06_map_omop_meddra",
    "s07_split_drug",
    "s07b_llm_clean",
    "s08_enrich_drug_identifiers",
    "s09_finalize_merge_and_report",
    "s10_package_deliverables",
}


def _require_er_tables(ctx: PipelineContext, stage_keys: Iterable[str]) -> None:
    """Check if ER tables exist before running dependent stages."""
    if not ER_DEPENDENT_STAGES.intersection(stage_keys):
        return

    s02_stage = ctx.config.metadata.get("s02_stream_stage", "s02_entity_format")
    s02_dir = (ctx.config.paths.staging_root / s02_stage).resolve()
    if s02_dir.exists() and any(s02_dir.glob("*.parquet")):
        return

    er_path = ctx.config.metadata.get("er_tables_path", "data/er_tables")
    er_dir = (ctx.config.paths.root / str(er_path)).resolve()
    if not er_dir.exists() or not any(er_dir.glob("*.csv*")):
        console.print(
            "[red]ER tables not found.[/red] Please run S01 (fetch) and S02 (format) "
            "to generate the ER tables directory before running S03-S10."
        )
        console.print(f"  Checked: {s02_dir}")
        console.print(f"  Checked: {er_dir}")
        raise typer.Exit(code=1)


def _run_selected(
    stage_keys: Iterable[str],
    run_id: Optional[str],
    skip_stages: Optional[Iterable[str]] = None,
) -> None:
    """Run selected stages in sequence."""
    skip = set(skip_stages or [])

    config = load_settings()
    
    # Display pipeline information
    cwd = Path.cwd().resolve()
    config_path = Path("configs/config.local.yaml").resolve()
    pipeline_type = "full_dataset" if "full_dataset" in str(cwd) else "main"
    
    console.print(f"\n[bold cyan]📋 Pipeline Configuration[/bold cyan]")
    console.print(f"  Pipeline Type: [yellow]{pipeline_type}[/yellow]")
    console.print(f"  Working Directory: [dim]{cwd}[/dim]")
    console.print(f"  Config Path: [dim]{config_path}[/dim]")
    console.print(f"  Root Directory: [dim]{config.paths.root}[/dim]")
    console.print(f"  Output Directory: [dim]{config.paths.output_root}[/dim]")
    console.print("")
    
    ctx = init_run_context(config, run_id=run_id)
    stage_order = list(stage_keys)
    _require_er_tables(ctx, set(stage_order) - skip)

    active_stages = [
        key
        for key in stage_order
        if key not in skip and getattr(config.stages, key, True)
    ]
    if not active_stages:
        console.print("[yellow]No stages are enabled for this command.[/yellow]")
        return

    progress_columns = (
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )

    with Progress(*progress_columns, console=console, transient=True) as progress:
        task_id = progress.add_task("Preparing pipeline...", total=len(active_stages))
        completed = 0
        for key in stage_order:
            if key in skip:
                console.print(f"[yellow]Skipping {key} (skip flag).[/yellow]")
                continue

            toggle = getattr(config.stages, key, True)
            if not toggle:
                console.print(f"[yellow]Skipping {key} (disabled in config).[/yellow]")
                continue

            stage_callable = get_stage_callable(key)
            progress.update(
                task_id,
                description=f"[cyan]Running {key} ({completed + 1}/{len(active_stages)})[/cyan]",
            )

            start_time = perf_counter()
            try:
                stage_callable(ctx)
            except Exception:
                progress.stop()
                console.print(f"[red]Stage {key} failed.[/red]")
                raise
            else:
                duration = timedelta(seconds=perf_counter() - start_time)
                progress.advance(task_id)
                console.print(
                    f"[green]Completed stage {key}[/green] [grey58]({duration})[/grey58]"
                )
                completed += 1


# =============================================================================
# CLI Commands
# =============================================================================

@app.command(name="run-stage")
def run_stage(
    stage: str = typer.Argument(..., help="Stage name (e.g., s07_split_drug)"),
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Run a single stage by name (S01-S10)."""
    if stage not in ORDERED_STAGES:
        console.print(f"[red]Unknown stage: {stage}[/red]")
        console.print(f"[yellow]Available stages: {', '.join(sorted(ORDERED_STAGES))}[/yellow]")
        raise typer.Exit(1)
    _run_selected([stage], run_id, None)


@app.command()
def setup(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Create required directories and validate configuration."""
    config = load_settings()
    ctx = init_run_context(config, run_id=run_id)
    console.print(f"[green]Setup complete. Logs: {ctx.log_path}[/green]")


@app.command()
def fetch(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Run S01 ingestion (OpenFDA download + raw landing)."""
    _run_selected(["s01_fetch_openfda"], run_id, None)


@app.command()
def format(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Run S02 entity formatting (build ER tables)."""
    _run_selected(["s02_entity_format"], run_id, None)


@app.command(name="enrich-local")
def enrich_local(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
    cohort: Optional[str] = typer.Option(None, help="Filter cohort: 'ped' or 'adult'"),
) -> None:
    """Run enrichment stage S08 using local RxNav database."""
    import os

    # Set environment variable for cohort filtering
    if cohort:
        if cohort == "ped":
            os.environ["TARGET_COHORT"] = "pediatric"
        elif cohort == "adult":
            os.environ["TARGET_COHORT"] = "adult"
        else:
            console.print(f"[red]Error: Invalid cohort '{cohort}'. Use 'ped' or 'adult'.[/red]")
            raise typer.Exit(1)
    else:
        os.environ.pop("TARGET_COHORT", None)

    # Use get_stage_callable for consistency
    _run_selected(["s08_enrich_drug_identifiers"], run_id, None)


@app.command()
def finalize(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Run final merge/report stage S09."""
    _run_selected(["s09_finalize_merge_and_report"], run_id, None)


@app.command()
def package(
    run_id: Optional[str] = typer.Option(None, help="Override run identifier."),
) -> None:
    """Package deliverable tables (S10)."""
    _run_selected(["s10_package_deliverables"], run_id, None)


@app.command()
def test() -> None:
    """Execute pytest suite with coverage."""
    console.print("[cyan]Running pytest...[/cyan]")
    subprocess.run(["pytest", "-q"], check=False)


if __name__ == "__main__":
    app()
