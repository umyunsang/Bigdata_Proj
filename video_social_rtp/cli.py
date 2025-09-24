from __future__ import annotations

import json
import os
import time
import click

from .core.config import load_settings, ensure_dirs
from .ingest.youtube import IngestParams, fetch_to_landing
from .bronze.batch import run_bronze_batch
from .silver.stream import run_silver_stream, SilverParams
from .features.gold import run_gold_features, GoldParams
from .train.pareto import run_train_pareto, TrainParams


@click.group()
def cli() -> None:
    """Video/Social RTP CLI (project build)."""


@cli.command()
def scaffold() -> None:
    """Create project directories from .env (Step 00)."""
    s = load_settings()
    ensure_dirs(s)
    click.echo("Project directories ensured under: %s" % s.project_root)


@cli.command()
@click.option("--query", "query", default="data engineering", show_default=True, help="YouTube search query")
@click.option("--max-items", "max_items", default=100, show_default=True, help="Max items to synthesize/fetch (<=50 per call)")
@click.option("--reservoir-k", "reservoir_k", default=64, show_default=True, help="Reservoir sample size")
@click.option("--region-code", "region_code", default=None, help="Region code (e.g., KR)")
@click.option("--relevance-language", "relevance_language", default=None, help="Relevance language (e.g., ko)")
@click.option("--mock/--no-mock", "mock", default=None, help="Force mock mode (default: auto)")
@click.option("--skip-if-exists/--no-skip-if-exists", "skip_if_exists", default=True, show_default=True, help="Skip if today's same query was fetched")
def fetch(query: str, max_items: int, reservoir_k: int, region_code: str | None, relevance_language: str | None, mock: bool | None, skip_if_exists: bool) -> None:
    """Fetch to landing (Step 01-A). Uses mock if no API key."""
    params = IngestParams(query=query, max_items=max_items, reservoir_k=reservoir_k, region_code=region_code, relevance_language=relevance_language)
    paths = fetch_to_landing(params, use_mock=mock, skip_if_exists=skip_if_exists)
    click.echo(json.dumps(paths, ensure_ascii=False))


@cli.command()
@click.option("--fallback-local/--no-fallback-local", "fallback_local", default=None, help="Force local copy mode if Spark blocked")
def bronze(fallback_local: bool | None) -> None:
    """Landing -> Bronze append with Bloom pre-filter (Step 01-B)."""
    n = run_bronze_batch(fallback_local=fallback_local)
    click.echo(f"Bronze appended rows: {n}")


@cli.command()
@click.option("--watermark", default="10 minutes", show_default=True)
@click.option("--window", "window_size", default="1 hour", show_default=True)
@click.option("--slide", "window_slide", default="5 minutes", show_default=True)
@click.option("--once/--no-once", "once", default=True, show_default=True, help="Trigger once and exit (Spark)")
@click.option("--fallback-local/--no-fallback-local", "fallback_local", default=None)
def silver(watermark: str, window_size: str, window_slide: str, once: bool, fallback_local: bool | None) -> None:
    """Streaming window aggregates to Silver (Step 02)."""
    params = SilverParams(watermark=watermark, window_size=window_size, window_slide=window_slide, once=once)
    run_silver_stream(params=params, fallback_local=fallback_local)


@cli.command()
@click.option("--top-pct", default=0.9, show_default=True, type=float, help="CDF cutoff percentile (0~1)")
@click.option("--fallback-local/--no-fallback-local", "fallback_local", default=None)
def gold(top_pct: float, fallback_local: bool | None) -> None:
    """Compute features and labels to Gold (Step 03)."""
    params = GoldParams(top_pct=top_pct)
    res = run_gold_features(params=params, fallback_local=fallback_local)
    click.echo(json.dumps(res, ensure_ascii=False))

@cli.command()
@click.option("--no-mlflow", is_flag=True, default=False, help="Disable MLflow logging if available")
@click.option("--fallback-local/--no-fallback-local", "fallback_local", default=None)
def train(no_mlflow: bool, fallback_local: bool | None) -> None:
    """Train candidates and select Pareto front (Step 04)."""
    params = TrainParams(use_mlflow=(not no_mlflow))
    res = run_train_pareto(params=params, fallback_local=fallback_local)
    click.echo(json.dumps(res, ensure_ascii=False))


@cli.command()
@click.option("--port", default=8501, show_default=True, type=int)
def ui(port: int) -> None:
    """Launch Streamlit UI (Step 05)."""
    import shutil, subprocess, sys
    from pathlib import Path

    app = Path(__file__).resolve().parents[0] / "serve" / "ui.py"
    exe = shutil.which("streamlit")
    if not exe:
        click.echo("streamlit가 설치되어 있지 않습니다. requirements.txt에 streamlit을 추가/설치 후 다시 시도하세요.")
        click.echo(f"수동 실행: streamlit run {app} --server.port={port}")
        sys.exit(1)
    cmd = [exe, "run", str(app), "--server.port", str(port)]
    click.echo("실행: " + " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        click.echo(f"실행 실패: {e}")
if __name__ == "__main__":
    cli()
