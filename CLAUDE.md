# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **K-POP video analytics platform** that collects YouTube data via the YouTube Data API v3 and processes it through a multi-stage big data pipeline to analyze artist popularity trends and predict viral potential.

**Key Technologies**: Apache Spark 3.5.x, Delta Lake, PySpark, Streamlit, YouTube Data API v3, Pandas (fallback mode)

## Architecture: 6-Stage Data Pipeline

The project implements a medallion architecture (Landing → Bronze → Silver → Gold) with ML training and serving stages:

```
YouTube API v3
    ↓
[00. Scaffold] - Directory setup
    ↓
[01. Landing] - Raw JSON storage (NDJSON format)
    ↓ (Reservoir Sampling, Bloom Filter for deduplication)
[01. Bronze] - Delta Lake ingestion with partitioning (ingest_date, artist)
    ↓
[02. Silver] - Structured Streaming aggregation (Sliding Window + Watermark)
    ↓
[03. Gold] - Feature engineering + CDF-based tier labeling (Tier 1-4)
    ↓ (HyperLogLog for cardinality estimation)
[04. Train] - Multi-model training + Pareto Front optimization
    ↓
[05. Serve] - Streamlit dashboard
```

### Artist-Centric Data Model

The pipeline extracts artist names from video titles/channels and partitions data by artist. Key features:
- **Artist extraction**: Pattern matching from titles (e.g., "NewJeans", "뉴진스", "newjeans" → "NEWJEANS")
- **Tier classification**: CDF-based dynamic cutoffs (Tier 1: top 5%, Tier 2: top 15%, Tier 3: top 40%, Tier 4: rest)
- **Trend analysis**: Growth rates (7d, 30d), momentum, volatility (requires 7-30 days of data accumulation)

## Commands

### Primary CLI (Recommended)

All commands use the unified CLI at `video_social_rtp.cli`:

```bash
# 00. Create project structure
python -m video_social_rtp.cli scaffold

# 01-A. Fetch YouTube data (auto-creates mock data if no API key)
python -m video_social_rtp.cli fetch \
    --query "NewJeans" \
    --max-items 50 \
    --region-code KR \
    --relevance-language ko \
    --skip-if-exists  # Skip if today's query already fetched

# 01-B. Bronze layer (Bloom filter + Delta Lake)
python -m video_social_rtp.cli bronze

# 02. Silver layer (Structured Streaming with --once for batch mode)
python -m video_social_rtp.cli silver --once

# 03. Gold layer (CDF-based tier labeling)
python -m video_social_rtp.cli gold --top-pct 0.9

# 04. Train models (Pareto Front selection)
python -m video_social_rtp.cli train

# 05. Launch Streamlit UI
python -m video_social_rtp.cli ui --port 8501
```

### Makefile Shortcuts

```bash
make scaffold   # Directory setup
make fetch_cli  # Fetch via CLI
make bronze_cli # Bronze processing
make silver_cli # Silver aggregation
make gold_cli   # Gold features (--top-pct 0.9)
make train_cli  # Model training
make ui_cli     # Launch UI
```

### Fallback Mode (No Spark/Delta)

Add `--fallback-local` to any stage to use Pandas/CSV instead of Spark/Delta:

```bash
python -m video_social_rtp.cli bronze --fallback-local
python -m video_social_rtp.cli silver --fallback-local
python -m video_social_rtp.cli gold --fallback-local
```

## Configuration

### Environment Setup

1. Copy `.env.example` to `.env`
2. Set `YT_API_KEY` (optional - will auto-generate mock data if missing)
3. Configure paths (defaults to `project/` subdirectories)

**Key Environment Variables**:
- `PROJECT_ROOT`: Base directory for all outputs (default: `project`)
- `YT_API_KEY`: YouTube Data API v3 key
- `LANDING_DIR`, `BRONZE_DIR`, `SILVER_DIR`, `GOLD_DIR`: Data lake paths
- `CHECKPOINT_DIR`: Spark Structured Streaming checkpoints
- `ARTIFACT_DIR`: MLflow runs, Pareto results, tier cutoffs

### Dual-Mode Operation

The codebase supports two execution paths:
1. **Spark/Delta mode** (primary): Uses PySpark, Delta Lake, Structured Streaming
2. **Local/Pandas mode** (fallback): CSV-based processing when Spark unavailable

Each stage checks environment variables like `BRONZE_FALLBACK_LOCAL` or CLI `--fallback-local` flag.

## Code Organization

### Core Package Structure

```
video_social_rtp/
├── cli.py                 # Unified CLI entry point (click-based)
├── core/
│   ├── config.py          # .env loader, Settings dataclass
│   ├── spark_env.py       # Spark session factory (Delta Lake configured)
│   ├── bloom.py           # Bloom Filter implementation (bitarray)
│   ├── sampling.py        # Reservoir Sampling (Vitter's Algorithm R)
│   ├── artists.py         # Artist name extraction/normalization
│   └── logging.py         # JSON structured logging
├── ingest/
│   └── youtube.py         # YouTube API v3 search.list wrapper
├── bronze/
│   └── batch.py           # Delta Lake ingestion + Bloom dedup
├── silver/
│   └── stream.py          # Structured Streaming (window+watermark)
├── features/
│   └── gold.py            # Feature engineering + CDF tier labeling
├── train/
│   └── pareto.py          # Multi-model training + Pareto Front
└── serve/
    └── ui.py              # Streamlit dashboard
```

### Important Patterns

**Params Pattern**: Each stage has a `Params` dataclass (e.g., `IngestParams`, `SilverParams`, `GoldParams`, `TrainParams`) passed to its `run_*` function.

**Settings Injection**: `load_settings()` from `core.config` returns a `Settings` object with all paths. Call `ensure_dirs(settings)` to create directories.

**Spark Session Creation**: `create_spark()` in `core.spark_env` returns a SparkSession with Delta Lake extensions configured. Always use this factory.

**Bloom Filter Lifecycle**: Bronze stage:
1. Reads last 7 days of Bronze Delta data
2. Builds Bloom filter from `post_id` column
3. Filters Landing events against Bloom (skip if probably duplicate)
4. Appends new rows to Delta table

**Artist Extraction**: `core.artists.py` contains `ARTIST_PATTERNS` dict mapping canonical names to variations. Bronze stage adds `artist` column via pattern matching on `text` field.

## Data Flow Details

### Landing Format (NDJSON)

Each YouTube search creates `landing/events_*.json` with schema:
```python
{
  "post_id": "search{N}_{video_id}",
  "text": "video_title",
  "lang": "en|ko|...",
  "ts": unix_timestamp_ms,
  "author_id": "channel_id",
  "video_id": "yt_video_id",
  "source": "yt"
}
```

### Bronze Schema (Delta Table)

Partitioned by `(ingest_date, artist)`. Columns:
- `post_id`, `text`, `lang`, `ts`, `author_id`, `video_id`, `source`
- `artist`: Extracted via `core.artists.extract_artist_from_text()`
- `ingest_date`: Date string for partitioning

### Silver Schema (Delta Table)

Aggregated by `(window, artist)`. Columns:
- `window`: Struct with `start`, `end` timestamps
- `artist`: Artist name
- `video_count`: Count of events
- `unique_videos`: countDistinct(video_id)
- `total_engagement`: Sum of engagement scores
- `unique_authors`: approx_count_distinct(author_id) via HLL

### Gold Schema (Delta Table + CSV)

One row per artist. Columns:
- `artist`, `total_engagement`, `avg_engagement`, `max_engagement`
- `total_videos`, `unique_viewers_est`
- `growth_rate_7d`, `growth_rate_30d`, `momentum` (requires historical data)
- `market_share`, `percentile`, `tier`, `trend_direction`

**Tier Assignment**: Uses `calculate_tier_cutoffs()` to compute percentiles (95th, 85th, 60th) from `total_engagement` distribution, then assigns Tier 1-4.

**Artifacts**: `artifacts/gold_tiers.json` contains cutoff values and tier distribution stats.

### Train Artifacts

**Pareto Front**: `artifacts/pareto.json` contains model comparison metrics:
- Models: Logistic Regression, Random Forest, Gradient Boosting Tree
- Metrics: Accuracy, F1 Score, Inference Latency (ms), Feature Count
- Pareto-optimal models flagged with `is_pareto: true`

## Automation

### 30-Day Data Accumulation

Scripts in `scripts/`:
- `daily_automation.sh`: Runs fetch→bronze→silver→gold twice daily
- `weekly_analysis.sh`: Weekly reporting
- `setup_cron.sh`: Configure cron jobs

**Cron Schedule** (from README):
```bash
0 9,15 * * * /path/to/daily_automation.sh  # 9am, 3pm daily
0 10 * * 0 /path/to/weekly_analysis.sh     # 10am Sunday
```

## Algorithm References

### Reservoir Sampling
- Implementation: `core/sampling.py`
- Algorithm: Vitter's Algorithm R
- Use case: Extract representative sample from landing data stream

### Bloom Filter
- Implementation: `core/bloom.py`
- Library: `bitarray` for bit vector
- Use case: Deduplicate Bronze ingestion by checking last 7 days of `post_id`
- Parameters: Capacity=500k, FPR=0.01 (configurable via env vars)

### HyperLogLog (Approximate Counting)
- Implementation: Spark's `approx_count_distinct()`
- Use case: Estimate unique viewers/authors in Silver/Gold layers

### CDF-Based Labeling
- Implementation: `features/gold.py` → `calculate_tier_cutoffs()`
- Method: Compute percentiles from engagement distribution
- Dynamic: Cutoffs recalculated on every Gold run (adapts to data drift)

### Pareto Front (Multi-Objective Optimization)
- Implementation: `train/pareto.py`
- Objectives: Maximize accuracy/F1, minimize latency/complexity
- Selection: Non-dominated solutions (models not strictly worse in all metrics)

## Structured Streaming Specifics

**Watermark**: 10 minutes (configurable via `--watermark`)
- Allows late events up to 10 min after event time
- Prevents state bloat from indefinite buffering

**Sliding Window**: 1 hour window, 5 minute slide (configurable)
- Overlapping windows for smooth aggregation
- Example: 09:00-10:00, 09:05-10:05, 09:10-10:10...

**Trigger**: `--once` flag uses `availableNow` trigger (batch mode)
- Processes all available data once and exits
- For continuous streaming, remove `--once`

**Checkpointing**: Stores offsets in `CHECKPOINT_DIR/silver/`
- Required for fault tolerance and exactly-once semantics
- Delete checkpoint dir to reset streaming state

## Important Gotchas

### Mock Data Auto-Generation
If `YT_API_KEY` not set, `ingest/youtube.py` synthesizes realistic mock events. This is intentional for API-keyless testing.

### Marker Files Prevent Re-Fetching
`landing/_markers/{query}_{date}.json` prevents duplicate fetches of same query on same day. Delete marker to force re-fetch.

### Empty Gold Table Handling
`features/gold.py` includes guards for empty Silver input (returns empty results, not errors).

### Spark Local Binding
If seeing "Service 'sparkDriver' could not bind" errors, set `SPARK_LOCAL_IP=127.0.0.1` in `.env`.

### MLflow Dependency
Train stage checks if `mlflow` is importable. If not installed or `--no-mlflow` flag set, skips MLflow logging (doesn't fail).

## Project Context

**Academic Project**: Built for a Big Data Analytics course assignment at Yonsei University (student_15030)

**Data Domain**: K-POP YouTube analytics (artist popularity trends)

**Timeline**: 30-day data accumulation period (2025-09-30 to 2025-10-30) via cron automation

**Documentation**: See `docs/` for stage-by-stage design docs (00-05), `submission/보고서_최종.md` for final report

**Dual-Language Codebase**: Comments/docs in Korean and English
