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

**Artist Extraction** (from `core/artists.py`):
- **Channel ID matching**: Prioritizes official YouTube channel IDs (e.g., `UC3IZKseVpdzPSBaWxBxundA` → "BLACKPINK")
- **Pattern matching**: Falls back to title/text matching with 30+ K-POP artist patterns
  - Example: "newjeans", "뉴진스" → "NEWJEANS"
  - Example: "bts", "방탄소년단", "防弾少年団" → "BTS"
- **Fallback**: Unknown artists labeled as "OTHER"
- **Supported Artists**: BTS, BLACKPINK, TWICE, NEWJEANS, AESPA, LESSERAFIM, IVE, ITZY, NMIXX, SEVENTEEN, NCT, STRAYKIDS, ATEEZ, ENHYPEN, and 15+ more

**Tier Classification**: CDF-based dynamic cutoffs (configurable via `--top-pct`):
- **Tier 1**: Top performers (default: top 10%)
- **Tier 2**: Upper mid-tier
- **Tier 3**: Lower mid-tier
- **Tier 4**: Baseline performers

**Trend Analysis**: Requires multi-day data accumulation (7-30 days):
- `growth_rate_7d`: 7-day engagement growth rate
- `growth_rate_30d`: 30-day engagement growth rate
- `momentum`: Acceleration/deceleration metric
- `volatility`: Engagement variance over time
- `trend_direction`: RISING/STEADY/FALLING classification

## Commands

### Primary CLI (Unified Entry Point)

All commands use the unified CLI at `video_social_rtp.cli`. This is the **only recommended way** to run the pipeline:

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
python -m video_social_rtp.cli train --no-mlflow

# 05. Launch Streamlit UI
python -m video_social_rtp.cli ui --port 8501
```

### Makefile Shortcuts

The Makefile provides shortcuts for CLI commands:

```bash
# CLI-based commands (recommended)
make fetch_cli   # Run: python -m video_social_rtp.cli fetch
make bronze_cli  # Run: python -m video_social_rtp.cli bronze
make silver_cli  # Run: python -m video_social_rtp.cli silver
make gold_cli    # Run: python -m video_social_rtp.cli gold --top-pct 0.9
make train_cli   # Run: python -m video_social_rtp.cli train
make ui_cli      # Run: python -m video_social_rtp.cli ui

# Legacy snippet commands (NOT recommended - for reference only)
make fetch       # Run old snippets (deprecated)
make bronze      # Run old snippets (deprecated)
make silver      # Run old snippets (deprecated)
```

**Note**: The `make scaffold`, `make fetch`, `make bronze`, etc. targets point to old snippet files in `video-social-rtp-snippets/` and are kept for backward compatibility. **Always use the `_cli` suffix targets** or run CLI commands directly.

### Fallback Mode (No Spark/Delta)

Add `--fallback-local` to any stage to use Pandas/CSV instead of Spark/Delta:

```bash
python -m video_social_rtp.cli bronze --fallback-local
python -m video_social_rtp.cli silver --fallback-local
python -m video_social_rtp.cli gold --fallback-local
python -m video_social_rtp.cli train --fallback-local
```

## Configuration

### Environment Setup

1. Copy `.env.example` to `.env`
2. Set `YT_API_KEY` (optional - will auto-generate mock data if missing)
3. Configure paths (defaults to `project/` subdirectories)

**Key Environment Variables** (from `core/config.py`):
- `PROJECT_ROOT`: Base directory for all outputs (default: `project/`)
- `YT_API_KEY` or `YOUTUBE_API_KEY`: YouTube Data API v3 key (optional)
- `LANDING_DIR`: Raw JSON landing zone (default: `${PROJECT_ROOT}/data/landing`)
- `BRONZE_DIR`: Delta Lake bronze table (default: `${PROJECT_ROOT}/data/bronze`)
- `SILVER_DIR`: Delta Lake silver table (default: `${PROJECT_ROOT}/data/silver`)
- `GOLD_DIR`: Gold features output (default: `${PROJECT_ROOT}/data/gold`)
- `CHECKPOINT_DIR`: Spark checkpoints (default: `${PROJECT_ROOT}/chk`)
- `LOG_DIR`: Application logs (default: `${PROJECT_ROOT}/logs`)
- `ARTIFACT_DIR`: Pareto results, tier cutoffs, Bloom filters (default: `${PROJECT_ROOT}/artifacts`)
- `BRONZE_BLOOM_CAPACITY`: Bloom filter capacity (default: 500000)
- `BRONZE_BLOOM_ERROR_RATE`: Bloom filter FPR (default: 0.01)
- `BRONZE_BLOOM_LOOKBACK_DAYS`: Days to check for duplicates (default: 7)

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

One row per artist. Schema from `features/gold.py`:
- `artist` (String): Artist name
- `total_engagement` (Double): Sum of all engagement metrics
- `avg_engagement` (Double): Average engagement per video
- `max_engagement` (Double): Maximum engagement for any video
- `total_videos` (Double): Total number of videos
- `unique_viewers_est` (Double): HyperLogLog estimate of unique viewers
- `growth_rate_7d` (Double): 7-day growth rate (requires historical data)
- `growth_rate_30d` (Double): 30-day growth rate (requires historical data)
- `momentum` (Double): Acceleration metric
- `volatility` (Double): Engagement variance
- `market_share` (Double): Percentage of total market engagement
- `percentile` (Double): CDF percentile (0.0-1.0)
- `tier` (Integer): Tier assignment (1-4)
- `trend_direction` (String): RISING/STEADY/FALLING

**Tier Assignment**: Dynamic CDF-based cutoffs computed from engagement distribution:
- **Tier 1**: Top performers (configurable, default top 10% via `--top-pct`)
- **Tier 2**: Upper mid-tier
- **Tier 3**: Lower mid-tier
- **Tier 4**: Baseline

**Output Formats**:
- Delta/Parquet table: `project/data/gold/features/`
- CSV export: `project/data/gold/features.csv`
- Artifacts: `project/artifacts/gold_tiers.json` (cutoff stats)

### Train Artifacts

**Pareto Front**: `project/artifacts/pareto.json` contains model comparison results

**Models Trained** (from `train/pareto.py`):
- Logistic Regression
- Random Forest Classifier

**Evaluation Metrics**:
- `accuracy`: Classification accuracy
- `f1`: F1 score (weighted)
- `latency_ms`: Inference latency in milliseconds
- `feat_count`: Number of features used

**Pareto Selection**: Non-dominated solutions selected using multi-objective optimization:
- Maximize: accuracy, f1
- Minimize: latency_ms, feat_count

Models on the Pareto front are flagged for deployment consideration.

## Automation

### Data Collection Strategy

**Manual Execution**: The project is designed for manual execution via CLI commands. There are no automation scripts in the repository.

**Recommended Schedule** (if implementing automation):
```bash
# Twice daily execution (9am, 3pm KST)
0 9,15 * * * cd /path/to/Bigdata_Proj && .venv/bin/python -m video_social_rtp.cli fetch && .venv/bin/python -m video_social_rtp.cli bronze && .venv/bin/python -m video_social_rtp.cli silver --once && .venv/bin/python -m video_social_rtp.cli gold

# Weekly model retraining (Sunday 10am KST)
0 10 * * 0 cd /path/to/Bigdata_Proj && .venv/bin/python -m video_social_rtp.cli train --no-mlflow
```

**30-Day Accumulation Period**: The project was designed with a 30-day data collection window (2025-09-30 to 2025-10-30) to gather sufficient data for trend analysis features (7d/30d growth rates, momentum).

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
If `YT_API_KEY` not set, `ingest/youtube.py` synthesizes realistic mock events. This is intentional for API-keyless testing and development.

### Marker Files Prevent Re-Fetching
Landing directory contains `_markers/` subdirectory with files like `{query}_{date}.json` to prevent duplicate fetches of the same query on the same day. Use `--no-skip-if-exists` flag to force re-fetch.

### Empty Input Handling
All stages include guards for empty input:
- Bronze: Skips processing if no landing files
- Silver: Returns empty DataFrame if no Bronze data
- Gold: Emits empty table with correct schema if no Silver data
- Train: Raises FileNotFoundError if Gold features missing

### Bloom Filter Persistence
Bronze stage saves Bloom filter to:
- Binary: `project/artifacts/bronze_bloom.bin`
- Metadata: `project/artifacts/bronze_bloom.json`

The filter is rebuilt from last 7 days of Bronze data if artifacts missing or invalid.

### Spark Local Binding Issues
If seeing "Service 'sparkDriver' could not bind" errors, set `SPARK_LOCAL_IP=127.0.0.1` in `.env` or environment.

### MLflow Optional Dependency
Train stage checks if `mlflow` is importable. Use `--no-mlflow` flag to skip MLflow logging (won't fail if MLflow not installed).

### Delta Lake Format Detection
All stages try reading Delta format first, then fall back to Parquet. This allows graceful degradation when Delta Lake extensions are not available.

## Project Context

**Academic Project**: Built for a Big Data Analytics course assignment at Yonsei University (student_15030)

**Data Domain**: K-POP YouTube analytics (artist popularity trends)

**Timeline**: 30-day data accumulation period (2025-09-30 to 2025-10-30) for manual data collection

**Documentation**:
- `docs/00~05_*.md`: Stage-by-stage design documents in Korean
- `docs/experiment_log_20251016.md`: Execution log with BTS official data
- `README.md`: Project overview and usage guide
- `CLAUDE.md`: This file (AI assistant guidance)

**Directory Structure**:
```
/home/student_15030/Bigdata_Proj/
├── video_social_rtp/        # Main package (unified CLI)
│   ├── cli.py               # CLI entry point
│   ├── core/                # Config, Spark, Bloom, Sampling, Artists
│   ├── ingest/              # YouTube API ingestion
│   ├── bronze/              # Bronze batch processing
│   ├── silver/              # Silver streaming
│   ├── features/            # Gold feature engineering
│   ├── train/               # Pareto model training
│   └── serve/               # Streamlit UI
├── video-social-rtp-snippets/  # Legacy code snippets (deprecated)
├── 실습자료_2024/           # Lab materials and notebooks
├── docs/                    # Documentation
├── examples/                # Example scripts
├── project/                 # Data output directory
│   ├── data/
│   │   ├── landing/        # Raw JSON (NDJSON)
│   │   ├── bronze/         # Delta Lake bronze
│   │   ├── silver/         # Delta Lake silver
│   │   └── gold/           # CSV features
│   ├── chk/                # Spark checkpoints
│   ├── artifacts/          # Bloom filters, Pareto results
│   ├── logs/               # Application logs
│   └── tmp/                # Temporary files
├── Makefile                # Command shortcuts
├── requirements.txt        # Python dependencies
├── .env                    # Environment configuration
└── README.md              # Project overview
```

**Dual-Language Codebase**: Comments/docs in Korean and English
