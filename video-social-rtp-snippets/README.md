# Video/Social Realtime Pipeline — Snippet Pack (Local FS, No Kafka/MinIO)

This is a **snippet-only** pack for coursework. Files contain *concise* code blocks
to illustrate key ideas (reservoir sampling, sliding window + watermark, Bloom filter,
Flajolet–Martin/HLL via approx_count_distinct, CDF/PDF labeling, Pareto-front selection,
and a Streamlit view).

> These are *not* full apps. They’re short, copy-ready excerpts to paste into your own codebases.
> Paths assume local Delta tables under `./data/*`. Adjust as needed.

## Layout
- `jobs/00_fetch_to_landing.py` – mock API fetch to `data/landing` (NDJSON), optional reservoir sampling dump
- `jobs/10_bronze_batch.py` – batch ingest + Bloom filter pre-check
- `jobs/20_silver_stream.py` – file streaming source + sliding window + watermark
- `jobs/30_gold_features.py` – approx unique users (HLL) + CDF/PDF thresholds
- `jobs/40_train_pareto.py` – multi-model metrics + Pareto front selection
- `jobs/50_predict_stream.py` – tiny prediction appender (optional)
- `app/app.py` – Streamlit snippet (CDF/PDF chart + Bloom existence check)

Quick start:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python jobs/00_fetch_to_landing.py
# Submit other snippets with spark-submit (Delta package required)
```

## One-folder Runner (docs 00→05)

This folder now contains a simple one-folder CLI and doc-named runners so you can execute all phases without leaving this directory.

- CLI: `python cli.py [scaffold|fetch|bronze|silver|gold|train|predict|ui]`
- Doc-mapped runners:
  - `00_프로젝트_개요_및_아키텍처.py` – overview + scaffold
  - `01_데이터_수집_및_배치_ETL_설계.py` – fetch → bronze
  - `02_스트리밍_데이터_처리_설계.py` – silver (streaming, blocking)
  - `03_피처_엔지니어링_및_라벨링_설계.py` – gold
  - `04_모델_학습_및_최적화_설계.py` – train
  - `05_실시간_예측_시스템_설계.py` – ui (Streamlit)

Examples:
```bash
# Create local paths under ./data and ./chk
python scaffold_paths.py

# Orchestration via CLI
python cli.py fetch
python cli.py bronze
python cli.py silver   # streaming (Ctrl+C to stop)
python cli.py gold --top-pct 0.9
python cli.py train
python cli.py ui

# Doc-mapped runners (Korean filenames)
python 01_데이터_수집_및_배치_ETL_설계.py
```
