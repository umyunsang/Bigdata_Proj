from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import functions as F, types as T

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session
from ..core.artists import extract_artist


@dataclass
class SilverParams:
    watermark: str = "10 minutes"
    window_size: str = "1 hour"
    window_slide: str = "5 minutes"
    poll_interval_sec: int = 5
    iterations: int = 0  # fallback loop count; 0 means single pass
    once: bool = False   # Spark trigger once


def _parse_minutes(spec: str) -> int:
    parts = spec.split()
    if len(parts) != 2:
        raise ValueError(f"Unsupported duration: {spec}")
    qty = int(parts[0])
    unit = parts[1].lower()
    if unit in ("minute", "minutes", "min", "mins"):
        return qty
    if unit in ("hour", "hours", "hr", "hrs"):
        return qty * 60
    raise ValueError(f"Unsupported duration unit: {spec}")


def _read_ndjson_lines(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _fallback_once(params: SilverParams) -> int:
    s = load_settings()
    ensure_dirs(s)
    out_dir = Path(s.silver_dir) / "social_metrics"
    out_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = Path(s.checkpoint_dir) / "silver"
    chk_dir.mkdir(parents=True, exist_ok=True)

    # load all landing files
    files = sorted(Path(s.landing_dir).glob("*.json"))
    events: List[Dict] = []
    for f in files:
        events.extend(list(_read_ndjson_lines(f)))
    if not events:
        (out_dir / "_EMPTY").touch()
        return 0

    # event_time boundaries
    ts_list = [int(e.get("ts", 0)) for e in events if e.get("ts")]
    if not ts_list:
        (out_dir / "_EMPTY").touch()
        return 0
    max_ts = max(ts_list)
    win_minutes = _parse_minutes(params.window_size)
    cutoff = max_ts - win_minutes * 60 * 1000

    # filter by window and accumulate per artist
    counts: Dict[str, Dict[str, object]] = defaultdict(lambda: {
        "count": 0,
        "videos": set(),
        "authors": set(),
    })
    for e in events:
        try:
            ts = int(e.get("ts", 0))
            if ts >= cutoff and e.get("video_id"):
                artist = extract_artist(e.get("text"), e.get("author_id"))
                bucket = counts[artist]
                bucket["count"] = int(bucket["count"]) + 1
                bucket["videos"].add(str(e["video_id"]))
                if e.get("author_id"):
                    bucket["authors"].add(str(e["author_id"]))
        except Exception:
            continue

    # write a compact CSV to social_metrics (fallback format)
    out_file = out_dir / f"metrics_fallback_{int(time.time())}.csv"
    with out_file.open("w", encoding="utf-8") as f:
        f.write("artist,window_start_ts,window_end_ts,engagement_count,unique_videos,unique_authors\n")
        win_minutes = _parse_minutes(params.window_size)
        win_ms = win_minutes * 60 * 1000
        for artist, stats in counts.items():
            f.write(
                f"{artist},{max_ts - win_ms},{max_ts},{int(stats['count'])},{len(stats['videos'])},{len(stats['authors'])}\n"
            )

    # naive checkpoint marker
    (chk_dir / "_fallback_marker").write_text(str(int(time.time())), encoding="utf-8")
    return len(counts)


def _run_structured_stream(params: SilverParams, log) -> None:
    s = load_settings()
    ensure_dirs(s)

    spark, delta_enabled = get_spark_session("silver_stream", s, log=log)
    try:
        schema = T.StructType([
            T.StructField("post_id", T.StringType(), True),
            T.StructField("video_id", T.StringType(), True),
            T.StructField("author_id", T.StringType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("lang", T.StringType(), True),
            T.StructField("ts", T.LongType(), True),
            T.StructField("source", T.StringType(), True),
        ])

        artist_udf = F.udf(lambda text, channel: extract_artist(text, channel), T.StringType())

        raw_stream = (
            spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 1)
            .json(str(s.landing_dir))
        )

        events = (
            raw_stream
            .dropna(subset=["post_id", "video_id", "ts"])
            .filter((F.col("lang").isNull()) | (F.col("lang") == "en"))
            .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000)))
            .withColumn("artist", artist_udf(F.col("text"), F.col("author_id")))
            .dropna(subset=["artist", "event_time"])
            .withWatermark("event_time", params.watermark)
            .dropDuplicates(["post_id"])
        )

        aggregated = (
            events.groupBy(
                F.window("event_time", params.window_size, params.window_slide),
                F.col("artist"),
            )
            .agg(
                F.count("*").alias("engagement_count"),
                F.approx_count_distinct("video_id").alias("unique_videos"),
                F.approx_count_distinct("author_id").alias("unique_authors"),
            )
        )

        results = (
            aggregated
            .select(
                F.col("artist"),
                (F.col("window.start").cast("long") * 1000).alias("window_start_ts"),
                (F.col("window.end").cast("long") * 1000).alias("window_end_ts"),
                "engagement_count",
                "unique_videos",
                "unique_authors",
            )
        )

        out_path = Path(s.silver_dir) / "social_metrics"
        checkpoint = Path(s.checkpoint_dir) / "silver" / "social_metrics"
        out_path.mkdir(parents=True, exist_ok=True)
        checkpoint.mkdir(parents=True, exist_ok=True)

        latest_csv = out_path / "latest_metrics.csv"

        def write_batch(batch_df, batch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                return
            deduped = batch_df.dropDuplicates(["artist", "window_start_ts", "window_end_ts"])
            mode = "append"
            target = deduped
            if delta_enabled:
                (target.write.format("delta")
                    .mode(mode)
                    .save(str(out_path)))
            else:
                (target.write.format("parquet")
                    .mode(mode)
                    .save(str(out_path)))
            try:
                pdf = deduped.toPandas()
                pdf.sort_values("window_end_ts", inplace=True)
                pdf.to_csv(latest_csv, index=False)
            except Exception:
                pass

        writer = (
            results.writeStream
            .outputMode("update")
            .option("checkpointLocation", str(checkpoint))
        )

        if params.once:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=f"{max(1, params.poll_interval_sec)} seconds")

        query = writer.foreachBatch(write_batch).start()
        query.awaitTermination()
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def run_silver_stream(params: Optional[SilverParams] = None, fallback_local: Optional[bool] = None) -> None:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("silver")
    params = params or SilverParams()

    use_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("SILVER_FALLBACK_LOCAL"))
    if use_fallback:
        n = _fallback_once(params)
        log.info(f"silver_fallback_groups={n}")
        return

    try:
        _run_structured_stream(params, log)
        log.info("silver_stream_completed")
    except Exception as e:
        log.info(f"silver_stream_fallback_reason={e}")
        n = _fallback_once(params)
        log.info(f"silver_fallback_groups={n}")
