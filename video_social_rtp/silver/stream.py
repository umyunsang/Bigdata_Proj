from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


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

    # filter by window and count per video_id
    counts: Dict[str, int] = defaultdict(int)
    for e in events:
        try:
            ts = int(e.get("ts", 0))
            if ts >= cutoff and e.get("video_id"):
                counts[str(e["video_id"])] += 1
        except Exception:
            continue

    # write a compact CSV to social_metrics (fallback format)
    out_file = out_dir / f"metrics_fallback_{int(time.time())}.csv"
    with out_file.open("w", encoding="utf-8") as f:
        f.write("video_id,count,window_end_ts\n")
        for vid, c in counts.items():
            f.write(f"{vid},{c},{max_ts}\n")

    # naive checkpoint marker
    (chk_dir / "_fallback_marker").write_text(str(int(time.time())), encoding="utf-8")
    return len(counts)


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

    # Try Spark streaming
    try:
        from pyspark.sql.functions import col, from_unixtime, window

        spark = None
        try:
            spark, delta_enabled = get_spark_session("silver_stream", s, log=log)

            schema = "post_id string, text string, lang string, ts long, author_id string, video_id string, source string"
            raw = spark.readStream.format("json").schema(schema).load(str(s.landing_dir))
            clean = (
                raw.filter((col("post_id").isNotNull()) & (col("lang") == "en"))
                   .withColumn("event_time", from_unixtime(col("ts")/1000).cast("timestamp"))
                   .withWatermark("event_time", params.watermark)
                   .dropDuplicates(["post_id"])
            )
            win = clean.groupBy(window(col("event_time"), params.window_size, params.window_slide), col("video_id")).count()
            writer = (
                win.writeStream
                   .format("delta" if delta_enabled else "parquet")
                   .option("checkpointLocation", str(Path(s.checkpoint_dir)/"silver"))
                   .outputMode("append")
            )
            if params.once:
                writer = writer.trigger(once=True)
            q = writer.start(str(Path(s.silver_dir)/"social_metrics"))
            q.awaitTermination()
            if q.isActive:
                q.stop()
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass
    except Exception as e:
        log.info(f"silver_stream_fallback_reason={e}")
        n = _fallback_once(params)
        log.info(f"silver_fallback_groups={n}")
