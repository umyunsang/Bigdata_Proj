from __future__ import annotations

import json
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import Row

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
    counts: Dict[str, Dict[str, object]] = defaultdict(lambda: {"count": 0, "videos": set(), "authors": set()})
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


def _run_silver_rdd(params: SilverParams, log) -> int:
    s = load_settings()
    ensure_dirs(s)

    spark = None
    try:
        spark, delta_enabled = get_spark_session("silver_batch", s, log=log)
        sc = spark.sparkContext
        storage_format = "delta" if delta_enabled else "parquet"

        landing_files = list(Path(s.landing_dir).glob("*.json"))
        if not landing_files:
            out_dir = Path(s.silver_dir) / "social_metrics"
            out_dir.mkdir(parents=True, exist_ok=True)
            if not any(out_dir.iterdir()):
                (out_dir / "_EMPTY").touch()
            return 0

        landing_pattern = str(Path(s.landing_dir) / "*.json")
        raw_rdd = sc.textFile(landing_pattern)

        def parse_line(line: str) -> Optional[Dict[str, object]]:
            try:
                item = json.loads(line)
            except Exception:
                return None
            post_id = item.get("post_id")
            video_id = item.get("video_id")
            lang = item.get("lang")
            ts = item.get("ts")
            if lang != "en" or not post_id or not video_id or ts is None:
                return None
            try:
                ts_val = int(ts)
            except Exception:
                return None
            return {
                "post_id": str(post_id),
                "video_id": str(video_id),
                "ts": ts_val,
                "artist": extract_artist(item.get("text") or item.get("title"), item.get("author_id")),
                "author_id": str(item.get("author_id", "")) if item.get("author_id") else "",
            }

        events = raw_rdd.map(parse_line).filter(lambda rec: rec is not None)
        deduped = events.map(lambda rec: (rec["post_id"], rec)).reduceByKey(lambda a, _: a).values()

        window_minutes = _parse_minutes(params.window_size)
        slide_minutes = max(1, _parse_minutes(params.window_slide))
        window_ms = window_minutes * 60 * 1000
        slide_ms = slide_minutes * 60 * 1000
        window_count = max(1, math.ceil(window_ms / slide_ms))

        def assign_windows(rec: Dict[str, object]):
            ts = rec["ts"]
            last_start = (ts // slide_ms) * slide_ms
            for offset in range(window_count):
                start = last_start - offset * slide_ms
                end = start + window_ms
                if ts < start or ts >= end:
                    continue
                yield (
                    (rec["artist"], start, end),
                    {
                        "count": 1,
                        "videos": {rec["video_id"]},
                        "authors": {rec["author_id"]} if rec.get("author_id") else set(),
                    },
                )

        pairs = deduped.flatMap(assign_windows)
        
        def merge(a: Dict[str, object], b: Dict[str, object]) -> Dict[str, object]:
            return {
                "count": int(a["count"]) + int(b["count"]),
                "videos": set(a["videos"]) | set(b["videos"]),
                "authors": set(a["authors"]) | set(b["authors"]),
            }

        aggregated = pairs.reduceByKey(merge)

        if aggregated.isEmpty():
            out_dir = Path(s.silver_dir) / "social_metrics"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "_EMPTY").touch()
            return 0

        def to_row(kv):
            (artist, start, end), stats = kv
            return Row(
                artist=str(artist),
                window_start_ts=int(start),
                window_end_ts=int(end),
                engagement_count=int(stats["count"]),
                unique_videos=len(stats["videos"]),
                unique_authors=len(stats["authors"]),
            )

        rows_rdd = aggregated.map(to_row)
        df = spark.createDataFrame(rows_rdd)

        out_path = Path(s.silver_dir) / "social_metrics"
        (df.write.format(storage_format)
            .mode("overwrite")
            .save(str(out_path)))

        return int(df.count())
    finally:
        if spark is not None:
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
        n = _run_silver_rdd(params, log)
        log.info(f"silver_groups={n}")
    except Exception as e:
        log.info(f"silver_rdd_fallback_reason={e}")
        n = _fallback_once(params)
        log.info(f"silver_fallback_groups={n}")
