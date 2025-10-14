from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import Row

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


def run_bronze_batch(fallback_local: bool | None = None) -> int:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("bronze")

    do_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("BRONZE_FALLBACK_LOCAL"))
    try:
        if do_fallback:
            raise RuntimeError("forced_fallback_local")

        spark = None
        try:
            spark, delta_enabled = get_spark_session("bronze_batch", s, log=log)
            sc = spark.sparkContext
            storage_format = "delta" if delta_enabled else "parquet"

            landing_files = list(Path(s.landing_dir).glob("*.json"))
            if not landing_files:
                log.info("bronze_rows=0")
                return 0

            landing_pattern = str(Path(s.landing_dir) / "*.json")
            raw_rdd = sc.textFile(landing_pattern)
            ingest_date = datetime.utcnow().date().isoformat()

            def parse_line(line: str) -> Row | None:
                try:
                    item = json.loads(line)
                except Exception:
                    return None
                post_id = item.get("post_id")
                video_id = item.get("video_id")
                if not post_id or not video_id:
                    return None
                return Row(
                    post_id=str(post_id),
                    video_id=str(video_id),
                    author_id=str(item.get("author_id", "")),
                    text=item.get("text", ""),
                    ts=int(item.get("ts", 0)),
                    ingest_date=ingest_date,
                    source="yt",
                )

            incoming_rdd = raw_rdd.map(parse_line).filter(lambda r: r is not None)

            # Build a broadcast set of existing post_ids using RDD transformations
            seen_ids: set[str] = set()
            if Path(s.bronze_dir).exists():
                formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
                for fmt in formats:
                    try:
                        existing_df = spark.read.format(fmt).load(str(s.bronze_dir)).select("post_id").na.drop()
                        seen_ids.update(existing_df.rdd.map(lambda row: str(row.post_id)).collect())
                        break
                    except Exception:
                        continue
            broadcast_ids = sc.broadcast(seen_ids)
            filtered_rdd = incoming_rdd.filter(lambda row: row.post_id not in broadcast_ids.value)

            cnt = filtered_rdd.count()
            if cnt == 0:
                log.info("bronze_rows=0")
                return 0

            filtered_df = spark.createDataFrame(filtered_rdd)
            (filtered_df.write.format(storage_format).mode("append")
                .partitionBy("ingest_date", "source")
                .save(str(s.bronze_dir)))

            log.info(f"bronze_rows={cnt}")
            return int(cnt)
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass
    except Exception as e:
        # Fallback: copy NDJSON files to bronze/raw and count lines
        log.info(f"Fallback to local bronze due to: {e}")
        raw_dir = Path(s.bronze_dir) / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        cnt = 0
        for f in Path(s.landing_dir).glob("*.json"):
            target = raw_dir / f.name
            target.write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
            with f.open("r", encoding="utf-8") as fh:
                for _ in fh:
                    cnt += 1
        log.info(f"bronze_rows_local_copy={cnt}")
        return int(cnt)
