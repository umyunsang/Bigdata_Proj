from __future__ import annotations

import os
from pathlib import Path
from pyspark.sql.functions import expr, current_date, lit

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
            storage_format = "delta" if delta_enabled else "parquet"

            df = spark.read.json(str(s.landing_dir))
            incoming = (
                df.selectExpr("post_id","video_id","author_id","text","ts")
                  .withColumn("ingest_date", current_date())
                  .withColumn("source", lit("yt"))
            )

            # Bloom filter based on existing store if available
            bf = None
            if Path(s.bronze_dir).exists():
                formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
                for fmt in formats:
                    try:
                        recent = spark.read.format(fmt).load(str(s.bronze_dir)).select("post_id").na.drop()
                        bf = recent.agg(expr("bloom_filter(post_id, 100000, 0.01) as bf")).collect()[0]["bf"]
                        break
                    except Exception:
                        continue

            filtered = incoming if bf is None else incoming.filter(~expr(f"might_contain('{bf}', post_id)"))
            (
                filtered.write.format(storage_format).mode("append")
                .partitionBy("ingest_date", "source")
                .save(str(s.bronze_dir))
            )

            cnt = filtered.count()
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
