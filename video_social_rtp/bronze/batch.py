from __future__ import annotations

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_date, lit

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging


def run_bronze_batch(fallback_local: bool | None = None) -> int:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("bronze")

    do_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("BRONZE_FALLBACK_LOCAL"))
    try:
        if do_fallback:
            raise RuntimeError("forced_fallback_local")

        try:
            from delta import configure_spark_with_delta_pip  # type: ignore
        except Exception:
            configure_spark_with_delta_pip = None

        builder = (
            SparkSession.builder.appName("bronze_batch")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", str((s.project_root / "warehouse").resolve()))
        )
        spark = (
            configure_spark_with_delta_pip(builder).getOrCreate() if configure_spark_with_delta_pip else builder.getOrCreate()
        )

        df = spark.read.json(str(s.landing_dir))
        incoming = (
            df.selectExpr("post_id","video_id","author_id","text","ts")
              .withColumn("ingest_date", current_date())
              .withColumn("source", lit("yt"))
        )

        # Bloom pre-filter
        bf = None
        if Path(s.bronze_dir).exists():
            try:
                recent = spark.read.format("delta").load(str(s.bronze_dir)).select("post_id").na.drop()
                bf = recent.agg(expr("bloom_filter(post_id, 100000, 0.01) as bf")).collect()[0]["bf"]
            except Exception:
                bf = None

        filtered = incoming if bf is None else incoming.filter(~expr(f"might_contain('{bf}', post_id)"))
        (
            filtered.write.format("delta").mode("append")
            .partitionBy("ingest_date", "source")
            .save(str(s.bronze_dir))
        )

        cnt = filtered.count()
        log.info(f"bronze_rows={cnt}")
        return int(cnt)
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
