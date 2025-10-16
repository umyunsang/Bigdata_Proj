from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import Row, functions as F

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session
from ..core.artists import extract_artist
from ..core.bloom import BloomFilter


def _bloom_artifact_paths(settings) -> tuple[Path, Path]:
    bin_path = Path(settings.artifact_dir) / "bronze_bloom.bin"
    meta_path = Path(settings.artifact_dir) / "bronze_bloom.json"
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    return bin_path, meta_path


def _load_existing_bloom(s, spark, delta_enabled, log) -> BloomFilter:
    bin_path, meta_path = _bloom_artifact_paths(s)
    if bin_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            bloom = BloomFilter.from_bytes(
                size_bits=int(meta["size_bits"]),
                num_hashes=int(meta["num_hashes"]),
                payload=bin_path.read_bytes(),
                count=int(meta.get("count", 0)),
            )
            log.info(f"bronze_bloom_loaded size_bits={bloom.size_bits} hash_count={bloom.num_hashes} count={bloom.count}")
            return bloom
        except Exception as exc:
            log.info(f"bronze_bloom_load_failed={exc}")

    formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
    existing_df = None
    for fmt in formats:
        try:
            df = spark.read.format(fmt).load(str(s.bronze_dir))
        except Exception:
            continue
        else:
            existing_df = df
            break

    if existing_df is None:
        log.info("bronze_bloom_new=empty_dataset")
        return BloomFilter.create(s.bronze_bloom_capacity, s.bronze_bloom_error_rate)

    cutoff = (datetime.utcnow() - timedelta(days=s.bronze_bloom_lookback_days)).date().isoformat()
    if "ingest_date" in existing_df.columns:
        existing_df = existing_df.filter(F.col("ingest_date") >= cutoff)

    id_df = existing_df.select("post_id").na.drop()
    approx_count = max(id_df.count(), 1)
    bloom = BloomFilter.create(max(approx_count, s.bronze_bloom_capacity), s.bronze_bloom_error_rate)

    for row in id_df.toLocalIterator():
        bloom.add(str(row.post_id))

    log.info(f"bronze_bloom_rebuilt size_bits={bloom.size_bits} hash_count={bloom.num_hashes} count={bloom.count}")
    return bloom


def _persist_bloom(settings, bloom: BloomFilter) -> None:
    bin_path, meta_path = _bloom_artifact_paths(settings)
    size_bits, num_hashes, payload, count = bloom.serialize()
    bin_path.write_bytes(payload)
    meta = {
        "size_bits": size_bits,
        "num_hashes": num_hashes,
        "count": count,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


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

            bloom = _load_existing_bloom(s, spark, delta_enabled, log)
            bloom_state = bloom.serialize()
            bloom_bc = sc.broadcast(bloom_state)

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
                text = item.get("text") or item.get("title")
                channel = item.get("author_id")
                artist = extract_artist(text, channel)
                return Row(
                    post_id=str(post_id),
                    video_id=str(video_id),
                    author_id=str(item.get("author_id", "")),
                    text=item.get("text", ""),
                    ts=int(item.get("ts", 0)),
                    ingest_date=ingest_date,
                    source="yt",
                    artist=artist,
                )

            incoming_rdd = raw_rdd.map(parse_line).filter(lambda r: r is not None)

            dedup_rdd = (
                incoming_rdd.map(lambda row: (row.post_id, row))
                .reduceByKey(lambda a, _: a)
                .values()
            )
            dedup_rdd = dedup_rdd.cache()
            total_candidates = dedup_rdd.count()

            def filter_partition(iterator):
                size_bits, num_hashes, payload, _ = bloom_bc.value
                local_bloom = BloomFilter.from_bytes(
                    size_bits=size_bits,
                    num_hashes=num_hashes,
                    payload=payload,
                    count=0,
                )
                for row in iterator:
                    if not local_bloom.might_contain(row.post_id):
                        yield row

            filtered_rdd = dedup_rdd.mapPartitions(filter_partition)
            filtered_rdd = filtered_rdd.cache()

            cnt = filtered_rdd.count()
            filtered_df = spark.createDataFrame(filtered_rdd)
            (filtered_df.write.format(storage_format).mode("append")
                .partitionBy("ingest_date", "artist")
                .save(str(s.bronze_dir)))

            new_ids = filtered_rdd.map(lambda row: row.post_id).collect()
            bloom.add_all(new_ids)
            _persist_bloom(s, bloom)

            dropped = total_candidates - cnt
            log.info(f"bronze_bloom_filtered={dropped}")
            log.info(f"bronze_rows={cnt}")
            dedup_rdd.unpersist()
            filtered_rdd.unpersist()
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
