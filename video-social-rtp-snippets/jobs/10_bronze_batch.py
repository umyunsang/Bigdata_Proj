"""Snippet: batch ingest landing -> bronze (Delta), with Bloom pre-filter + Artist extraction.
- Bloom Filter로 중복 제거 (최근 7일 데이터 기준)
- 아티스트 추출 및 파티셔닝 (ingest_date, artist)

실행:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/10_bronze_batch.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import LANDING, BRONZE, ARTIFACT, ensure_dirs
from conf.artists import extract_artist

ensure_dirs(BRONZE, ARTIFACT)

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, udf, expr
from pyspark.sql.types import StringType

# Spark session with Delta Lake
spark = (SparkSession.builder
    .appName("bronze_batch_snippet")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", str(BRONZE / "_warehouse"))
    .getOrCreate())


def build_bloom_from_bronze(spark, bronze_path: Path, lookback_days: int = 7):
    """Build Bloom filter from recent Bronze data for deduplication."""
    if not bronze_path.exists():
        return None

    try:
        # Read recent Bronze data
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        recent = (spark.read.format("delta").load(str(bronze_path))
            .filter(col("ingest_date") >= cutoff)
            .select("post_id")
            .na.drop())

        if recent.count() == 0:
            return None

        # Create Bloom filter using Spark's built-in bloom_filter
        bf = recent.agg(expr("bloom_filter(post_id, 100000, 0.01) as bf")).collect()[0]["bf"]
        return bf
    except Exception as e:
        print(f"[WARN] Could not build Bloom filter: {e}")
        return None


def main():
    # Read all JSON lines from landing
    landing_files = list(LANDING.glob("events_*.json"))
    if not landing_files:
        print("[INFO] No landing files found")
        spark.stop()
        return

    print(f"[INFO] Found {len(landing_files)} landing files")
    df = spark.read.json(str(LANDING))

    # Artist extraction UDF
    extract_artist_udf = udf(lambda text, author_id: extract_artist(text, author_id), StringType())

    # Select and transform columns
    incoming = (df
        .selectExpr("post_id", "video_id", "author_id", "text", "ts", "lang", "source")
        .withColumn("ingest_date", current_date().cast("string"))
        .withColumn("artist", extract_artist_udf(col("text"), col("author_id"))))

    # Build Bloom filter from recent Bronze data
    bf = build_bloom_from_bronze(spark, BRONZE, lookback_days=7)

    # Filter duplicates if Bloom filter exists
    if bf is not None:
        print("[INFO] Applying Bloom filter for deduplication")
        filtered = incoming.filter(~expr(f"might_contain('{bf}', post_id)"))
    else:
        print("[INFO] No Bloom filter available, processing all records")
        filtered = incoming

    # Count before write
    new_count = filtered.count()
    print(f"[INFO] New records after dedup: {new_count}")

    if new_count > 0:
        # Append to Delta table with partitioning
        (filtered.write
            .format("delta")
            .mode("append")
            .partitionBy("ingest_date", "artist")
            .save(str(BRONZE)))

        print(f"Bronze appended. Rows: {new_count}")

        # Show artist distribution
        print("\n[INFO] Artist distribution:")
        filtered.groupBy("artist").count().orderBy(col("count").desc()).show(10)
    else:
        print("[INFO] No new records to append")

    spark.stop()


if __name__ == "__main__":
    main()
