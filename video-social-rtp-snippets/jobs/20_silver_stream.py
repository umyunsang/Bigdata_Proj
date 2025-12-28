"""Snippet: Bronze -> Silver streaming aggregation with sliding window + watermark.
- 아티스트별 집계 (window, artist)
- HyperLogLog (approx_count_distinct) 사용
- Structured Streaming with Delta Lake

실행 (스트리밍):
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/20_silver_stream.py

실행 (배치 모드, 한 번만):
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/20_silver_stream.py --once

실행 (Bronze에서 배치로 읽기):
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/20_silver_stream.py --batch
"""
import os
import sys
import argparse
from pathlib import Path

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import LANDING, BRONZE, SILVER, CHECKPOINT, ensure_dirs

ensure_dirs(SILVER, CHECKPOINT / "silver")

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, count, countDistinct,
    approx_count_distinct, sum as spark_sum, lit
)

# Default streaming parameters
WATERMARK = os.getenv("WATERMARK", "10 minutes")
WIN_SIZE = os.getenv("WINDOW_SIZE", "1 hour")
WIN_SLIDE = os.getenv("WINDOW_SLIDE", "5 minutes")


def create_spark():
    """Create Spark session with Delta Lake."""
    return (SparkSession.builder
        .appName("silver_stream_snippet")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(SILVER / "_warehouse"))
        .getOrCreate())


def run_streaming(spark, once: bool = False):
    """Run structured streaming from Landing JSON files."""
    schema = """
        post_id STRING, text STRING, lang STRING, ts LONG,
        author_id STRING, video_id STRING, source STRING
    """

    # Read stream from landing
    raw = (spark.readStream
        .format("json")
        .schema(schema)
        .load(str(LANDING)))

    # Transform: event time, watermark, artist extraction
    from conf.artists import extract_artist
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    extract_artist_udf = udf(
        lambda text, author_id: extract_artist(text, author_id),
        StringType()
    )

    clean = (raw
        .filter(col("post_id").isNotNull())
        .withColumn("event_time", to_timestamp((col("ts") / 1000).cast("timestamp")))
        .withColumn("artist", extract_artist_udf(col("text"), col("author_id")))
        .withWatermark("event_time", WATERMARK)
        .dropDuplicates(["post_id"]))

    # Aggregate by window and artist
    agg = (clean
        .groupBy(
            window(col("event_time"), WIN_SIZE, WIN_SLIDE),
            col("artist")
        )
        .agg(
            count("*").alias("video_count"),
            countDistinct("video_id").alias("unique_videos"),
            approx_count_distinct("author_id").alias("unique_authors"),
            lit(1.0).alias("total_engagement")  # Placeholder - real engagement from API
        ))

    # Write stream
    trigger = {"availableNow": True} if once else {"processingTime": "30 seconds"}

    query = (agg.writeStream
        .format("delta")
        .option("checkpointLocation", str(CHECKPOINT / "silver"))
        .outputMode("append")
        .trigger(**trigger)
        .start(str(SILVER / "social_metrics")))

    if once:
        query.awaitTermination()
        print(f"[INFO] Streaming completed (once mode)")
    else:
        print(f"[INFO] Streaming started. Waiting for termination...")
        query.awaitTermination()


def run_batch(spark):
    """Run batch processing from Bronze Delta table."""
    print("[INFO] Running batch mode from Bronze")

    if not BRONZE.exists():
        print("[ERROR] Bronze path does not exist")
        return

    # Read Bronze
    bronze_df = spark.read.format("delta").load(str(BRONZE))

    # Transform
    clean = (bronze_df
        .filter(col("post_id").isNotNull())
        .withColumn("event_time", to_timestamp((col("ts") / 1000).cast("timestamp"))))

    # Aggregate by window and artist
    agg = (clean
        .groupBy(
            window(col("event_time"), WIN_SIZE, WIN_SLIDE),
            col("artist")
        )
        .agg(
            count("*").alias("video_count"),
            countDistinct("video_id").alias("unique_videos"),
            approx_count_distinct("author_id").alias("unique_authors"),
            lit(1.0).alias("total_engagement")
        ))

    # Write to Silver
    (agg.write
        .format("delta")
        .mode("overwrite")
        .save(str(SILVER / "social_metrics")))

    print(f"[INFO] Batch completed. Rows: {agg.count()}")

    # Show sample
    print("\n[INFO] Sample aggregation by artist:")
    agg.groupBy("artist").agg(
        spark_sum("video_count").alias("total_videos"),
        spark_sum("unique_authors").alias("total_authors")
    ).orderBy(col("total_videos").desc()).show(10)


def main():
    parser = argparse.ArgumentParser(description="Silver streaming/batch aggregation")
    parser.add_argument("--once", action="store_true", help="Run streaming once and exit")
    parser.add_argument("--batch", action="store_true", help="Run batch mode from Bronze")
    args = parser.parse_args()

    spark = create_spark()

    try:
        if args.batch:
            run_batch(spark)
        else:
            run_streaming(spark, once=args.once)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
