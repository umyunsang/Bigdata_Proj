"""Snippet: Silver -> Gold feature engineering with CDF-based tier labeling.
- 아티스트별 피처 집계
- CDF 기반 동적 티어 분류 (Tier 1-4)
- HyperLogLog 기반 유니크 유저 추정
- 트렌드 분석 (growth rate, momentum, volatility)

실행:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/30_gold_features.py
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/30_gold_features.py --top-pct 0.9
"""
import os
import sys
import json
import argparse
from pathlib import Path

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import SILVER, GOLD, ARTIFACT, ensure_dirs

ensure_dirs(GOLD, ARTIFACT)

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, max as spark_max, min as spark_min,
    count, stddev, lit, when, percent_rank
)
from pyspark.sql.window import Window

# Default parameters
TOP_PCT = float(os.getenv("TOP_PCT", "0.9"))


def create_spark():
    """Create Spark session with Delta Lake."""
    return (SparkSession.builder
        .appName("gold_features_snippet")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(GOLD / "_warehouse"))
        .getOrCreate())


def calculate_tier_cutoffs(df, engagement_col: str, top_pct: float):
    """Calculate CDF-based tier cutoffs."""
    # Use approxQuantile for percentile calculation
    quantiles = df.approxQuantile(engagement_col, [0.25, 0.5, 0.75, top_pct], 0.01)

    cutoffs = {
        "tier1_min": quantiles[3] if len(quantiles) > 3 else 0,  # Top performers
        "tier2_min": quantiles[2] if len(quantiles) > 2 else 0,  # Upper mid
        "tier3_min": quantiles[1] if len(quantiles) > 1 else 0,  # Lower mid
        "tier4_min": quantiles[0] if len(quantiles) > 0 else 0,  # Baseline
        "top_pct": top_pct,
    }
    return cutoffs


def assign_tiers(df, cutoffs, engagement_col: str = "total_engagement"):
    """Assign tiers based on CDF cutoffs."""
    return df.withColumn(
        "tier",
        when(col(engagement_col) >= cutoffs["tier1_min"], 1)
        .when(col(engagement_col) >= cutoffs["tier2_min"], 2)
        .when(col(engagement_col) >= cutoffs["tier3_min"], 3)
        .otherwise(4)
    )


def calculate_trend_direction(df):
    """Calculate trend direction based on growth rates."""
    return df.withColumn(
        "trend_direction",
        when(col("growth_rate_7d") > 0.1, "RISING")
        .when(col("growth_rate_7d") < -0.1, "FALLING")
        .otherwise("STEADY")
    )


def main():
    parser = argparse.ArgumentParser(description="Gold feature engineering")
    parser.add_argument("--top-pct", type=float, default=TOP_PCT, help="CDF cutoff percentile (0~1)")
    args = parser.parse_args()

    spark = create_spark()

    try:
        # Read Silver metrics
        silver_path = SILVER / "social_metrics"
        if not silver_path.exists():
            print(f"[ERROR] Silver path does not exist: {silver_path}")
            return

        metrics = spark.read.format("delta").load(str(silver_path))
        print(f"[INFO] Loaded Silver data: {metrics.count()} rows")

        # Aggregate by artist
        artist_agg = (metrics
            .groupBy("artist")
            .agg(
                spark_sum("video_count").alias("total_videos"),
                spark_sum("unique_videos").alias("unique_videos"),
                spark_sum("unique_authors").alias("unique_viewers_est"),
                spark_sum("total_engagement").alias("total_engagement"),
                avg("total_engagement").alias("avg_engagement"),
                spark_max("total_engagement").alias("max_engagement"),
                stddev("total_engagement").alias("volatility"),
                count("*").alias("window_count")
            )
            .fillna(0))

        # Calculate market share
        total_engagement = artist_agg.agg(spark_sum("total_engagement")).collect()[0][0] or 1
        artist_agg = artist_agg.withColumn(
            "market_share",
            col("total_engagement") / lit(total_engagement)
        )

        # Calculate percentile rank
        window_spec = Window.orderBy("total_engagement")
        artist_agg = artist_agg.withColumn("percentile", percent_rank().over(window_spec))

        # Placeholder growth rates (requires historical data for real calculation)
        artist_agg = (artist_agg
            .withColumn("growth_rate_7d", lit(0.0))
            .withColumn("growth_rate_30d", lit(0.0))
            .withColumn("momentum", lit(0.0)))

        # Calculate tier cutoffs
        cutoffs = calculate_tier_cutoffs(artist_agg, "total_engagement", args.top_pct)
        print(f"[INFO] Tier cutoffs: {cutoffs}")

        # Assign tiers
        labeled = assign_tiers(artist_agg, cutoffs)

        # Calculate trend direction
        labeled = calculate_trend_direction(labeled)

        # Select final columns
        final = labeled.select(
            "artist",
            "total_engagement",
            "avg_engagement",
            "max_engagement",
            "total_videos",
            "unique_viewers_est",
            "growth_rate_7d",
            "growth_rate_30d",
            "momentum",
            "volatility",
            "market_share",
            "percentile",
            "tier",
            "trend_direction"
        )

        # Write to Gold (Delta)
        (final.write
            .format("delta")
            .mode("overwrite")
            .save(str(GOLD / "features")))

        # Also export as CSV for easy inspection
        final.toPandas().to_csv(str(GOLD / "features.csv"), index=False)

        print(f"[INFO] Gold features written: {final.count()} artists")

        # Save tier cutoffs to artifacts
        cutoffs_path = ARTIFACT / "gold_tiers.json"
        with cutoffs_path.open("w", encoding="utf-8") as f:
            json.dump(cutoffs, f, ensure_ascii=False, indent=2)
        print(f"[INFO] Tier cutoffs saved: {cutoffs_path}")

        # Show results
        print("\n[INFO] Gold features by tier:")
        final.groupBy("tier").count().orderBy("tier").show()

        print("\n[INFO] Top 10 artists:")
        final.orderBy(col("total_engagement").desc()).show(10)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
