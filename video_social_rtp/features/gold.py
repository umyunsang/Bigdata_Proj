from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


@dataclass
class GoldParams:
    top_pct: float = 0.9


def _write_artifact(payload: Dict[str, object]) -> Path:
    s = load_settings()
    ensure_dirs(s)
    p = Path(s.artifact_dir) / "gold_tiers.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def _gold_schema() -> StructType:
    return StructType(
        [
            StructField("artist", StringType(), True),
            StructField("total_engagement", DoubleType(), True),
            StructField("avg_engagement", DoubleType(), True),
            StructField("max_engagement", DoubleType(), True),
            StructField("total_videos", DoubleType(), True),
            StructField("unique_viewers_est", DoubleType(), True),
            StructField("growth_rate_7d", DoubleType(), True),
            StructField("growth_rate_30d", DoubleType(), True),
            StructField("momentum", DoubleType(), True),
            StructField("volatility", DoubleType(), True),
            StructField("market_share", DoubleType(), True),
            StructField("percentile", DoubleType(), True),
            StructField("tier", IntegerType(), True),
            StructField("trend_direction", StringType(), True),
        ]
    )


def _emit_empty_gold(settings, spark, delta_enabled, log):
    empty_df = spark.createDataFrame([], schema=_gold_schema())
    out = Path(settings.gold_dir) / "features"
    (empty_df.write.format("delta" if delta_enabled else "parquet")
        .mode("overwrite")
        .save(str(out)))

    csv_path = Path(settings.gold_dir) / "features.csv"
    csv_path.write_text(
        "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,percentile,tier,trend_direction\n",
        encoding="utf-8",
    )

    artifact = _write_artifact({"tier_cutoffs": {}, "generated_rows": 0, "generated_at": int(time.time())})
    log.info("gold_delta_empty")
    return {
        "path": str(out),
        "csv": str(csv_path),
        "artifact": str(artifact),
        "rows": "0",
    }


def run_gold_features(params: Optional[GoldParams] = None, fallback_local: Optional[bool] = None) -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("gold")
    params = params or GoldParams()
    use_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("GOLD_FALLBACK_LOCAL"))

    # Try Spark first unless forced fallback
    if not use_fallback:
        try:
            spark = None
            try:
                spark, delta_enabled = get_spark_session("gold_features", s, log=log)

                silver_path = Path(s.silver_dir) / "social_metrics"
                formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
                metrics_df = None
                for fmt in formats:
                    try:
                        metrics_df = spark.read.format(fmt).load(str(silver_path))
                        break
                    except Exception:
                        continue
                if metrics_df is None:
                    raise FileNotFoundError(f"silver_metrics_not_found={silver_path}")

                required_cols = {"artist", "window_start_ts", "window_end_ts", "engagement_count", "unique_videos", "unique_authors"}
                missing = required_cols - set(metrics_df.columns)
                if missing:
                    raise RuntimeError(f"silver_metrics_missing_columns={sorted(missing)}")

                metrics_df = metrics_df.select(*(sorted(required_cols)))
                if metrics_df.rdd.isEmpty():
                    return _emit_empty_gold(s, spark, delta_enabled, log)

                metrics_df = metrics_df.withColumn("window_end_ts", F.col("window_end_ts").cast("long"))
                max_ts = metrics_df.agg(F.max("window_end_ts").alias("max_ts")).collect()[0]["max_ts"]
                if max_ts is None:
                    return _emit_empty_gold(s, spark, delta_enabled, log)

                window_ms = {
                    "7d": 7 * 24 * 60 * 60 * 1000,
                    "30d": 30 * 24 * 60 * 60 * 1000,
                }

                metrics_df = (
                    metrics_df
                    .withColumn("recent_7", F.when(F.col("window_end_ts") >= max_ts - window_ms["7d"], F.col("engagement_count")).otherwise(F.lit(0)))
                    .withColumn("prev_7", F.when((F.col("window_end_ts") < max_ts - window_ms["7d"]) & (F.col("window_end_ts") >= max_ts - 2 * window_ms["7d"]), F.col("engagement_count")).otherwise(F.lit(0)))
                    .withColumn("recent_30", F.when(F.col("window_end_ts") >= max_ts - window_ms["30d"], F.col("engagement_count")).otherwise(F.lit(0)))
                    .withColumn("prev_30", F.when((F.col("window_end_ts") < max_ts - window_ms["30d"]) & (F.col("window_end_ts") >= max_ts - 2 * window_ms["30d"]), F.col("engagement_count")).otherwise(F.lit(0)))
                )

                agg_silver = (
                    metrics_df.groupBy("artist")
                    .agg(
                        F.sum("engagement_count").alias("total_engagement"),
                        F.avg("engagement_count").alias("avg_engagement"),
                        F.max("engagement_count").alias("max_engagement"),
                        F.sum("unique_videos").alias("window_unique_videos"),
                        F.sum("unique_authors").alias("window_unique_authors"),
                        F.sum("recent_7").alias("recent_7_engagement"),
                        F.sum("prev_7").alias("prev_7_engagement"),
                        F.sum("recent_30").alias("recent_30_engagement"),
                        F.sum("prev_30").alias("prev_30_engagement"),
                        F.stddev_pop("engagement_count").alias("volatility"),
                    )
                )

                bronze_stats = None
                bronze_path = Path(s.bronze_dir)
                for fmt in formats:
                    try:
                        bronze_df = spark.read.format(fmt).load(str(bronze_path))
                    except Exception:
                        continue
                    else:
                        if "artist" not in bronze_df.columns:
                            bronze_stats = None
                            continue
                        bronze_stats = (
                            bronze_df.groupBy("artist")
                            .agg(
                                F.countDistinct("video_id").alias("total_videos"),
                                F.approx_count_distinct("author_id").alias("unique_viewers_est"),
                            )
                        )
                        break

                if bronze_stats is not None:
                    features_df = agg_silver.join(bronze_stats, "artist", "left")
                else:
                    features_df = agg_silver

                features_df = features_df.fillna({"volatility": 0.0})
                features_df = features_df.withColumn(
                    "total_videos",
                    F.when(F.col("total_videos").isNull(), F.col("window_unique_videos")).otherwise(F.col("total_videos")),
                )
                features_df = features_df.withColumn(
                    "unique_viewers_est",
                    F.when(F.col("unique_viewers_est").isNull(), F.col("window_unique_authors")).otherwise(F.col("unique_viewers_est")),
                )

                features_df = features_df.withColumn(
                    "growth_rate_7d",
                    F.when(F.col("prev_7_engagement") <= 0, F.lit(0.0)).otherwise(
                        (F.col("recent_7_engagement") - F.col("prev_7_engagement")) / F.col("prev_7_engagement") * 100.0
                    ),
                )
                features_df = features_df.withColumn(
                    "growth_rate_30d",
                    F.when(F.col("prev_30_engagement") <= 0, F.lit(0.0)).otherwise(
                        (F.col("recent_30_engagement") - F.col("prev_30_engagement")) / F.col("prev_30_engagement") * 100.0
                    ),
                )
                features_df = features_df.withColumn(
                    "momentum",
                    F.col("growth_rate_7d") - F.col("growth_rate_30d"),
                )

                sum_window = Window.partitionBy(F.lit(1))
                percent_window = Window.orderBy(F.col("total_engagement"))

                features_df = features_df.withColumn(
                    "market_share",
                    F.when(
                        F.sum("total_engagement").over(sum_window) > 0,
                        F.col("total_engagement") / F.sum("total_engagement").over(sum_window),
                    ).otherwise(F.lit(0.0)),
                )
                features_df = features_df.withColumn("percentile", F.percent_rank().over(percent_window))
                features_df = features_df.withColumn(
                    "tier",
                    F.when(F.col("percentile") >= 0.95, F.lit(1))
                    .when(F.col("percentile") >= 0.85, F.lit(2))
                    .when(F.col("percentile") >= 0.60, F.lit(3))
                    .otherwise(F.lit(4)),
                )
                features_df = features_df.withColumn(
                    "trend_direction",
                    F.when(F.col("growth_rate_7d") > 5, F.lit("UP"))
                    .when(F.col("growth_rate_7d") < -5, F.lit("DOWN"))
                    .otherwise(F.lit("STEADY")),
                )

                out = Path(s.gold_dir) / "features"
                final_cols = [
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
                    "trend_direction",
                ]

                final_df = features_df.select(*final_cols)
                final_df = final_df.orderBy(F.col("total_engagement").desc())

                (final_df.write.format("delta" if delta_enabled else "parquet")
                    .mode("overwrite")
                    .save(str(out)))

                row_count = final_df.count()

                csv_path = Path(s.gold_dir) / "features.csv"
                try:
                    final_df.toPandas().to_csv(csv_path, index=False)
                except Exception as exc:
                    log.info(f"gold_csv_write_failed={exc}")

                percentile_stats = features_df.agg(
                    F.expr("percentile_approx(total_engagement, 0.95, 1000)").alias("tier_1"),
                    F.expr("percentile_approx(total_engagement, 0.85, 1000)").alias("tier_2"),
                    F.expr("percentile_approx(total_engagement, 0.60, 1000)").alias("tier_3"),
                ).collect()[0]

                artifact_payload = {
                    "tier_cutoffs": {
                        "tier_1": float(percentile_stats["tier_1"]) if percentile_stats["tier_1"] is not None else 0.0,
                        "tier_2": float(percentile_stats["tier_2"]) if percentile_stats["tier_2"] is not None else 0.0,
                        "tier_3": float(percentile_stats["tier_3"]) if percentile_stats["tier_3"] is not None else 0.0,
                    },
                    "generated_rows": int(row_count),
                    "generated_at": int(time.time()),
                }

                art = _write_artifact(artifact_payload)
                log.info(f"gold_delta_written={out}")
                return {
                    "path": str(out),
                    "csv": str(csv_path),
                    "artifact": str(art),
                    "rows": str(row_count),
                }
            finally:
                if spark is not None:
                    try:
                        spark.stop()
                    except Exception:
                        pass
        except Exception as e:
            log.info(f"gold_fallback_reason={e}")

    # Fallback: parse CSV social_metrics with pandas and compute features
    out_dir = Path(s.gold_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sm_dir = Path(s.silver_dir) / "social_metrics"
    files = sorted([p for p in sm_dir.glob("*.csv")])
    if not files:
        empty = out_dir / "features.csv"
        empty.write_text(
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,percentile,tier,trend_direction\n",
            encoding="utf-8",
        )
        art = _write_artifact({"tier_cutoffs": {}, "generated_rows": 0, "generated_at": int(time.time())})
        return {"path": str(empty), "csv": str(empty), "artifact": str(art), "rows": "0"}

    import pandas as pd
    import numpy as np

    dfs = [pd.read_csv(p) for p in files]
    sm = pd.concat(dfs, ignore_index=True)
    if sm.empty or "artist" not in sm.columns:
        empty = out_dir / "features.csv"
        empty.write_text(
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,percentile,tier,trend_direction\n",
            encoding="utf-8",
        )
        art = _write_artifact({"tier_cutoffs": {}, "generated_rows": 0, "generated_at": int(time.time())})
        return {"path": str(empty), "csv": str(empty), "artifact": str(art), "rows": "0"}

    sm["window_end_ts"] = pd.to_numeric(sm.get("window_end_ts"), errors="coerce")
    sm = sm.dropna(subset=["window_end_ts", "artist"])
    if sm.empty:
        empty = out_dir / "features.csv"
        empty.write_text(
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,percentile,tier,trend_direction\n",
            encoding="utf-8",
        )
        art = _write_artifact({"tier_cutoffs": {}, "generated_rows": 0, "generated_at": int(time.time())})
        return {"path": str(empty), "csv": str(empty), "artifact": str(art), "rows": "0"}

    max_ts = sm["window_end_ts"].max()
    window_ms7 = 7 * 24 * 60 * 60 * 1000
    window_ms30 = 30 * 24 * 60 * 60 * 1000

    sm["recent_7"] = np.where(sm["window_end_ts"] >= max_ts - window_ms7, sm["engagement_count"], 0)
    sm["prev_7"] = np.where(
        (sm["window_end_ts"] < max_ts - window_ms7) & (sm["window_end_ts"] >= max_ts - 2 * window_ms7),
        sm["engagement_count"],
        0,
    )
    sm["recent_30"] = np.where(sm["window_end_ts"] >= max_ts - window_ms30, sm["engagement_count"], 0)
    sm["prev_30"] = np.where(
        (sm["window_end_ts"] < max_ts - window_ms30) & (sm["window_end_ts"] >= max_ts - 2 * window_ms30),
        sm["engagement_count"],
        0,
    )

    grouped = sm.groupby("artist")
    total_engagement = grouped["engagement_count"].sum()
    avg_engagement = grouped["engagement_count"].mean()
    max_engagement = grouped["engagement_count"].max()
    if "unique_videos" in sm.columns:
        total_videos = grouped["unique_videos"].sum()
    else:
        total_videos = grouped.size()

    if "unique_authors" in sm.columns:
        unique_viewers = grouped["unique_authors"].sum()
    else:
        unique_viewers = grouped.size()
    recent_7 = grouped["recent_7"].sum()
    prev_7 = grouped["prev_7"].sum()
    recent_30 = grouped["recent_30"].sum()
    prev_30 = grouped["prev_30"].sum()
    volatility = grouped["engagement_count"].std(ddof=0).fillna(0.0)

    features = pd.DataFrame({
        "artist": total_engagement.index,
        "total_engagement": total_engagement.values,
        "avg_engagement": avg_engagement.values,
        "max_engagement": max_engagement.values,
        "total_videos": total_videos.values,
        "unique_viewers_est": unique_viewers.values,
        "recent_7": recent_7.values,
        "prev_7": prev_7.values,
        "recent_30": recent_30.values,
        "prev_30": prev_30.values,
        "volatility": volatility.values,
    })

    def safe_growth(recent, prev):
        return np.where(prev <= 0, 0.0, (recent - prev) / prev * 100.0)

    features["growth_rate_7d"] = safe_growth(features["recent_7"], features["prev_7"])
    features["growth_rate_30d"] = safe_growth(features["recent_30"], features["prev_30"])
    features["momentum"] = features["growth_rate_7d"] - features["growth_rate_30d"]

    total_sum = features["total_engagement"].sum()
    features["market_share"] = np.where(total_sum > 0, features["total_engagement"] / total_sum, 0.0)
    features["percentile"] = features["total_engagement"].rank(pct=True)

    def assign_tier(pct):
        if pct >= 0.95:
            return 1
        if pct >= 0.85:
            return 2
        if pct >= 0.60:
            return 3
        return 4

    features["tier"] = features["percentile"].apply(assign_tier)
    features["trend_direction"] = np.where(features["growth_rate_7d"] > 5, "UP", np.where(features["growth_rate_7d"] < -5, "DOWN", "STEADY"))

    final_cols = [
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
        "trend_direction",
    ]

    features = features[final_cols].sort_values(by="total_engagement", ascending=False).reset_index(drop=True)
    out_file = out_dir / "features.csv"
    features.to_csv(out_file, index=False)

    percentile_stats = {
        "tier_1": float(features["total_engagement"].quantile(0.95)) if not features.empty else 0.0,
        "tier_2": float(features["total_engagement"].quantile(0.85)) if not features.empty else 0.0,
        "tier_3": float(features["total_engagement"].quantile(0.60)) if not features.empty else 0.0,
    }
    art = _write_artifact({"tier_cutoffs": percentile_stats, "generated_rows": int(len(features)), "generated_at": int(time.time())})
    log.info(f"gold_csv_written={out_file}")
    return {"path": str(out_file), "csv": str(out_file), "artifact": str(art), "rows": str(len(features))}
