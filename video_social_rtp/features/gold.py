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
            StructField("quarter", StringType(), True),
            StructField("total_engagement", DoubleType(), True),
            StructField("total_videos", DoubleType(), True),
            StructField("unique_authors", DoubleType(), True),
            StructField("growth_qoq", DoubleType(), True),
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
        .option("overwriteSchema", "true")
        .save(str(out)))

    csv_path = Path(settings.gold_dir) / "features.csv"
    csv_path.write_text(
        "artist,quarter,total_engagement,total_videos,unique_authors,growth_qoq,market_share,percentile,tier,trend_direction\n",
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

                # Silver now outputs quarterly data with these columns
                required_cols = {
                    "quarter",
                    "artist",
                    "total_engagement",
                    "total_videos",
                    "unique_authors",
                    "growth_qoq",
                    "market_share",
                }
                missing = required_cols - set(metrics_df.columns)
                if missing:
                    raise RuntimeError(f"silver_metrics_missing_columns={sorted(missing)}")

                metrics_df = metrics_df.select(*(sorted(required_cols)))
                if metrics_df.rdd.isEmpty():
                    return _emit_empty_gold(s, spark, delta_enabled, log)

                # Silver already computed growth_qoq and market_share per quarter
                # Now we add percentile and tier based on total_engagement per quarter

                # Calculate percentile within each quarter
                percent_window = Window.partitionBy("quarter").orderBy(F.col("total_engagement"))

                features_df = metrics_df.withColumn("percentile", F.percent_rank().over(percent_window))

                # Assign tier based on percentile (per quarter)
                features_df = features_df.withColumn(
                    "tier",
                    F.when(F.col("percentile") >= 0.95, F.lit(1))
                    .when(F.col("percentile") >= 0.85, F.lit(2))
                    .when(F.col("percentile") >= 0.60, F.lit(3))
                    .otherwise(F.lit(4)),
                )

                # Assign trend_direction based on growth_qoq
                features_df = features_df.withColumn(
                    "trend_direction",
                    F.when(F.col("growth_qoq") > 10, F.lit("UP"))
                    .when(F.col("growth_qoq") < -10, F.lit("DOWN"))
                    .otherwise(F.lit("STEADY")),
                )

                out = Path(s.gold_dir) / "features"
                final_cols = [
                    "artist",
                    "quarter",
                    "total_engagement",
                    "total_videos",
                    "unique_authors",
                    "growth_qoq",
                    "market_share",
                    "percentile",
                    "tier",
                    "trend_direction",
                ]

                final_df = features_df.select(*final_cols)
                final_df = final_df.orderBy("quarter", F.col("total_engagement").desc())

                (final_df.write.format("delta" if delta_enabled else "parquet")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .save(str(out)))

                row_count = final_df.count()

                csv_path = Path(s.gold_dir) / "features.csv"
                try:
                    final_df.toPandas().to_csv(csv_path, index=False)
                except Exception as exc:
                    log.info(f"gold_csv_write_failed={exc}")

                # Calculate tier distribution per quarter
                tier_dist = features_df.groupBy("quarter", "tier").count().orderBy("quarter", "tier").collect()

                artifact_payload = {
                    "tier_distribution": [{"quarter": row["quarter"], "tier": row["tier"], "count": row["count"]} for row in tier_dist],
                    "generated_rows": int(row_count),
                    "generated_at": int(time.time()),
                    "analysis_mode": "quarterly",
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
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,engagement_pdf,engagement_cdf,percentile,tier,trend_direction\n",
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
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,engagement_pdf,engagement_cdf,percentile,tier,trend_direction\n",
            encoding="utf-8",
        )
        art = _write_artifact({"tier_cutoffs": {}, "generated_rows": 0, "generated_at": int(time.time())})
        return {"path": str(empty), "csv": str(empty), "artifact": str(art), "rows": "0"}

    sm["window_end_ts"] = pd.to_numeric(sm.get("window_end_ts"), errors="coerce")
    sm = sm.dropna(subset=["window_end_ts", "artist"])
    if sm.empty:
        empty = out_dir / "features.csv"
        empty.write_text(
            "artist,total_engagement,avg_engagement,max_engagement,total_videos,unique_viewers_est,growth_rate_7d,growth_rate_30d,momentum,volatility,market_share,engagement_pdf,engagement_cdf,percentile,tier,trend_direction\n",
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
    features["engagement_pdf"] = features["market_share"]
    features = features.sort_values("total_engagement").reset_index(drop=True)
    features["engagement_cdf"] = features["engagement_pdf"].cumsum()
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

    features = features.sort_values("total_engagement", ascending=False).reset_index(drop=True)
    features = features.drop(columns=[
        "recent_7",
        "prev_7",
        "recent_30",
        "prev_30",
    ])

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
        "engagement_pdf",
        "engagement_cdf",
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
