from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging


@dataclass
class GoldParams:
    top_pct: float = 0.9


def _write_artifact(cutoff: float, pct: float) -> Path:
    s = load_settings()
    ensure_dirs(s)
    p = Path(s.artifact_dir) / "gold_cutoff.json"
    p.write_text(json.dumps({"percentile": pct, "cutoff": cutoff}, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def run_gold_features(params: Optional[GoldParams] = None, fallback_local: Optional[bool] = None) -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("gold")
    params = params or GoldParams()
    use_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("GOLD_FALLBACK_LOCAL"))

    # Try Spark first unless forced fallback
    if not use_fallback:
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import approx_count_distinct as acd, col
            try:
                from delta import configure_spark_with_delta_pip  # type: ignore
            except Exception:
                configure_spark_with_delta_pip = None

            builder = (
                SparkSession.builder.appName("gold_features")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", str((s.project_root / "warehouse").resolve()))
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate() if configure_spark_with_delta_pip else builder.getOrCreate()

            silver_path = str(Path(s.silver_dir) / "social_metrics")
            metrics = spark.read.format("delta").load(silver_path)
            # Aggregate engagement per video over available windows
            eng = metrics.select("video_id", "count").groupBy("video_id").sum("count").withColumnRenamed("sum(count)", "engagement_24h")
            uniq_est = metrics.groupBy("video_id").agg(acd("video_id").alias("uniq_users_est"))
            joined = eng.join(uniq_est, "video_id", "left")
            # empty guard
            if not joined.take(1):
                out = Path(s.gold_dir) / "features"
                (joined.limit(0).write.format("delta").mode("overwrite").save(str(out)))
                art = _write_artifact(0.0, params.top_pct)
                log.info(f"gold_delta_empty={out}")
                return {"path": str(out), "cutoff": "0.0", "artifact": str(art)}
            # quantile with guard
            try:
                cut_list = joined.approxQuantile("engagement_24h", [params.top_pct], 0.001)
                cut = float(cut_list[0]) if cut_list else 0.0
            except Exception:
                cut = 0.0
            labeled = joined.withColumn("label", (col("engagement_24h") >= cut).cast("int"))

            out = Path(s.gold_dir) / "features"
            (labeled.write.format("delta").mode("overwrite").save(str(out)))
            art = _write_artifact(float(cut), params.top_pct)
            log.info(f"gold_delta_written={out}")
            return {"path": str(out), "cutoff": str(cut), "artifact": str(art)}
        except Exception as e:
            log.info(f"gold_fallback_reason={e}")

    # Fallback: parse CSV social_metrics with pandas and compute features
    out_dir = Path(s.gold_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sm_dir = Path(s.silver_dir) / "social_metrics"
    files = sorted([p for p in sm_dir.glob("*.csv")])
    if not files:
        # no silver metrics; produce empty
        empty = out_dir / "features.csv"
        empty.write_text("video_id,engagement_24h,uniq_users_est,label\n", encoding="utf-8")
        art = _write_artifact(0.0, params.top_pct)
        return {"path": str(empty), "cutoff": "0.0", "artifact": str(art)}

    import pandas as pd

    dfs = [pd.read_csv(p) for p in files]
    sm = pd.concat(dfs, ignore_index=True)
    grp = sm.groupby("video_id", as_index=False)["count"].sum().rename(columns={"count": "engagement_24h"})
    # fallback proxy for uniq_users_est: use engagement as proxy
    grp["uniq_users_est"] = grp["engagement_24h"].astype(int)
    # percentile cutoff via pandas
    cut = float(grp["engagement_24h"].quantile(params.top_pct)) if not grp.empty else 0.0
    grp["label"] = (grp["engagement_24h"] >= cut).astype(int)
    out_file = out_dir / "features.csv"
    grp.to_csv(out_file, index=False)
    art = _write_artifact(cut, params.top_pct)
    log.info(f"gold_csv_written={out_file}")
    return {"path": str(out_file), "cutoff": str(cut), "artifact": str(art)}
