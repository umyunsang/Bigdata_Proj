from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


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
            from pyspark.sql.functions import approx_count_distinct as acd, col

            spark = None
            try:
                spark, delta_enabled = get_spark_session("gold_features", s, log=log)

                silver_path = Path(s.silver_dir) / "social_metrics"
                metrics = None
                formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
                for fmt in formats:
                    try:
                        metrics = spark.read.format(fmt).load(str(silver_path))
                        break
                    except Exception:
                        continue
                if metrics is None:
                    raise FileNotFoundError(f"silver_metrics_not_found={silver_path}")

                eng = metrics.select("video_id", "count").groupBy("video_id").sum("count").withColumnRenamed("sum(count)", "engagement_24h")
                uniq_est = metrics.groupBy("video_id").agg(acd("video_id").alias("uniq_users_est"))
                joined = eng.join(uniq_est, "video_id", "left")
                if not joined.take(1):
                    out = Path(s.gold_dir) / "features"
                    (joined.limit(0).write.format("delta" if delta_enabled else "parquet").mode("overwrite").save(str(out)))
                    art = _write_artifact(0.0, params.top_pct)
                    log.info(f"gold_delta_empty={out}")
                    return {"path": str(out), "cutoff": "0.0", "artifact": str(art)}

                try:
                    cut_list = joined.approxQuantile("engagement_24h", [params.top_pct], 0.001)
                    cut = float(cut_list[0]) if cut_list else 0.0
                except Exception:
                    cut = 0.0
                labeled = joined.withColumn("label", (col("engagement_24h") >= cut).cast("int"))

                out = Path(s.gold_dir) / "features"
                (labeled.write.format("delta" if delta_enabled else "parquet").mode("overwrite").save(str(out)))
                art = _write_artifact(float(cut), params.top_pct)
                log.info(f"gold_delta_written={out}")
                return {"path": str(out), "cutoff": str(cut), "artifact": str(art)}
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
