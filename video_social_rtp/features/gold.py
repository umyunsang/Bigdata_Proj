from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import Row

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
            spark = None
            try:
                spark, delta_enabled = get_spark_session("gold_features", s, log=log)
                sc = spark.sparkContext

                silver_path = Path(s.silver_dir) / "social_metrics"
                metrics_df = None
                formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
                for fmt in formats:
                    try:
                        metrics_df = spark.read.format(fmt).load(str(silver_path)).select("video_id", "count")
                        break
                    except Exception:
                        continue
                if metrics_df is None:
                    raise FileNotFoundError(f"silver_metrics_not_found={silver_path}")

                metrics_rdd = (
                    metrics_df.rdd
                    .map(lambda row: row.asDict())
                    .filter(lambda rec: rec.get("video_id") is not None)
                    .map(
                        lambda rec: (
                            str(rec["video_id"]),
                            float(rec["count"]) if rec.get("count") is not None else 0.0,
                        )
                    )
                )
                if metrics_rdd.isEmpty():
                    out = Path(s.gold_dir) / "features"
                    empty_df = spark.createDataFrame(sc.emptyRDD(), schema="video_id string, engagement_24h double, uniq_users_est double, label int")
                    (empty_df.write.format("delta" if delta_enabled else "parquet").mode("overwrite").save(str(out)))
                    art = _write_artifact(0.0, params.top_pct)
                    log.info(f"gold_delta_empty={out}")
                    return {"path": str(out), "cutoff": "0.0", "artifact": str(art)}

                engagement_rdd = metrics_rdd.reduceByKey(lambda a, b: a + b)
                uniq_est_rdd = metrics_rdd.map(lambda kv: (kv[0], 1.0)).reduceByKey(lambda a, b: a + b)
                joined_rdd = engagement_rdd.leftOuterJoin(uniq_est_rdd).map(lambda kv: (kv[0], float(kv[1][0]), float(kv[1][1] if kv[1][1] is not None else kv[1][0])))

                joined_list = joined_rdd.collect()
                if not joined_list:
                    out = Path(s.gold_dir) / "features"
                    empty_df = spark.createDataFrame(sc.emptyRDD(), schema="video_id string, engagement_24h double, uniq_users_est double, label int")
                    (empty_df.write.format("delta" if delta_enabled else "parquet").mode("overwrite").save(str(out)))
                    art = _write_artifact(0.0, params.top_pct)
                    log.info(f"gold_delta_empty={out}")
                    return {"path": str(out), "cutoff": "0.0", "artifact": str(art)}

                engagements = [val[1] for val in joined_list]
                positive_ids: set[str] = set()
                all_equal = False
                if not engagements:
                    cut = 0.0
                else:
                    sorted_vals = sorted(engagements)
                    unique_vals = sorted(set(sorted_vals))
                    all_equal = len(unique_vals) == 1
                    if all_equal:
                        # 모든 지표가 동일하면 값 기준 분류가 불가능하므로 상위 비율만큼 label=1로 지정
                        positives = max(1, int(round(len(sorted_vals) * (1 - params.top_pct))))
                        positives = min(len(sorted_vals), positives)
                        ranked = sorted(joined_list, key=lambda t: (t[1], t[0]), reverse=True)
                        positive_ids = {vid for vid, _, _ in ranked[:positives]}
                        cut = float(unique_vals[0])
                    elif params.top_pct <= 0:
                        cut = float(sorted_vals[0])
                    elif params.top_pct >= 1:
                        cut = float(sorted_vals[-1])
                    else:
                        idx = int(round((len(sorted_vals) - 1) * params.top_pct))
                        idx = max(0, min(len(sorted_vals) - 1, idx))
                        cut = float(sorted_vals[idx])

                    if not all_equal:
                        positive_ids = {vid for vid, eng, _ in joined_list if float(eng) >= cut}

                labeled_rows = []
                for vid, eng, uniq in joined_list:
                    uniq_val = float(uniq if uniq is not None else eng)
                    label = 1 if vid in positive_ids else 0
                    labeled_rows.append(
                        Row(
                            video_id=vid,
                            engagement_24h=float(eng),
                            uniq_users_est=uniq_val,
                            label=label,
                        )
                    )

                labeled_df = spark.createDataFrame(labeled_rows)
                out = Path(s.gold_dir) / "features"
                writer = labeled_df.coalesce(1).write.format("delta" if delta_enabled else "parquet")
                writer.mode("overwrite").save(str(out))

                # CSV 스냅샷도 함께 저장하여 fallback/외부 분석에서 활용
                csv_path = Path(s.gold_dir) / "features.csv"
                try:
                    labeled_df.toPandas().to_csv(csv_path, index=False)
                except Exception as exc:
                    log.info(f"gold_csv_write_failed={exc}")

                art = _write_artifact(float(cut), params.top_pct)
                log.info(f"gold_delta_written={out}")
                return {
                    "path": str(out),
                    "cutoff": str(cut),
                    "artifact": str(art),
                    "csv": str(csv_path),
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
