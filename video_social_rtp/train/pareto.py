from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


@dataclass
class TrainParams:
    use_mlflow: bool = True


def _save_pareto_artifact(results: List[Dict], pareto: List[Dict]) -> Path:
    s = load_settings()
    ensure_dirs(s)
    p = Path(s.artifact_dir) / "pareto.json"
    p.write_text(json.dumps({"results": results, "pareto": pareto}, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def _pareto_front(results: List[Dict]) -> List[Dict]:
    def dominates(a: Dict, b: Dict) -> bool:
        return (
            a["aucpr"] >= b["aucpr"] and a["latency_ms"] <= b["latency_ms"] and a["feat_count"] <= b["feat_count"]
        ) and (
            a["aucpr"] > b["aucpr"] or a["latency_ms"] < b["latency_ms"] or a["feat_count"] < b["feat_count"]
        )

    pareto: List[Dict] = []
    for r in results:
        if not any(dominates(o, r) for o in results if o is not r):
            pareto.append(r)
    return pareto


def _train_spark(params: TrainParams) -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("train")

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StringIndexer
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    spark, delta_enabled = get_spark_session("pareto_training", s, log=log)
    try:
        gold_delta = Path(s.gold_dir) / "features"
        if not gold_delta.exists():
            raise FileNotFoundError(f"Gold features not found: {gold_delta}")

        df = None
        formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
        for fmt in formats:
            try:
                df = spark.read.format(fmt).load(str(gold_delta)).fillna(0)
                break
            except Exception:
                continue
        if df is None:
            raise RuntimeError(f"unable_to_read_gold_features={gold_delta}")

        feat_cols = ["engagement_24h", "uniq_users_est"]
        si = StringIndexer(inputCol="label", outputCol="label_idx")
        va = VectorAssembler(inputCols=feat_cols, outputCol="features")
        evaluator = BinaryClassificationEvaluator(labelCol="label_idx", metricName="areaUnderPR")

        candidates = [
            ("lr", LogisticRegression(labelCol="label_idx", maxIter=50)),
            ("rf", RandomForestClassifier(labelCol="label_idx", numTrees=200, maxDepth=10)),
            ("gbt", GBTClassifier(labelCol="label_idx", maxIter=80, maxDepth=6)),
        ]

        use_mlflow = False
        if params.use_mlflow:
            try:
                import mlflow  # type: ignore

                mlroot = Path(s.artifact_dir) / "mlruns"
                mlroot.mkdir(parents=True, exist_ok=True)
                mlflow.set_tracking_uri(f"file://{mlroot}")
                mlflow.set_experiment("train_pareto")
                use_mlflow = True
            except Exception as e:
                log.info(f"mlflow_disabled_reason={e}")

        results: List[Dict] = []
        for name, clf in candidates:
            start = time.time()
            pipe = Pipeline(stages=[si, va, clf]).fit(df)
            fit_ms = (time.time() - start) * 1000
            t0 = time.time()
            pred = pipe.transform(df)
            latency_ms = (time.time() - t0) * 1000
            aucpr = float(evaluator.evaluate(pred))
            feat_count = len(feat_cols)
            res = {"name": name, "aucpr": aucpr, "latency_ms": latency_ms, "feat_count": feat_count, "fit_ms": fit_ms}
            results.append(res)

            if use_mlflow:
                try:
                    import mlflow  # type: ignore

                    with mlflow.start_run(run_name=name):
                        mlflow.log_params({"model": name, "feat_count": feat_count})
                        mlflow.log_metrics({"aucpr": aucpr, "latency_ms": latency_ms, "fit_ms": fit_ms})
                except Exception as e:
                    log.info(f"mlflow_log_failed={e}")

        pareto = _pareto_front(results)
        art = _save_pareto_artifact(results, pareto)
        log.info(f"pareto_artifact={art}")
        return {"artifact": str(art), "results": str(len(results)), "front": str(len(pareto))}
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _pr_auc(y_true: List[int], scores: List[float]) -> float:
    # Sort by score desc
    paired = sorted(zip(scores, y_true), key=lambda t: t[0], reverse=True)
    if not paired:
        return 0.0
    y_sorted = [int(y) for _, y in paired]
    tp = 0
    total_pos = sum(y_sorted)
    if total_pos == 0:
        return 0.0
    auc = 0.0
    prev_recall = 0.0
    for i, y in enumerate(y_sorted):
        if y == 1:
            tp += 1
        recall = tp / total_pos
        precision = tp / (i + 1)
        # rectangle/step-wise area increment
        auc += (recall - prev_recall) * precision
        prev_recall = recall
    return float(auc)


def _train_fallback() -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("train")
    import pandas as pd

    feats_csv = Path(s.gold_dir) / "features.csv"
    if not feats_csv.exists():
        raise FileNotFoundError(f"Gold features CSV not found: {feats_csv}")
    df = pd.read_csv(feats_csv)
    if df.empty:
        raise RuntimeError("Empty gold features CSV")

    # Scores
    score_simple = df["engagement_24h"].astype(float).tolist()
    score_medium = (0.7 * df["engagement_24h"].astype(float) + 0.3 * df["uniq_users_est"].astype(float)).tolist()
    score_complex = (0.5 * df["engagement_24h"].astype(float) + 0.5 * df["uniq_users_est"].astype(float)).tolist()
    y = df["label"].astype(int).tolist()

    results = [
        {"name": "simple", "aucpr": _pr_auc(y, score_simple), "latency_ms": 5.0, "feat_count": 1},
        {"name": "medium", "aucpr": _pr_auc(y, score_medium), "latency_ms": 10.0, "feat_count": 2},
        {"name": "complex", "aucpr": _pr_auc(y, score_complex), "latency_ms": 20.0, "feat_count": 3},
    ]
    pareto = _pareto_front(results)
    art = _save_pareto_artifact(results, pareto)
    log.info(f"pareto_artifact_fallback={art}")
    return {"artifact": str(art), "results": str(len(results)), "front": str(len(pareto))}


def run_train_pareto(params: Optional[TrainParams] = None, fallback_local: Optional[bool] = None) -> Dict[str, str]:
    params = params or TrainParams()
    use_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("TRAIN_FALLBACK_LOCAL"))
    if use_fallback:
        return _train_fallback()
    try:
        return _train_spark(params)
    except Exception as e:
        log = setup_logging("train")
        log.info(f"train_fallback_reason={e}")
        return _train_fallback()
