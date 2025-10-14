from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees, RandomForest

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

    spark, delta_enabled = get_spark_session("pareto_training", s, log=log)
    try:
        gold_delta = Path(s.gold_dir) / "features"
        if not gold_delta.exists():
            raise FileNotFoundError(f"Gold features not found: {gold_delta}")

        df = None
        formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
        for fmt in formats:
            try:
                df = spark.read.format(fmt).load(str(gold_delta)).select("label", "engagement_24h", "uniq_users_est").fillna(0)
                break
            except Exception:
                continue
        if df is None:
            raise RuntimeError(f"unable_to_read_gold_features={gold_delta}")

        lp_rdd = df.rdd.map(
            lambda row: LabeledPoint(float(row.label), Vectors.dense([float(row.engagement_24h), float(row.uniq_users_est)]))
        )
        if lp_rdd.isEmpty():
            raise RuntimeError("Gold features RDD is empty")

        lp_rdd = lp_rdd.cache()
        class_count = lp_rdd.map(lambda lp: int(lp.label)).distinct().count()
        if class_count < 2:
            raise RuntimeError(
                "Need at least two classes for training; collect more data or adjust Gold percentile to generate label variety."
            )

        feat_cols = ["engagement_24h", "uniq_users_est"]

        def train_lr(data):
            model = LogisticRegressionWithLBFGS.train(data, iterations=50, numClasses=2)
            model.clearThreshold()
            return model, model.predict

        def train_rf(data):
            model = RandomForest.trainClassifier(
                data,
                numClasses=2,
                categoricalFeaturesInfo={},
                numTrees=200,
                maxDepth=10,
                seed=42,
            )

            def score(features):
                if not model.trees:
                    return 0.0
                votes = sum(tree.predict(features) for tree in model.trees)
                return float(votes) / float(len(model.trees))

            return model, score

        def train_gbt(data):
            model = GradientBoostedTrees.trainClassifier(
                data,
                categoricalFeaturesInfo={},
                numIterations=80,
                learningRate=0.1,
                maxDepth=6,
            )

            def score(features):
                return float(model.predict(features))

            return model, score

        candidates: List[Tuple[str, Callable]] = [
            ("lr", train_lr),
            ("rf", train_rf),
            ("gbt", train_gbt),
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
        for name, trainer in candidates:
            start = time.time()
            model, scorer = trainer(lp_rdd)
            fit_ms = (time.time() - start) * 1000
            t0 = time.time()
            scores = lp_rdd.map(lambda lp: (int(lp.label), float(scorer(lp.features))))
            score_list = scores.collect()
            latency_ms = (time.time() - t0) * 1000
            y = [lbl for lbl, _ in score_list]
            y_scores = [sc for _, sc in score_list]
            aucpr = _pr_auc(y, y_scores)
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
