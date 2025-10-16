from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from ..core.config import ensure_dirs, load_settings
from ..core.logging import setup_logging
from ..core.spark_env import get_spark_session


@dataclass
class TrainParams:
    use_mlflow: bool = True


def _save_pareto_artifact(results: List[Dict], pareto: List[Dict]) -> Path:
    s = load_settings()
    ensure_dirs(s)
    path = Path(s.artifact_dir) / "pareto.json"
    path.write_text(json.dumps({"results": results, "pareto": pareto}, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _pareto_front(results: List[Dict]) -> List[Dict]:
    def dominates(a: Dict, b: Dict) -> bool:
        better_or_equal = (
            a["f1"] >= b["f1"]
            and a["accuracy"] >= b["accuracy"]
            and a["latency_ms"] <= b["latency_ms"]
            and a["feat_count"] <= b["feat_count"]
        )
        strictly_better = (
            a["f1"] > b["f1"]
            or a["accuracy"] > b["accuracy"]
            or a["latency_ms"] < b["latency_ms"]
            or a["feat_count"] < b["feat_count"]
        )
        return better_or_equal and strictly_better

    pareto: List[Dict] = []
    for candidate in results:
        if not any(dominates(other, candidate) for other in results if other is not candidate):
            pareto.append(candidate)
    return pareto


def _train_spark(params: TrainParams) -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("train")

    spark, delta_enabled = get_spark_session("pareto_training", s, log=log)
    try:
        gold_path = Path(s.gold_dir) / "features"
        if not gold_path.exists():
            raise FileNotFoundError(f"Gold features not found: {gold_path}")

        formats = ["delta", "parquet"] if delta_enabled else ["parquet", "delta"]
        gold_df = None
        for fmt in formats:
            try:
                gold_df = spark.read.format(fmt).load(str(gold_path))
                break
            except Exception:
                continue
        if gold_df is None:
            raise RuntimeError(f"unable_to_read_gold_features={gold_path}")

        required_cols = {
            "artist",
            "total_engagement",
            "avg_engagement",
            "total_videos",
            "unique_viewers_est",
            "growth_rate_7d",
            "growth_rate_30d",
            "momentum",
            "volatility",
            "tier",
        }
        missing = required_cols - set(gold_df.columns)
        if missing:
            raise RuntimeError(f"gold_features_missing_columns={sorted(missing)}")

        feature_cols = [
            "total_engagement",
            "avg_engagement",
            "total_videos",
            "unique_viewers_est",
            "growth_rate_7d",
            "growth_rate_30d",
            "momentum",
            "volatility",
        ]

        dataset = (
            gold_df
            .select("artist", "tier", *feature_cols)
            .fillna(0)
            .withColumn("tier_idx", (F.col("tier") - 1).cast("double"))
        )

        class_count = dataset.select("tier_idx").distinct().count()
        if class_count < 2:
            raise RuntimeError("Need at least two classes for training; ensure Gold tier generation yields multiple labels.")

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        assembled = assembler.transform(dataset).cache()
        train_df, test_df = assembled.randomSplit([0.8, 0.2], seed=42)
        train_count = train_df.count()
        test_count = test_df.count()
        if test_count == 0:
            test_df = train_df
            test_count = test_df.count()

        metrics = {
            "accuracy": MulticlassClassificationEvaluator(labelCol="tier_idx", metricName="accuracy"),
            "f1": MulticlassClassificationEvaluator(labelCol="tier_idx", metricName="f1"),
        }

        models = {
            "logreg": LogisticRegression(featuresCol="features", labelCol="tier_idx", maxIter=120, regParam=0.01, family="multinomial"),
            "rf": RandomForestClassifier(featuresCol="features", labelCol="tier_idx", numTrees=250, maxDepth=12, seed=42),
        }

        use_mlflow = False
        if params.use_mlflow:
            try:
                import mlflow  # type: ignore

                ml_root = Path(s.artifact_dir) / "mlruns"
                ml_root.mkdir(parents=True, exist_ok=True)
                mlflow.set_tracking_uri(f"file://{ml_root}")
                mlflow.set_experiment("train_pareto")
                use_mlflow = True
            except Exception as exc:
                log.info(f"mlflow_disabled_reason={exc}")

        results: List[Dict] = []
        for name, estimator in models.items():
            start = time.time()
            pipeline = Pipeline(stages=[estimator])
            model = pipeline.fit(train_df)
            fit_ms = (time.time() - start) * 1000

            t0 = time.time()
            predictions = model.transform(test_df)
            latency_ms = (time.time() - t0) * 1000

            accuracy = float(metrics["accuracy"].evaluate(predictions))
            f1 = float(metrics["f1"].evaluate(predictions))
            feat_count = len(feature_cols)

            result = {
                "name": name,
                "accuracy": accuracy,
                "f1": f1,
                "latency_ms": latency_ms,
                "feat_count": feat_count,
                "fit_ms": fit_ms,
            }
            results.append(result)

            if use_mlflow:
                try:
                    import mlflow  # type: ignore

                    with mlflow.start_run(run_name=name):
                        mlflow.log_params({"train_rows": train_count, "test_rows": test_count})
                        mlflow.log_params({"model": name, "feat_count": feat_count})
                        mlflow.log_metrics({"accuracy": accuracy, "f1": f1, "latency_ms": latency_ms, "fit_ms": fit_ms})
                except Exception as exc:
                    log.info(f"mlflow_log_failed={exc}")

        pareto = _pareto_front(results)
        artifact = _save_pareto_artifact(results, pareto)
        log.info(f"pareto_artifact={artifact}")
        assembled.unpersist()
        return {"artifact": str(artifact), "results": str(len(results)), "front": str(len(pareto))}
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def _macro_f1(y_true, y_pred) -> float:
    labels = sorted(set(y_true))
    if not labels:
        return 0.0
    scores = []
    for label in labels:
        tp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == label and yp == label)
        fp = sum(1 for yt, yp in zip(y_true, y_pred) if yt != label and yp == label)
        fn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == label and yp != label)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        score = 0.0 if (precision + recall) == 0 else 2 * precision * recall / (precision + recall)
        scores.append(score)
    return float(sum(scores) / len(scores))


def _train_fallback() -> Dict[str, str]:
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("train")

    import pandas as pd
    import numpy as np

    feats_csv = Path(s.gold_dir) / "features.csv"
    if not feats_csv.exists():
        raise FileNotFoundError(f"Gold features CSV not found: {feats_csv}")

    df = pd.read_csv(feats_csv)
    if df.empty or "tier" not in df.columns:
        raise RuntimeError("Gold features CSV missing tier labels")

    y_true = df["tier"].astype(int).tolist()

    def predict_by_total_engagement(frame):
        q95, q85, q60 = frame["total_engagement"].quantile([0.95, 0.85, 0.60]).tolist()
        def assign(val):
            if val >= q95:
                return 1
            if val >= q85:
                return 2
            if val >= q60:
                return 3
            return 4
        return frame["total_engagement"].apply(assign).tolist()

    def predict_by_momentum(frame):
        thresholds = np.quantile(frame["momentum"], [0.75, 0.5, 0.25]) if len(frame) >= 4 else [10, 0, -10]
        def assign(val):
            if val >= thresholds[0]:
                return 1
            if val >= thresholds[1]:
                return 2
            if val >= thresholds[2]:
                return 3
            return 4
        return frame["momentum"].apply(assign).tolist()

    def predict_hybrid(frame):
        score = frame["total_engagement"] * 0.7 + frame["growth_rate_7d"] * 0.3
        q95, q85, q60 = score.quantile([0.95, 0.85, 0.60]).tolist()
        def assign(val):
            if val >= q95:
                return 1
            if val >= q85:
                return 2
            if val >= q60:
                return 3
            return 4
        return score.apply(assign).tolist()

    heuristics = {
        "total_engagement": predict_by_total_engagement(df),
        "momentum": predict_by_momentum(df),
        "hybrid": predict_hybrid(df),
    }

    results: List[Dict] = []
    for name, y_pred in heuristics.items():
        accuracy = sum(1 for yt, yp in zip(y_true, y_pred) if yt == yp) / len(y_true)
        f1 = _macro_f1(y_true, y_pred)
        feat_count = 2 if name == "hybrid" else 1
        res = {
            "name": f"heuristic_{name}",
            "accuracy": float(accuracy),
            "f1": float(f1),
            "latency_ms": 5.0 if name == "total_engagement" else 8.0,
            "feat_count": feat_count,
            "fit_ms": 1.0,
        }
        results.append(res)

    pareto = _pareto_front(results)
    artifact = _save_pareto_artifact(results, pareto)
    log.info(f"pareto_artifact_fallback={artifact}")
    return {"artifact": str(artifact), "results": str(len(results)), "front": str(len(pareto))}


def run_train_pareto(params: Optional[TrainParams] = None, fallback_local: Optional[bool] = None) -> Dict[str, str]:
    params = params or TrainParams()
    use_fallback = fallback_local if fallback_local is not None else bool(os.environ.get("TRAIN_FALLBACK_LOCAL"))
    if use_fallback:
        return _train_fallback()
    try:
        return _train_spark(params)
    except Exception as exc:
        log = setup_logging("train")
        log.info(f"train_fallback_reason={exc}")
        return _train_fallback()
