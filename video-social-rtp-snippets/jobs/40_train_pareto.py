"""Snippet: Multi-model training with Pareto front selection + MLflow tracking.
- 다중 모델 학습 (LogisticRegression, RandomForest, GBT)
- Pareto Front 선택 (accuracy, f1, latency, feat_count)
- MLflow 실험 추적 (선택적)

실행:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/40_train_pareto.py
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/40_train_pareto.py --no-mlflow
"""
import os
import sys
import time
import json
import argparse
from pathlib import Path

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import GOLD, ARTIFACT, ensure_dirs

ensure_dirs(ARTIFACT)

# Spark imports
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    DecisionTreeClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)


def create_spark():
    """Create Spark session with Delta Lake."""
    return (SparkSession.builder
        .appName("pareto_training_snippet")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(ARTIFACT / "_warehouse"))
        .getOrCreate())


def dominates(a: dict, b: dict) -> bool:
    """Check if solution 'a' dominates solution 'b' (Pareto dominance)."""
    # Maximize: accuracy, f1
    # Minimize: latency_ms, feat_count
    better_or_equal = (
        a["accuracy"] >= b["accuracy"] and
        a["f1"] >= b["f1"] and
        a["latency_ms"] <= b["latency_ms"] and
        a["feat_count"] <= b["feat_count"]
    )
    strictly_better = (
        a["accuracy"] > b["accuracy"] or
        a["f1"] > b["f1"] or
        a["latency_ms"] < b["latency_ms"] or
        a["feat_count"] < b["feat_count"]
    )
    return better_or_equal and strictly_better


def find_pareto_front(results: list) -> list:
    """Find non-dominated solutions (Pareto front)."""
    pareto = []
    for r in results:
        if not any(dominates(o, r) for o in results if o is not r):
            pareto.append(r)
    return pareto


def train_and_evaluate(spark, df, feat_cols: list, use_mlflow: bool = True):
    """Train multiple models and evaluate."""
    # Prepare features
    si = StringIndexer(inputCol="tier", outputCol="label_idx", handleInvalid="keep")
    va = VectorAssembler(inputCols=feat_cols, outputCol="features", handleInvalid="keep")

    # Model candidates (all support multiclass classification)
    candidates = [
        ("lr", LogisticRegression(labelCol="label_idx", maxIter=50)),
        ("rf", RandomForestClassifier(labelCol="label_idx", numTrees=100, maxDepth=8)),
        ("dt", DecisionTreeClassifier(labelCol="label_idx", maxDepth=8)),
    ]

    # Evaluators
    binary_eval = BinaryClassificationEvaluator(labelCol="label_idx", metricName="areaUnderPR")
    multi_eval = MulticlassClassificationEvaluator(labelCol="label_idx", metricName="accuracy")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label_idx", metricName="f1")

    # Optional MLflow setup
    mlflow = None
    if use_mlflow:
        try:
            import mlflow as _mlflow
            mlflow = _mlflow
            mlflow.set_tracking_uri(f"file://{ARTIFACT / 'mlruns'}")
            mlflow.set_experiment("train_pareto")
            print("[INFO] MLflow tracking enabled")
        except ImportError:
            print("[WARN] MLflow not installed, tracking disabled")
            mlflow = None

    results = []
    for name, clf in candidates:
        print(f"\n[INFO] Training model: {name}")

        # Start MLflow run
        if mlflow:
            mlflow.start_run(run_name=name)

        try:
            # Build and fit pipeline
            pipe = Pipeline(stages=[si, va, clf])
            t0 = time.time()
            model = pipe.fit(df)
            train_time = time.time() - t0

            # Predict and evaluate
            t0 = time.time()
            pred = model.transform(df)
            latency_ms = (time.time() - t0) * 1000

            accuracy = multi_eval.evaluate(pred)
            f1 = f1_eval.evaluate(pred)

            result = {
                "name": name,
                "accuracy": round(accuracy, 4),
                "f1": round(f1, 4),
                "latency_ms": round(latency_ms, 2),
                "feat_count": len(feat_cols),
                "train_time_s": round(train_time, 2),
            }
            results.append(result)

            print(f"  Accuracy: {accuracy:.4f}, F1: {f1:.4f}, Latency: {latency_ms:.2f}ms")

            # Log to MLflow
            if mlflow:
                mlflow.log_params({"model_type": name, "feat_count": len(feat_cols)})
                mlflow.log_metrics({
                    "accuracy": accuracy,
                    "f1": f1,
                    "latency_ms": latency_ms,
                    "train_time_s": train_time
                })
                # Save model
                mlflow.spark.log_model(model, f"model_{name}")

        except Exception as e:
            print(f"  [ERROR] Training failed: {e}")

        finally:
            if mlflow:
                mlflow.end_run()

    return results


def main():
    parser = argparse.ArgumentParser(description="Pareto training with MLflow")
    parser.add_argument("--no-mlflow", action="store_true", help="Disable MLflow tracking")
    args = parser.parse_args()

    spark = create_spark()

    try:
        # Read Gold features
        gold_path = GOLD / "features"
        if not gold_path.exists():
            print(f"[ERROR] Gold path does not exist: {gold_path}")
            return

        df = spark.read.format("delta").load(str(gold_path)).fillna(0)
        print(f"[INFO] Loaded Gold data: {df.count()} rows")

        # Feature columns
        feat_cols = [
            "total_engagement",
            "avg_engagement",
            "max_engagement",
            "total_videos",
            "unique_viewers_est",
            "volatility",
            "market_share"
        ]

        # Check available columns
        available_cols = [c for c in feat_cols if c in df.columns]
        print(f"[INFO] Feature columns: {available_cols}")

        if len(available_cols) < 2:
            print("[ERROR] Not enough feature columns")
            return

        # Train and evaluate
        results = train_and_evaluate(spark, df, available_cols, use_mlflow=not args.no_mlflow)

        if not results:
            print("[ERROR] No training results")
            return

        # Find Pareto front
        pareto = find_pareto_front(results)

        print("\n" + "=" * 60)
        print("[INFO] All results:")
        for r in results:
            print(f"  {r['name']}: acc={r['accuracy']}, f1={r['f1']}, latency={r['latency_ms']}ms")

        print("\n[INFO] Pareto front (non-dominated):")
        for r in pareto:
            print(f"  * {r['name']}: acc={r['accuracy']}, f1={r['f1']}, latency={r['latency_ms']}ms")

        # Save results
        pareto_path = ARTIFACT / "pareto.json"
        with pareto_path.open("w", encoding="utf-8") as f:
            json.dump({
                "all_results": results,
                "pareto_front": pareto,
                "feature_columns": available_cols
            }, f, ensure_ascii=False, indent=2)
        print(f"\n[INFO] Results saved: {pareto_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
