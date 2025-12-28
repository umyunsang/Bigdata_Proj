"""Snippet: Prediction streaming with trained model.
- 학습된 모델로 실시간 예측
- 예측 결과를 Delta 테이블에 저장
- 배치 모드로 전체 예측 생성

실행:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/50_predict_stream.py
"""
import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import GOLD, ARTIFACT, ensure_dirs

ensure_dirs(GOLD / "predictions", ARTIFACT)

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, udf
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml import PipelineModel


def create_spark():
    """Create Spark session with Delta Lake."""
    return (SparkSession.builder
        .appName("predict_stream_snippet")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(ARTIFACT / "_warehouse"))
        .getOrCreate())


def load_pareto_info():
    """Load Pareto front results to find best model."""
    pareto_path = ARTIFACT / "pareto.json"
    if pareto_path.exists():
        with pareto_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return None


def rule_based_prediction(row):
    """Simple rule-based prediction when no ML model available."""
    engagement = row.get("total_engagement", 0)
    market_share = row.get("market_share", 0)

    if engagement > 100 and market_share > 0.1:
        return 1  # Tier 1 (High potential)
    elif engagement > 50:
        return 2  # Tier 2
    elif engagement > 10:
        return 3  # Tier 3
    else:
        return 4  # Tier 4


def predict_with_model(spark, df, model_path: Path):
    """Load model and make predictions."""
    try:
        model = PipelineModel.load(str(model_path))
        predictions = model.transform(df)
        return predictions.withColumn("prediction_method", lit("ml_model"))
    except Exception as e:
        print(f"[WARN] Could not load model: {e}")
        return None


def predict_with_rules(df):
    """Apply rule-based predictions."""
    return df.withColumn(
        "predicted_tier",
        when(col("total_engagement") > 100, 1)
        .when(col("total_engagement") > 50, 2)
        .when(col("total_engagement") > 10, 3)
        .otherwise(4)
    ).withColumn(
        "viral_potential",
        when(col("tier") <= 2, "HIGH")
        .when(col("tier") == 3, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "prediction_method",
        lit("rule_based")
    )


def main():
    parser = argparse.ArgumentParser(description="Prediction streaming")
    parser.add_argument("--model", default=None, help="Path to trained model")
    args = parser.parse_args()

    spark = create_spark()

    try:
        # Read Gold features
        gold_path = GOLD / "features"
        if not gold_path.exists():
            print(f"[ERROR] Gold path does not exist: {gold_path}")
            return

        features = spark.read.format("delta").load(str(gold_path))
        print(f"[INFO] Loaded features: {features.count()} artists")

        # Load Pareto info
        pareto_info = load_pareto_info()
        if pareto_info:
            print(f"[INFO] Pareto front models: {[m['name'] for m in pareto_info.get('pareto_front', [])]}")

        # Try ML model first, fall back to rules
        predictions = None
        if args.model:
            predictions = predict_with_model(spark, features, Path(args.model))

        if predictions is None:
            print("[INFO] Using rule-based prediction")
            predictions = predict_with_rules(features)

        # Add timestamp and metadata
        predictions = (predictions
            .withColumn("prediction_time", current_timestamp())
            .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))))

        # Select output columns
        output_cols = [
            "artist",
            "tier",
            "predicted_tier" if "predicted_tier" in predictions.columns else "tier",
            "viral_potential" if "viral_potential" in predictions.columns else lit("N/A"),
            "total_engagement",
            "market_share",
            "trend_direction",
            "prediction_method",
            "prediction_time",
            "batch_id"
        ]

        # Filter to available columns
        available_cols = [c for c in output_cols if c in predictions.columns or not isinstance(c, str)]
        output = predictions.select(*[c for c in available_cols if c in predictions.columns])

        # Append to predictions table
        (output.write
            .format("delta")
            .mode("append")
            .save(str(GOLD / "predictions")))

        print(f"[INFO] Predictions appended: {output.count()} rows")

        # Show sample predictions
        print("\n[INFO] Sample predictions:")
        output.select("artist", "tier", "viral_potential", "trend_direction").show(10)

        # Summary by viral potential
        if "viral_potential" in output.columns:
            print("\n[INFO] Viral potential distribution:")
            output.groupBy("viral_potential").count().orderBy("viral_potential").show()

        # Export to CSV for dashboard
        csv_path = GOLD / "predictions_latest.csv"
        output.toPandas().to_csv(str(csv_path), index=False)
        print(f"[INFO] CSV exported: {csv_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
