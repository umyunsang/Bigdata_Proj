from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

from .config import Settings

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


def _prepare_runtime_dirs(settings: Settings) -> Tuple[str, Path]:
    local_ip = os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    tmp_root = settings.project_root / "tmp"
    spark_tmp = tmp_root / "spark_local"
    ivy_dir = tmp_root / "ivy_cache"
    py4j_tmp = tmp_root / "py4j"
    for d in (spark_tmp, ivy_dir, py4j_tmp):
        d.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("SPARK_LOCAL_DIRS", str(spark_tmp))
    os.environ.setdefault("PYSPARK_TEMP_DIR", str(py4j_tmp))
    return local_ip, ivy_dir.resolve()


def get_spark_session(app_name: str, settings: Settings, log=None) -> Tuple["SparkSession", bool]:
    """Create a SparkSession that honours sandbox restrictions.

    Returns a tuple of (spark_session, delta_enabled).
    """
    local_ip, ivy_dir = _prepare_runtime_dirs(settings)

    from pyspark.sql import SparkSession  # imported lazily

    master = os.environ.get("SPARK_MASTER", "local[2]")

    def build(enable_delta: bool) -> SparkSession.Builder:
        builder = SparkSession.builder.appName(app_name).master(master)
        builder = builder.config("spark.sql.warehouse.dir", str((settings.project_root / "warehouse").resolve()))
        builder = builder.config("spark.jars.ivy", str(ivy_dir))
        builder = builder.config("spark.driver.host", local_ip)
        builder = builder.config("spark.driver.bindAddress", local_ip)
        builder = builder.config("spark.local.dir", str((settings.project_root / "tmp" / "spark_local")))
        builder = builder.config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        builder = builder.config("spark.ui.enabled", "false")
        builder = builder.config("spark.sql.session.timeZone", os.environ.get("SPARK_SQL_TIMEZONE", "UTC"))
        # 저장 공간 최적화를 위한 압축 설정
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
        builder = builder.config("spark.sql.parquet.enableVectorizedReader", "true")
        builder = builder.config("spark.sql.parquet.mergeSchema", "false")
        if enable_delta:
            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        return builder

    delta_enabled = False
    if os.environ.get("SPARK_DISABLE_DELTA", "0") != "1":
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore
        except Exception as exc:  # pragma: no cover - import guard
            if log:
                log.info(f"delta_disabled_import={exc}")
        else:
            try:
                builder_delta = configure_spark_with_delta_pip(build(True))
                spark = builder_delta.getOrCreate()
                delta_enabled = True
                return spark, delta_enabled
            except Exception as exc:
                if log:
                    log.info(f"delta_disabled_reason={exc}")

    spark = build(False).getOrCreate()
    return spark, delta_enabled
