from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Dict


ROOT = Path(__file__).resolve().parents[2]


def _load_env_file(env_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not env_path.exists():
        return data
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # simple ${PROJECT_ROOT} expansion
        if "${PROJECT_ROOT}" in v:
            v = v.replace("${PROJECT_ROOT}", os.environ.get("PROJECT_ROOT", "project"))
        data[k] = v
    return data


def _ensure_env_loaded() -> None:
    # one-time soft load
    env_path = ROOT / ".env"
    loaded = _load_env_file(env_path)
    for k, v in loaded.items():
        os.environ.setdefault(k, v)


def _abs(path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return (ROOT / p).resolve()


@dataclass
class Settings:
    project_root: Path
    landing_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    checkpoint_dir: Path
    log_dir: Path
    artifact_dir: Path
    yt_api_key: str | None = None
    bronze_bloom_capacity: int = 500_000
    bronze_bloom_error_rate: float = 0.01
    bronze_bloom_lookback_days: int = 7


def load_settings() -> Settings:
    _ensure_env_loaded()
    project_root = _abs(os.environ.get("PROJECT_ROOT", "project"))
    landing = _abs(os.environ.get("LANDING_DIR", f"{project_root}/data/landing"))
    bronze = _abs(os.environ.get("BRONZE_DIR", f"{project_root}/data/bronze"))
    silver = _abs(os.environ.get("SILVER_DIR", f"{project_root}/data/silver"))
    gold = _abs(os.environ.get("GOLD_DIR", f"{project_root}/data/gold"))
    chk = _abs(os.environ.get("CHECKPOINT_DIR", f"{project_root}/chk"))
    logs = _abs(os.environ.get("LOG_DIR", f"{project_root}/logs"))
    arts = _abs(os.environ.get("ARTIFACT_DIR", f"{project_root}/artifacts"))
    key = os.environ.get("YT_API_KEY") or os.environ.get("YOUTUBE_API_KEY")
    bloom_capacity = int(os.environ.get("BRONZE_BLOOM_CAPACITY", "500000"))
    bloom_error = float(os.environ.get("BRONZE_BLOOM_ERROR_RATE", "0.01"))
    bloom_days = int(os.environ.get("BRONZE_BLOOM_LOOKBACK_DAYS", "7"))
    return Settings(
        project_root=project_root,
        landing_dir=landing,
        bronze_dir=bronze,
        silver_dir=silver,
        gold_dir=gold,
        checkpoint_dir=chk,
        log_dir=logs,
        artifact_dir=arts,
        yt_api_key=key,
        bronze_bloom_capacity=bloom_capacity,
        bronze_bloom_error_rate=bloom_error,
        bronze_bloom_lookback_days=bloom_days,
    )


def ensure_dirs(s: Settings) -> None:
    for p in [
        s.project_root,
        s.landing_dir,
        s.bronze_dir,
        s.silver_dir,
        s.gold_dir,
        s.checkpoint_dir,
        s.log_dir,
        s.artifact_dir,
    ]:
        p.mkdir(parents=True, exist_ok=True)
