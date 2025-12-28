"""공통 유틸리티: 환경변수 로드, 경로 관리, 샘플링."""
import os
import random
from pathlib import Path
from typing import Dict, List, Any

ROOT = Path(__file__).resolve().parents[1]


def load_env_file(env_path: Path = None) -> Dict[str, str]:
    """Load .env file and return key-value pairs."""
    if env_path is None:
        env_path = ROOT / "conf" / ".env"
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
        data[k] = v
    return data


def ensure_env_loaded() -> None:
    """Load .env file into environment (soft, setdefault only)."""
    loaded = load_env_file()
    for k, v in loaded.items():
        os.environ.setdefault(k, v)


def get_path(env_key: str, default: str) -> Path:
    """Get path from environment, resolve to absolute under ROOT if relative."""
    ensure_env_loaded()
    raw = os.getenv(env_key, default)
    p = Path(raw)
    if p.is_absolute():
        return p
    return (ROOT / p).resolve()


def reservoir_sample(iterable, k: int = 64) -> List[Any]:
    """Vitter's Algorithm R: reservoir sampling."""
    sample, n = [], 0
    for x in iterable:
        n += 1
        if len(sample) < k:
            sample.append(x)
        else:
            j = random.randint(1, n)
            if j <= k:
                sample[j - 1] = x
    return sample


def ensure_dirs(*paths: Path) -> None:
    """Create directories if they don't exist."""
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


# Default paths
LANDING = get_path("LANDING_DIR", "data/landing")
BRONZE = get_path("BRONZE_DIR", "data/bronze")
SILVER = get_path("SILVER_DIR", "data/silver")
GOLD = get_path("GOLD_DIR", "data/gold")
CHECKPOINT = get_path("CHECKPOINT_DIR", "chk")
ARTIFACT = get_path("ARTIFACT_DIR", "data/artifacts")


if __name__ == "__main__":
    print("ROOT:", ROOT)
    print("LANDING:", LANDING)
    print("BRONZE:", BRONZE)
    print("SILVER:", SILVER)
    print("GOLD:", GOLD)
