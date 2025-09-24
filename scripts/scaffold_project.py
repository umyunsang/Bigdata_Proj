#!/usr/bin/env python3
"""
Step 00: Create project runtime directories based on env or defaults.

Rules:
- Default PROJECT_ROOT=./project
- Accept .env (KEY=VALUE) if present; simple parser without external deps
- Create: data/{landing,bronze,silver,gold}, chk, logs, artifacts
"""
import os
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]


def load_env(env_path: Path) -> Dict[str, str]:
    vals: Dict[str, str] = {}
    if not env_path.exists():
        return vals
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        # trim simple quotes if present
        v = v.strip().strip('"').strip("'")
        # naive ${VAR} expansion for PROJECT_ROOT only
        if "${PROJECT_ROOT}" in v:
            v = v.replace("${PROJECT_ROOT}", os.environ.get("PROJECT_ROOT", "project"))
        vals[k] = v
    return vals


def main() -> None:
    # merge env from .env if exists
    env_file = ROOT / ".env"
    loaded = load_env(env_file)
    for k, v in loaded.items():
        os.environ.setdefault(k, v)

    project_root = os.environ.get("PROJECT_ROOT", "project")
    pr = ROOT / project_root

    landing = Path(os.environ.get("LANDING_DIR", f"{project_root}/data/landing"))
    bronze = Path(os.environ.get("BRONZE_DIR", f"{project_root}/data/bronze"))
    silver = Path(os.environ.get("SILVER_DIR", f"{project_root}/data/silver"))
    gold = Path(os.environ.get("GOLD_DIR", f"{project_root}/data/gold"))
    chk = Path(os.environ.get("CHECKPOINT_DIR", f"{project_root}/chk"))
    logs = Path(os.environ.get("LOG_DIR", f"{project_root}/logs"))
    artifacts = Path(os.environ.get("ARTIFACT_DIR", f"{project_root}/artifacts"))

    to_make = [
        pr,
        landing,
        bronze,
        silver,
        gold,
        chk,
        logs,
        artifacts,
    ]

    created = []
    for p in to_make:
        ap = (ROOT / p) if not p.is_absolute() else p
        ap.mkdir(parents=True, exist_ok=True)
        created.append(str(ap.relative_to(ROOT) if str(ap).startswith(str(ROOT)) else ap))

    print("[Step 00] Project directories ready:")
    for c in created:
        print(" -", c)
    print("\nNext:")
    print(" - Verify .env values (copied from .env.example)")
    print(" - Proceed to Step 01 (YouTube fetch → landing, then bronze)")


if __name__ == "__main__":
    main()
