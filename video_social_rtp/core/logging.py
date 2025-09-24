from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from .config import load_settings, ensure_dirs


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "lvl": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(name: str = "video_social_rtp") -> logging.Logger:
    s = load_settings()
    ensure_dirs(s)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers[:] = []

    # stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonLineFormatter())
    logger.addHandler(sh)

    # file handler
    log_file = Path(s.log_dir) / f"{name}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(JsonLineFormatter())
    logger.addHandler(fh)

    logger.propagate = False
    return logger

