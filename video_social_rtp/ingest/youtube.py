from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from datetime import date
import hashlib

from ..core.config import load_settings, ensure_dirs
from ..core.logging import setup_logging
from ..core.sampling import reservoir_sample


@dataclass
class IngestParams:
    query: str = "data engineering"
    max_items: int = 100
    lang: str = "en"
    reservoir_k: int = 64
    source: str = "yt"
    region_code: Optional[str] = None  # e.g., "KR"
    relevance_language: Optional[str] = None  # e.g., "ko"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _mock_events(p: IngestParams) -> List[Dict]:
    items = []
    for i in range(p.max_items):
        items.append({
            "post_id": f"p{i}",
            "text": f"mock event {i} about {p.query}",
            "lang": p.lang,
            "ts": _now_ms(),
            "author_id": f"u{i%11}",
            "video_id": f"v{i%7}",
            "source": p.source,
        })
    return items


def _write_ndjson(path: Path, items: Iterable[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def fetch_to_landing(params: Optional[IngestParams] = None, use_mock: Optional[bool] = None, skip_if_exists: bool = True) -> Dict[str, str]:
    """Fetch events and write NDJSON to landing. Returns paths (file and sample).

    If YT_API_KEY is missing or use_mock=True, generates mock events.
    """
    s = load_settings()
    ensure_dirs(s)
    log = setup_logging("ingest")
    params = params or IngestParams()

    # Optional: skip if today's same query already fetched
    # Marker key = YYYY-MM-DD + query + region + rel-lang
    mdir = Path(s.landing_dir) / "_markers"
    mdir.mkdir(parents=True, exist_ok=True)
    qsig = f"{date.today().isoformat()}|{params.query}|{params.region_code or ''}|{params.relevance_language or ''}"
    mh = hashlib.md5(qsig.encode("utf-8")).hexdigest()[:12]
    marker = mdir / f"{date.today().isoformat()}_{mh}.json"
    if skip_if_exists and marker.exists():
        log.info(f"skip_fetch_today_marker={marker}")
        # attempt to return the latest landing file for reference
        latest = None
        try:
            cand = sorted(Path(s.landing_dir).glob("events_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            latest = cand[0] if cand else None
        except Exception:
            latest = None
        return {"skipped": "true", "marker": str(marker), "landing_file": str(latest) if latest else ""}

    # Decide mode
    api_key = s.yt_api_key
    do_mock = use_mock if use_mock is not None else (api_key is None)

    if do_mock:
        log.info("Using mock events (no API key provided)")
        items = _mock_events(params)
    else:
        # Lazy import to avoid dependency when not used
        try:
            from googleapiclient.discovery import build  # type: ignore
            from googleapiclient.errors import HttpError  # type: ignore
        except Exception as e:
            log.error(f"Failed to import googleapiclient, fallback to mock: {e}")
            items = _mock_events(params)
        else:
            try:
                yt = build("youtube", "v3", developerKey=api_key)
                req = yt.search().list(
                    q=params.query,
                    part="snippet",
                    type="video",
                    maxResults=min(params.max_items, 50),
                    regionCode=params.region_code,
                    relevanceLanguage=params.relevance_language,
                )
                res = req.execute()
                items = []
                for i, it in enumerate(res.get("items", [])):
                    idobj = it.get("id", {}) or {}
                    vid = idobj.get("videoId")
                    snip = (it.get("snippet", {}) or {})
                    if not vid:
                        # skip items without videoId
                        continue
                    items.append({
                        "post_id": f"search{i}_{vid}",
                        "text": snip.get("title", ""),
                        "lang": params.lang,
                        "ts": _now_ms(),
                        "author_id": snip.get("channelId", ""),
                        "video_id": vid,
                        "source": params.source,
                    })
                if not items:
                    raise RuntimeError("no_items_from_search")
            except Exception as e:
                log.info(f"youtube_fetch_fallback_reason={e}")
                items = _mock_events(params)

    fname = Path(s.landing_dir) / f"events_{int(time.time())}.json"
    _write_ndjson(fname, items)
    sample = reservoir_sample(items, k=params.reservoir_k)
    sname = Path(s.bronze_dir) / "_sample" / f"sample_{int(time.time())}.json"
    sname.parent.mkdir(parents=True, exist_ok=True)
    sname.write_text(json.dumps(sample, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info(f"landing_file={fname}")
    log.info(f"sample_file={sname}")
    # write/refresh marker
    try:
        marker.write_text(json.dumps({
            "query": params.query,
            "region_code": params.region_code,
            "relevance_language": params.relevance_language,
            "ts": int(time.time()),
            "items": len(items),
            "landing_file": str(fname)
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return {"landing_file": str(fname), "sample_file": str(sname), "marker": str(marker)}
