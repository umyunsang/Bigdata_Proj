"""Snippet: YouTube API fetcher -> NDJSON landing.
- YouTube Data API v3 연동 (API 키 없으면 mock 데이터 생성)
- Reservoir Sampling (Vitter's Algorithm R)
- 마커 파일로 중복 fetch 방지

실행:
  python jobs/00_fetch_to_landing.py --query "NewJeans" --max-items 50 --region KR
"""
import os
import sys
import time
import json
import hashlib
import argparse
from datetime import datetime, date
from pathlib import Path

# Add conf to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from conf.utils import LANDING, BRONZE, reservoir_sample, ensure_dirs

ensure_dirs(LANDING, BRONZE / "_sample", LANDING / "_markers")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_published_at(published_at_str: str) -> int:
    """Parse YouTube publishedAt ISO 8601 string to milliseconds timestamp."""
    try:
        dt = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except Exception:
        return _now_ms()


def mock_events(query: str, max_items: int, lang: str = "ko") -> list:
    """Generate mock events when no API key available."""
    artists = ["BTS", "BLACKPINK", "NewJeans", "AESPA", "IVE", "TWICE"]
    items = []
    for i in range(max_items):
        artist = artists[i % len(artists)]
        items.append({
            "post_id": f"mock_{int(time.time())}_{i}",
            "text": f"[{artist}] {query} 관련 영상 {i}",
            "lang": lang,
            "ts": _now_ms(),
            "author_id": f"channel_{artist.lower()}_{i % 5}",
            "video_id": f"vid_{i:04d}",
            "source": "yt_mock",
        })
    return items


def fetch_youtube(query: str, max_items: int = 50, region_code: str = None,
                  relevance_language: str = None, api_key: str = None) -> list:
    """Fetch videos from YouTube Data API v3."""
    if not api_key:
        print("[WARN] YT_API_KEY not set, using mock data")
        return mock_events(query, max_items, relevance_language or "ko")

    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("[WARN] google-api-python-client not installed, using mock data")
        return mock_events(query, max_items, relevance_language or "ko")

    try:
        yt = build("youtube", "v3", developerKey=api_key)
        api_params = {
            "q": query,
            "part": "snippet",
            "type": "video",
            "maxResults": min(max_items, 50),  # YouTube API max is 50
        }
        if region_code:
            api_params["regionCode"] = region_code
        if relevance_language:
            api_params["relevanceLanguage"] = relevance_language

        req = yt.search().list(**api_params)
        res = req.execute()

        items = []
        for i, it in enumerate(res.get("items", [])):
            idobj = it.get("id", {}) or {}
            vid = idobj.get("videoId")
            snip = it.get("snippet", {}) or {}
            if not vid:
                continue

            published_at = snip.get("publishedAt", "")
            timestamp = _parse_published_at(published_at) if published_at else _now_ms()

            items.append({
                "post_id": f"search{i}_{vid}",
                "text": snip.get("title", ""),
                "lang": relevance_language or "en",
                "ts": timestamp,
                "author_id": snip.get("channelId", ""),
                "video_id": vid,
                "source": "yt",
            })

        if not items:
            print("[WARN] No items from API, using mock data")
            return mock_events(query, max_items, relevance_language or "ko")

        print(f"[INFO] Fetched {len(items)} items from YouTube API")
        return items

    except Exception as e:
        print(f"[WARN] API error: {e}, using mock data")
        return mock_events(query, max_items, relevance_language or "ko")


def check_marker(query: str, region_code: str, relevance_language: str) -> tuple:
    """Check if today's same query was already fetched. Returns (exists, marker_path)."""
    mdir = LANDING / "_markers"
    qsig = f"{date.today().isoformat()}|{query}|{region_code or ''}|{relevance_language or ''}"
    mh = hashlib.md5(qsig.encode("utf-8")).hexdigest()[:12]
    marker = mdir / f"{date.today().isoformat()}_{mh}.json"
    return marker.exists(), marker


def write_ndjson(path: Path, items: list) -> None:
    """Write items as NDJSON (newline-delimited JSON)."""
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Fetch YouTube data to landing")
    parser.add_argument("--query", default="K-POP", help="Search query")
    parser.add_argument("--max-items", type=int, default=50, help="Max items to fetch")
    parser.add_argument("--region", dest="region_code", default=None, help="Region code (e.g., KR)")
    parser.add_argument("--lang", dest="relevance_language", default=None, help="Relevance language (e.g., ko)")
    parser.add_argument("--reservoir-k", type=int, default=16, help="Reservoir sample size")
    parser.add_argument("--force", action="store_true", help="Force fetch even if marker exists")
    args = parser.parse_args()

    # Check marker
    exists, marker = check_marker(args.query, args.region_code, args.relevance_language)
    if exists and not args.force:
        print(f"[SKIP] Already fetched today: {marker}")
        return

    # Fetch
    api_key = os.getenv("YT_API_KEY") or os.getenv("YOUTUBE_API_KEY")
    items = fetch_youtube(
        query=args.query,
        max_items=args.max_items,
        region_code=args.region_code,
        relevance_language=args.relevance_language,
        api_key=api_key
    )

    # Write NDJSON landing file
    fname = LANDING / f"events_{int(time.time())}.json"
    write_ndjson(fname, items)
    print(f"Wrote: {fname} ({len(items)} items)")

    # Reservoir sample
    sample = reservoir_sample(items, k=args.reservoir_k)
    sname = BRONZE / "_sample" / f"sample_{int(time.time())}.json"
    sname.parent.mkdir(parents=True, exist_ok=True)
    with sname.open("w", encoding="utf-8") as f:
        json.dump(sample, f, ensure_ascii=False, indent=2)
    print(f"Sample saved: {sname} ({len(sample)} items)")

    # Write marker
    marker.write_text(json.dumps({
        "query": args.query,
        "region_code": args.region_code,
        "relevance_language": args.relevance_language,
        "ts": int(time.time()),
        "items": len(items),
        "landing_file": str(fname)
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Marker: {marker}")


if __name__ == "__main__":
    main()
