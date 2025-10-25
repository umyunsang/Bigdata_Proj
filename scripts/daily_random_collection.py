#!/usr/bin/env python3
"""
Daily Random YouTube Video Collection Script

Collects K-POP videos from YouTube Data API v3 using random sampling strategy.
Target: ~4,500-5,000 videos per day within 9,500 QPD quota.
"""

import argparse
import csv
import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Append parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from video_social_rtp.core.config import load_settings
from video_social_rtp.core.logging import setup_logging
from video_social_rtp.ingest.youtube import fetch_to_landing, IngestParams


# K-POP Artists for random query
ARTISTS = [
    "BLACKPINK", "NewJeans", "BTS", "IVE", "aespa",
    "TWICE", "LESSERAFIM", "ITZY", "Stray Kids", "NMIXX",
    "SEVENTEEN", "NCT", "ENHYPEN", "ATEEZ", "BABYMONSTER",
    "2NE1", "Red Velvet", "TREASURE", "TXT", "G-IDLE"
]

# Sort order types
ORDER_TYPES = ["relevance", "viewCount", "date", "rating"]

# 2024 Quarters
QUARTERS_2024 = [
    ("2024-01-01T00:00:00Z", "2024-03-31T23:59:59Z", "2024-Q1"),
    ("2024-04-01T00:00:00Z", "2024-06-30T23:59:59Z", "2024-Q2"),
    ("2024-07-01T00:00:00Z", "2024-09-30T23:59:59Z", "2024-Q3"),
    ("2024-10-01T00:00:00Z", "2024-12-31T23:59:59Z", "2024-Q4"),
]


def collect_random_batch(
    quota_limit: int = 9500,
    max_results: int = 50,
    delay_seconds: float = 1.0
) -> List[Dict]:
    """
    Collect videos using random sampling strategy.

    Args:
        quota_limit: Maximum QPD to use (default 9500)
        max_results: Results per API call (default 50, max allowed)
        delay_seconds: Delay between API calls (default 1.0)

    Returns:
        List of video data dictionaries
    """
    log = setup_logging("daily_collection")
    s = load_settings()

    # Calculate number of calls
    quota_per_call = 100
    max_calls = quota_limit // quota_per_call  # 9500 / 100 = 95 calls
    log.info(f"Starting random collection: {max_calls} calls, {max_calls * max_results} target videos")

    collected = []
    seen_video_ids = set()
    quota_used = 0
    landing_files = []

    for call_num in range(max_calls):
        # Random parameters
        artist = random.choice(ARTISTS)
        order = random.choice(ORDER_TYPES)
        quarter = random.choice(QUARTERS_2024)
        published_after, published_before, quarter_label = quarter

        try:
            log.info(f"Call {call_num + 1}/{max_calls}: q={artist}, order={order}, quarter={quarter_label}")

            # Create params for this call
            params = IngestParams(
                query=artist,
                max_items=max_results,
                region_code="KR",
                relevance_language="ko",
                lang="ko",
                source="yt"
            )

            # Fetch to landing (creates NDJSON file)
            result = fetch_to_landing(params=params, skip_if_exists=False)

            if result.get("skipped") == "true":
                log.info(f"Skipped (already fetched today): {artist}")
                continue

            landing_file = result.get("landing_file")
            if landing_file:
                landing_files.append(landing_file)

                # Read the landing file and extract data
                with open(landing_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        event = json.loads(line.strip())
                        video_id = event.get("video_id")
                        if video_id and video_id not in seen_video_ids:
                            # Extract metadata
                            collected_item = {
                                "video_id": video_id,
                                "channel_id": event.get("author_id", ""),
                                "title": event.get("text", ""),
                                "description": "",  # Not available in landing format
                                "published_at": event.get("ts", ""),
                                "artist": artist,
                                "quarter": quarter_label,
                                "collected_at": datetime.now().isoformat(),
                                "order_type": order,
                            }
                            collected.append(collected_item)
                            seen_video_ids.add(video_id)

            quota_used += quota_per_call
            log.info(f"Unique videos: {len(seen_video_ids)}, quota used: {quota_used}")

            # Rate limiting
            time.sleep(delay_seconds)

        except Exception as e:
            log.error(f"Error in call {call_num + 1}: {e}")
            continue

    log.info(f"Collection complete: {len(collected)} unique videos, {quota_used} QPD used")
    log.info(f"Landing files created: {len(landing_files)}")
    return collected


def save_daily_csv(data: List[Dict], output_dir: Path) -> Path:
    """Save collected data to daily CSV file."""
    today = datetime.now().strftime("%Y%m%d")
    filename = f"youtube_random_2024_{today}.csv"
    filepath = output_dir / filename

    # Ensure directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write CSV
    if data:
        fieldnames = [
            "video_id", "channel_id", "title", "description",
            "published_at", "artist", "quarter", "collected_at", "order_type"
        ]
        with filepath.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    print(f"✅ Daily CSV saved: {filepath} ({len(data)} videos)")
    return filepath


def merge_to_master(daily_csv: Path, master_csv: Path) -> None:
    """Merge daily CSV into master file with deduplication."""
    log = setup_logging("daily_collection")

    # Load existing master data
    existing_ids = set()
    existing_rows = []

    if master_csv.exists():
        with master_csv.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row["video_id"])
                existing_rows.append(row)

    # Load daily data
    daily_rows = []
    with daily_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["video_id"] not in existing_ids:
                daily_rows.append(row)
                existing_ids.add(row["video_id"])

    # Merge and write
    all_rows = existing_rows + daily_rows

    fieldnames = [
        "video_id", "channel_id", "title", "description",
        "published_at", "artist", "quarter", "collected_at", "order_type"
    ]

    with master_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    log.info(f"Master CSV updated: {len(existing_rows)} existing + {len(daily_rows)} new = {len(all_rows)} total")
    print(f"✅ Master CSV updated: {master_csv}")
    print(f"   Previous: {len(existing_rows)} videos")
    print(f"   New today: {len(daily_rows)} videos")
    print(f"   Total: {len(all_rows)} videos")


def main():
    parser = argparse.ArgumentParser(description="Daily random YouTube video collection")
    parser.add_argument("--quota", type=int, default=9500, help="QPD limit (default: 9500)")
    parser.add_argument("--max-results", type=int, default=50, help="Results per call (default: 50)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between calls in seconds (default: 1.0)")
    parser.add_argument("--output-dir", type=str, default="project/data/raw", help="Output directory")
    args = parser.parse_args()

    print("=" * 60)
    print("📊 Daily Random YouTube Video Collection")
    print("=" * 60)
    print(f"Target QPD: {args.quota}")
    print(f"Results per call: {args.max_results}")
    print(f"Estimated videos: {(args.quota // 100) * args.max_results}")
    print("=" * 60)

    # Collect data
    data = collect_random_batch(
        quota_limit=args.quota,
        max_results=args.max_results,
        delay_seconds=args.delay
    )

    if not data:
        print("⚠️  No data collected!")
        return

    # Save daily CSV
    output_dir = Path(args.output_dir)
    daily_csv = save_daily_csv(data, output_dir)

    # Merge to master
    master_csv = output_dir / "youtube_random_2024.csv"
    merge_to_master(daily_csv, master_csv)

    print("=" * 60)
    print("✅ Daily collection complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
