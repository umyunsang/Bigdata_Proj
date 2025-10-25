#!/usr/bin/env python3
"""
과거 7일치 데이터를 한 번에 수집하는 스크립트

YouTube Data API v3의 publishedAfter 파라미터를 사용하여
각 날짜별로 데이터를 수집하고, Bronze/Silver/Gold 처리까지 자동 실행

API 할당량 사용량:
- 검색 1회당 100 units
- 7일 × 3개 쿼리 = 21회 검색 = 2,100 units
- 일일 할당량 10,000 중 21% 사용

실행:
    python scripts/collect_7days_data.py
"""

import sys
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from video_social_rtp.core.config import load_settings, ensure_dirs
from video_social_rtp.core.logging import setup_logging


def collect_date_range_data(
    query: str,
    days_back: int = 7,
    region_code: str = "KR",
    relevance_language: str = "ko",
    max_results_per_day: int = 50
) -> List[Dict]:
    """
    과거 N일치 데이터를 날짜별로 수집

    Args:
        query: 검색 쿼리 (예: "NewJeans")
        days_back: 과거 며칠치 데이터를 수집할지
        region_code: 지역 코드 (KR, US 등)
        relevance_language: 언어 코드 (ko, en 등)
        max_results_per_day: 각 날짜당 최대 결과 수

    Returns:
        수집된 이벤트 리스트
    """
    s = load_settings()
    log = setup_logging("collect_7days")

    api_key = s.yt_api_key
    if not api_key:
        log.error("YT_API_KEY not found in .env")
        print("❌ YouTube API 키가 설정되지 않았습니다!")
        print("   .env 파일에 YT_API_KEY를 설정하세요.")
        sys.exit(1)

    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ImportError:
        log.error("google-api-python-client not installed")
        print("❌ google-api-python-client 패키지가 필요합니다!")
        print("   pip install google-api-python-client")
        sys.exit(1)

    yt = build("youtube", "v3", developerKey=api_key)

    all_items = []
    total_api_calls = 0

    print(f"\n📅 {query} 검색: 과거 {days_back}일 데이터 수집 시작")
    print("="*60)

    # 각 날짜별로 데이터 수집
    for day_offset in range(days_back):
        # 날짜 범위 계산 (UTC 기준)
        end_date = datetime.utcnow() - timedelta(days=day_offset)
        start_date = end_date - timedelta(days=1)

        # RFC 3339 형식으로 변환
        published_after = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        published_before = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        date_str = start_date.strftime("%Y-%m-%d")

        try:
            req = yt.search().list(
                q=query,
                part="snippet",
                type="video",
                maxResults=min(max_results_per_day, 50),
                regionCode=region_code,
                relevanceLanguage=relevance_language,
                publishedAfter=published_after,
                publishedBefore=published_before,
                order="date"  # 날짜순 정렬
            )

            res = req.execute()
            total_api_calls += 1

            day_items = []
            for i, item in enumerate(res.get("items", [])):
                id_obj = item.get("id", {}) or {}
                vid = id_obj.get("videoId")
                snippet = item.get("snippet", {}) or {}

                if not vid:
                    continue

                # 실제 publish 시간을 타임스탬프로 변환 (중요!)
                published_at = snippet.get("publishedAt", "")
                try:
                    # ISO 8601 format: 2024-01-15T12:34:56Z
                    pub_dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    ts = int(pub_dt.timestamp() * 1000)
                except Exception as e:
                    # Fallback to day start time if parsing fails
                    ts = int(start_date.timestamp() * 1000)

                day_items.append({
                    "post_id": f"search{date_str}_{i}_{vid}",
                    "text": snippet.get("title", ""),
                    "lang": relevance_language or "en",
                    "ts": ts,
                    "author_id": snippet.get("channelId", ""),
                    "video_id": vid,
                    "source": "yt",
                    "collected_date": date_str  # 수집 날짜 메타데이터
                })

            all_items.extend(day_items)

            print(f"  {date_str}: {len(day_items):3d} videos | "
                  f"Total: {len(all_items):4d} | API calls: {total_api_calls}")

            # API rate limit 회피 (분당 60 quota)
            time.sleep(1)

        except HttpError as e:
            log.error(f"API error on {date_str}: {e}")
            print(f"  ⚠️  {date_str}: API 오류 - {e}")
            continue
        except Exception as e:
            log.error(f"Error on {date_str}: {e}")
            print(f"  ⚠️  {date_str}: 오류 - {e}")
            continue

    print("="*60)
    print(f"✅ 수집 완료: {len(all_items)} videos, {total_api_calls} API calls")
    print(f"   예상 API units 사용: ~{total_api_calls * 100} (일일 할당량의 {total_api_calls}%)")

    return all_items


def save_to_landing(items: List[Dict], query: str) -> Path:
    """수집한 데이터를 landing 디렉토리에 저장"""
    s = load_settings()
    ensure_dirs(s)

    landing_dir = Path(s.landing_dir)
    timestamp = int(time.time())
    filename = landing_dir / f"events_{query.replace(' ', '_')}_{timestamp}.json"

    # NDJSON 형식으로 저장
    with filename.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"\n💾 저장 완료: {filename}")
    print(f"   파일 크기: {filename.stat().st_size / 1024:.1f} KB")

    return filename


def run_pipeline():
    """Bronze → Silver → Gold 파이프라인 실행"""
    import subprocess

    print("\n" + "="*60)
    print("🔄 파이프라인 실행 시작")
    print("="*60)

    steps = [
        ("Bronze", ["python", "-m", "video_social_rtp.cli", "bronze"]),
        ("Silver", ["python", "-m", "video_social_rtp.cli", "silver", "--once"]),
        ("Gold", ["python", "-m", "video_social_rtp.cli", "gold", "--top-pct", "0.9"])
    ]

    for step_name, cmd in steps:
        print(f"\n▶️  {step_name} 단계 실행 중...")
        try:
            result = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode == 0:
                print(f"   ✅ {step_name} 완료")
            else:
                print(f"   ⚠️  {step_name} 실패 (code {result.returncode})")
                if result.stderr:
                    print(f"   Error: {result.stderr[:200]}")
        except Exception as e:
            print(f"   ❌ {step_name} 오류: {e}")

    print("\n" + "="*60)
    print("✅ 파이프라인 완료!")
    print("="*60)


def main():
    print("\n" + "="*60)
    print("📊 K-POP 7일치 데이터 수집 스크립트")
    print("="*60)

    # 수집할 쿼리 목록
    queries = [
        "NewJeans",
        "BTS official",
        "BLACKPINK"
    ]

    all_collected_items = []

    # 각 쿼리별로 7일치 데이터 수집
    for query in queries:
        items = collect_date_range_data(
            query=query,
            days_back=7,
            region_code="KR",
            relevance_language="ko",
            max_results_per_day=50
        )

        if items:
            save_to_landing(items, query)
            all_collected_items.extend(items)

        # 쿼리 간 대기 (API rate limit)
        if query != queries[-1]:
            print("\n⏳ 다음 쿼리까지 2초 대기...")
            time.sleep(2)

    # 전체 수집 요약
    print("\n" + "="*60)
    print("📈 수집 요약")
    print("="*60)
    print(f"총 쿼리 수: {len(queries)}")
    print(f"총 영상 수: {len(all_collected_items)}")
    print(f"예상 API units: ~{len(queries) * 7 * 100}")

    # 날짜별 분포 확인
    from collections import Counter
    date_dist = Counter(item.get("collected_date", "unknown") for item in all_collected_items)
    print(f"\n날짜별 분포:")
    for date_str in sorted(date_dist.keys()):
        print(f"  {date_str}: {date_dist[date_str]:3d} videos")

    # 파이프라인 실행 여부 확인
    print("\n" + "="*60)
    response = input("\n파이프라인을 실행하시겠습니까? (Bronze→Silver→Gold) [Y/n]: ")

    if response.lower() in ['y', 'yes', '']:
        run_pipeline()

        print("\n" + "="*60)
        print("🎉 모든 작업 완료!")
        print("="*60)
        print("\n다음 단계:")
        print("  1. 분석 스크립트 실행: python examples/quick_analysis.py")
        print("  2. Streamlit UI 실행: python -m video_social_rtp.cli ui --port 8501")
        print("  3. 모델 학습: python -m video_social_rtp.cli train")
    else:
        print("\n파이프라인 실행을 건너뛰었습니다.")
        print("수동 실행: python -m video_social_rtp.cli bronze")


if __name__ == "__main__":
    main()
