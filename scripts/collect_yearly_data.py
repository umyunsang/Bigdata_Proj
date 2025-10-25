#!/usr/bin/env python3
"""
1년치 K-POP 데이터를 4개 분기로 나눠서 수집

분기 구성:
- Q1 (상반기 초): 2024-01-01 ~ 2024-03-31 (90일)
- Q2 (상반기 후): 2024-04-01 ~ 2024-06-30 (91일)
- Q3 (하반기 초): 2024-07-01 ~ 2024-09-30 (92일)
- Q4 (하반기 후): 2024-10-01 ~ 2024-10-24 (24일, 현재까지)

API 할당량 예상:
- 각 분기: ~90일 × 3개 쿼리 × 100 units = 27,000 units/quarter
- 총: ~108,000 units (4분기 합계)
- 일일 할당량 10,000 → 하루에 1개 분기씩 실행 권장

실행:
    # 전체 실행 (API 할당량 주의!)
    python scripts/collect_yearly_data.py --all

    # 개별 분기 실행 (권장)
    python scripts/collect_yearly_data.py --quarter Q1
    python scripts/collect_yearly_data.py --quarter Q2
    python scripts/collect_yearly_data.py --quarter Q3
    python scripts/collect_yearly_data.py --quarter Q4
"""

import sys
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from video_social_rtp.core.config import load_settings, ensure_dirs
from video_social_rtp.core.logging import setup_logging


# 분기 정의 (2024년 기준)
QUARTERS = {
    "Q1": {
        "name": "상반기 초 (Q1 2024)",
        "start": "2024-01-01",
        "end": "2024-03-31",
        "days": 91
    },
    "Q2": {
        "name": "상반기 후 (Q2 2024)",
        "start": "2024-04-01",
        "end": "2024-06-30",
        "days": 91
    },
    "Q3": {
        "name": "하반기 초 (Q3 2024)",
        "start": "2024-07-01",
        "end": "2024-09-30",
        "days": 92
    },
    "Q4": {
        "name": "하반기 후 (Q4 2024)",
        "start": "2024-10-01",
        "end": "2024-10-24",
        "days": 24
    }
}


def collect_quarter_data(
    query: str,
    start_date: str,
    end_date: str,
    quarter_name: str,
    region_code: str = "KR",
    relevance_language: str = "ko",
    max_results_per_request: int = 50
) -> List[Dict]:
    """
    특정 분기의 데이터 수집

    Args:
        query: 검색 쿼리
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        quarter_name: 분기 이름
        region_code: 지역 코드
        relevance_language: 언어 코드
        max_results_per_request: 요청당 최대 결과 수

    Returns:
        수집된 이벤트 리스트
    """
    s = load_settings()
    log = setup_logging(f"collect_{quarter_name.lower()}")

    api_key = s.yt_api_key
    if not api_key:
        log.error("YT_API_KEY not found")
        print("❌ YouTube API 키가 설정되지 않았습니다!")
        sys.exit(1)

    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ImportError:
        log.error("google-api-python-client not installed")
        print("❌ google-api-python-client 필요!")
        sys.exit(1)

    yt = build("youtube", "v3", developerKey=api_key)

    # 날짜 파싱
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # RFC 3339 형식으로 변환
    published_after = start_dt.strftime("%Y-%m-%dT00:00:00Z")
    published_before = (end_dt + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    all_items = []
    page_token = None
    api_calls = 0
    max_api_calls = 100  # 안전 제한 (5,000 units)

    print(f"\n📅 {quarter_name}: {query}")
    print(f"   기간: {start_date} ~ {end_date}")
    print("   " + "="*56)

    try:
        while api_calls < max_api_calls:
            req = yt.search().list(
                q=query,
                part="snippet",
                type="video",
                maxResults=max_results_per_request,
                regionCode=region_code,
                relevanceLanguage=relevance_language,
                publishedAfter=published_after,
                publishedBefore=published_before,
                order="date",
                pageToken=page_token
            )

            res = req.execute()
            api_calls += 1

            items = res.get("items", [])
            if not items:
                break

            for i, item in enumerate(items):
                id_obj = item.get("id", {}) or {}
                vid = id_obj.get("videoId")
                snippet = item.get("snippet", {}) or {}

                if not vid:
                    continue

                # 실제 publish 시간을 타임스탬프로 변환 (수정됨!)
                published_at = snippet.get("publishedAt", "")
                try:
                    # ISO 8601 format: 2024-01-15T12:34:56Z
                    pub_dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    ts = int(pub_dt.timestamp() * 1000)
                    date_str = pub_dt.strftime("%Y-%m-%d")
                except Exception as e:
                    # Fallback
                    ts = int(start_dt.timestamp() * 1000)
                    date_str = start_date

                all_items.append({
                    "post_id": f"search{quarter_name.lower()}_{date_str}_{len(all_items)}_{vid}",
                    "text": snippet.get("title", ""),
                    "lang": relevance_language or "en",
                    "ts": ts,
                    "author_id": snippet.get("channelId", ""),
                    "video_id": vid,
                    "source": "yt",
                    "collected_quarter": quarter_name,
                    "collected_date": date_str
                })

            # 진행 상황 출력
            print(f"   페이지 {api_calls}: {len(all_items):4d} videos | "
                  f"API calls: {api_calls:3d}")

            # 다음 페이지 토큰 확인
            page_token = res.get("nextPageToken")
            if not page_token:
                break

            # API rate limit 회피
            time.sleep(0.5)

    except HttpError as e:
        log.error(f"API error: {e}")
        print(f"   ⚠️  API 오류: {e}")
    except Exception as e:
        log.error(f"Error: {e}")
        print(f"   ⚠️  오류: {e}")

    print("   " + "="*56)
    print(f"   ✅ {quarter_name} 수집 완료: {len(all_items)} videos, {api_calls} API calls")
    print(f"   예상 API units: ~{api_calls * 100}")

    return all_items


def save_quarter_to_landing(items: List[Dict], query: str, quarter: str) -> Path:
    """분기별 데이터를 landing 디렉토리에 저장"""
    s = load_settings()
    ensure_dirs(s)

    landing_dir = Path(s.landing_dir)
    timestamp = int(time.time())
    filename = landing_dir / f"events_{query.replace(' ', '_')}_{quarter}_{timestamp}.json"

    # NDJSON 형식으로 저장
    with filename.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"\n💾 저장: {filename.name}")
    print(f"   크기: {filename.stat().st_size / 1024:.1f} KB")

    return filename


def collect_all_quarters(queries: List[str], selected_quarter: str = None):
    """모든 분기 또는 선택된 분기의 데이터 수집"""

    quarters_to_collect = [selected_quarter] if selected_quarter else list(QUARTERS.keys())

    total_items = 0
    total_api_calls = 0

    for quarter in quarters_to_collect:
        q_info = QUARTERS[quarter]

        print("\n" + "="*60)
        print(f"🗓️  {q_info['name']}")
        print(f"   기간: {q_info['start']} ~ {q_info['end']} ({q_info['days']}일)")
        print("="*60)

        quarter_items = []

        for query in queries:
            items = collect_quarter_data(
                query=query,
                start_date=q_info["start"],
                end_date=q_info["end"],
                quarter_name=quarter,
                region_code="KR",
                relevance_language="ko",
                max_results_per_request=50
            )

            if items:
                save_quarter_to_landing(items, query, quarter)
                quarter_items.extend(items)

            # 쿼리 간 대기
            if query != queries[-1]:
                time.sleep(2)

        total_items += len(quarter_items)

        # 분기 요약
        print(f"\n📊 {quarter} 요약:")
        print(f"   총 영상 수: {len(quarter_items)}")

        # 날짜별 분포
        from collections import Counter
        date_dist = Counter(item.get("collected_date", "unknown") for item in quarter_items)
        print(f"   날짜 범위: {min(date_dist.keys())} ~ {max(date_dist.keys())}")
        print(f"   고유 날짜 수: {len(date_dist)}")

        # 분기 간 대기 (API quota 회복)
        if quarter != quarters_to_collect[-1]:
            print("\n⏳ 다음 분기까지 5초 대기...")
            time.sleep(5)

    return total_items


def main():
    parser = argparse.ArgumentParser(description="1년치 K-POP 데이터 수집 (분기별)")
    parser.add_argument(
        "--quarter",
        choices=["Q1", "Q2", "Q3", "Q4"],
        help="수집할 분기 (미지정 시 사용자 선택)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="모든 분기 한 번에 수집 (API 할당량 주의!)"
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        default=["NewJeans", "BTS official", "BLACKPINK"],
        help="검색 쿼리 목록"
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print("📊 K-POP 1년치 데이터 수집 (2024년)")
    print("="*60)

    # API 할당량 경고
    if args.all:
        print("\n⚠️  경고: 모든 분기를 한 번에 수집하면")
        print("   예상 API units: ~108,000 (일일 할당량 10,000)")
        print("   → 약 11일에 걸쳐 수집됩니다!")
        print("\n💡 권장: 하루에 1개 분기씩 수집")
        response = input("\n계속 진행하시겠습니까? [y/N]: ")
        if response.lower() != 'y':
            print("취소되었습니다.")
            return

    # 분기 선택
    selected_quarter = None
    if args.all:
        selected_quarter = None  # 모든 분기
    elif args.quarter:
        selected_quarter = args.quarter
    else:
        # 대화형 선택
        print("\n수집할 분기를 선택하세요:")
        for q, info in QUARTERS.items():
            print(f"  {q}: {info['name']} ({info['days']}일)")

        choice = input("\n선택 (Q1/Q2/Q3/Q4 또는 ALL): ").upper().strip()
        if choice == "ALL":
            selected_quarter = None
        elif choice in QUARTERS:
            selected_quarter = choice
        else:
            print("❌ 잘못된 선택입니다.")
            return

    # 데이터 수집 실행
    total_items = collect_all_quarters(args.queries, selected_quarter)

    # 최종 요약
    print("\n" + "="*60)
    print("🎉 수집 완료!")
    print("="*60)
    print(f"총 영상 수: {total_items}")
    print(f"검색 쿼리: {', '.join(args.queries)}")

    # 다음 단계 안내
    print("\n💡 다음 단계:")
    print("  1. 기존 데이터 삭제:")
    print("     rm -rf project/data/bronze/*")
    print("     rm -rf project/data/silver/*")
    print("     rm -rf project/data/gold/*")
    print("     rm -rf project/chk/silver/*")
    print("")
    print("  2. 파이프라인 실행:")
    print("     python -m video_social_rtp.cli bronze")
    print("     python -m video_social_rtp.cli silver --once")
    print("     python -m video_social_rtp.cli gold")
    print("")
    print("  3. 분석 실행:")
    print("     python examples/quick_analysis.py")
    print("")
    print("  4. 성장률 확인:")
    print("     - growth_rate_7d 값이 0이 아닌지 확인")
    print("     - trend_direction이 RISING/FALLING으로 변경되었는지 확인")


if __name__ == "__main__":
    main()
