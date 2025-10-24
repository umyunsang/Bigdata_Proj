#!/usr/bin/env python3
"""
즉시 실행 가능한 Gold 데이터 분석 예제

데이터 요구사항:
- 최소 1일 데이터: Tier 분류, engagement 비교, 시장 점유율
- 7일 이상 데이터: growth_rate_7d, momentum 분석
- 30일 이상 데이터: growth_rate_30d, volatility 분석

실행:
    python examples/quick_analysis.py
"""

from pathlib import Path
import pandas as pd
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from video_social_rtp.core.config import load_settings


def load_gold_data() -> pd.DataFrame:
    """Load gold features CSV"""
    s = load_settings()
    csv_path = Path(s.gold_dir) / "features.csv"

    if not csv_path.exists():
        print(f"❌ Gold features not found: {csv_path}")
        print("\n💡 Run the pipeline first:")
        print("   python -m video_social_rtp.cli fetch")
        print("   python -m video_social_rtp.cli bronze")
        print("   python -m video_social_rtp.cli silver --once")
        print("   python -m video_social_rtp.cli gold")
        sys.exit(1)

    return pd.read_csv(csv_path)


def analyze_tier_distribution(df: pd.DataFrame):
    """✅ 즉시 가능: Tier 분포 분석"""
    print("\n" + "="*60)
    print("📊 1. Tier 분포 (즉시 가능)")
    print("="*60)

    tier_dist = df['tier'].value_counts().sort_index()
    print(tier_dist)

    print("\nTier 의미:")
    print("  Tier 1: 초대형 바이럴 (상위 10%)")
    print("  Tier 2: 대형 인기 (상위 10~25%)")
    print("  Tier 3: 중견 (상위 25~60%)")
    print("  Tier 4: 신인/소규모 (하위 40%)")


def analyze_top_artists(df: pd.DataFrame):
    """✅ 즉시 가능: Top 아티스트 분석"""
    print("\n" + "="*60)
    print("🏆 2. Top 5 아티스트 (즉시 가능)")
    print("="*60)

    top5 = df.nlargest(5, 'total_engagement')[
        ['artist', 'total_engagement', 'total_videos', 'tier', 'trend_direction']
    ]
    print(top5.to_string(index=False))


def analyze_market_share(df: pd.DataFrame):
    """✅ 즉시 가능: 시장 점유율 분석"""
    print("\n" + "="*60)
    print("💹 3. 시장 점유율 (즉시 가능)")
    print("="*60)

    market = df[['artist', 'market_share', 'total_engagement']].sort_values(
        'market_share', ascending=False
    )
    print(market.to_string(index=False))


def analyze_tier1_characteristics(df: pd.DataFrame):
    """✅ 즉시 가능: Tier 1 아티스트 특성"""
    print("\n" + "="*60)
    print("⭐ 4. Tier 1 아티스트 평균 특성 (즉시 가능)")
    print("="*60)

    tier1 = df[df['tier'] == 1]

    if len(tier1) == 0:
        print("⚠️ Tier 1 아티스트가 없습니다. 더 많은 데이터를 수집하세요.")
        return

    stats = tier1.agg({
        'total_videos': 'mean',
        'avg_engagement': 'mean',
        'max_engagement': 'mean',
        'unique_viewers_est': 'mean'
    })

    print(f"평균 영상 수: {stats['total_videos']:.1f}")
    print(f"평균 Engagement: {stats['avg_engagement']:.1f}")
    print(f"최대 Engagement: {stats['max_engagement']:.1f}")
    print(f"예상 시청자 수: {stats['unique_viewers_est']:.1f}")

    print("\n💡 인사이트:")
    print(f"   Tier 1 달성 조건: 영상 {stats['total_videos']:.0f}개 이상, "
          f"평균 engagement {stats['avg_engagement']:.0f} 이상")


def analyze_cdf_ranking(df: pd.DataFrame):
    """✅ 즉시 가능: CDF 백분위 순위"""
    print("\n" + "="*60)
    print("📈 5. CDF 백분위 순위 (즉시 가능)")
    print("="*60)

    ranking = df.sort_values('percentile', ascending=False)[
        ['artist', 'percentile', 'tier', 'total_engagement']
    ]
    print(ranking.to_string(index=False))

    print("\n💡 백분위 해석:")
    print("   1.0 = 상위 0% (최고)")
    print("   0.5 = 상위 50% (중간)")
    print("   0.0 = 하위 0% (최하)")


def analyze_growth_rates(df: pd.DataFrame):
    """⚠️ 7일 데이터 필요: 성장률 분석"""
    print("\n" + "="*60)
    print("📊 6. 성장률 분석 (7일 데이터 필요)")
    print("="*60)

    growth = df[['artist', 'growth_rate_7d', 'growth_rate_30d', 'momentum']].copy()
    print(growth.to_string(index=False))

    # Check if data is meaningful
    if growth['growth_rate_7d'].abs().max() < 0.01:
        print("\n⚠️ 성장률이 모두 0입니다!")
        print("   → 최소 7일 이상 데이터를 수집해야 의미 있는 분석이 가능합니다.")
        print("\n📅 데이터 수집 계획:")
        print("   1. 매일 2회 파이프라인 실행 (오전 9시, 오후 3시)")
        print("   2. 7일 후: growth_rate_7d, momentum 활성화")
        print("   3. 30일 후: growth_rate_30d, volatility 활성화")
    else:
        print("\n✅ 성장률 데이터 정상!")

        # Find trending artists
        trending = growth[growth['growth_rate_7d'] > 10].sort_values(
            'growth_rate_7d', ascending=False
        )
        if not trending.empty:
            print("\n🔥 급성장 아티스트 (7d growth > 10%):")
            print(trending.to_string(index=False))


def analyze_volatility(df: pd.DataFrame):
    """⚠️ 30일 데이터 필요: 변동성 분석"""
    print("\n" + "="*60)
    print("📉 7. 변동성 분석 (30일 데이터 필요)")
    print("="*60)

    volatility = df[['artist', 'volatility', 'trend_direction']].sort_values(
        'volatility', ascending=False
    )
    print(volatility.to_string(index=False))

    if df['volatility'].max() < 1.0:
        print("\n⚠️ 변동성 데이터가 충분하지 않습니다.")
        print("   → 30일 이상 데이터를 수집하면 더 정확한 분석이 가능합니다.")


def benchmark_comparison(df: pd.DataFrame):
    """✅ 즉시 가능: 아티스트 벤치마킹"""
    print("\n" + "="*60)
    print("🔍 8. 아티스트 벤치마킹 (즉시 가능)")
    print("="*60)

    # Find top 2 artists for comparison
    top2 = df.nlargest(2, 'total_engagement')['artist'].tolist()

    if len(top2) < 2:
        print("⚠️ 비교할 아티스트가 부족합니다.")
        return

    comparison = df[df['artist'].isin(top2)][
        ['artist', 'total_engagement', 'tier', 'total_videos',
         'growth_rate_7d', 'trend_direction']
    ]

    print(f"\n{top2[0]} vs {top2[1]} 비교:")
    print(comparison.to_string(index=False))

    # Interpretation
    winner = comparison.loc[comparison['total_engagement'].idxmax(), 'artist']
    print(f"\n💡 현재 선두: {winner}")

    if comparison['growth_rate_7d'].abs().max() > 0.01:
        growth_winner = comparison.loc[comparison['growth_rate_7d'].idxmax(), 'artist']
        print(f"📈 성장률 1위: {growth_winner}")
    else:
        print("📈 성장률: 7일 데이터 필요")


def main():
    print("🎵 K-POP 아티스트 인기도 분석")
    print("="*60)

    # Load data
    df = load_gold_data()
    print(f"✅ 데이터 로드 성공: {len(df)} 아티스트")

    # Run analyses
    analyze_tier_distribution(df)
    analyze_top_artists(df)
    analyze_market_share(df)
    analyze_tier1_characteristics(df)
    analyze_cdf_ranking(df)
    benchmark_comparison(df)
    analyze_growth_rates(df)
    analyze_volatility(df)

    # Summary
    print("\n" + "="*60)
    print("✅ 분석 완료!")
    print("="*60)
    print("\n💡 다음 단계:")
    print("1. Streamlit UI 실행: python -m video_social_rtp.cli ui --port 8501")
    print("2. 매일 데이터 수집으로 성장률 트래킹 시작")
    print("3. 7일 후: 트렌드 분석 활성화")
    print("4. 30일 후: 완전한 비즈니스 인사이트 도출")


if __name__ == "__main__":
    main()
