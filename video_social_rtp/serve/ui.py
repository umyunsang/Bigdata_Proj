from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import altair as alt

from video_social_rtp.core.config import load_settings
from video_social_rtp.core.bloom import BloomFilter


def _load_gold_features_csv() -> Optional[pd.DataFrame]:
    """Load Gold features (quarterly data with tier assignment)"""
    s = load_settings()
    csv = Path(s.gold_dir) / "features.csv"
    if csv.exists():
        try:
            return pd.read_csv(csv)
        except Exception:
            return None
    return None


def _load_quarterly_silver_csv() -> Optional[pd.DataFrame]:
    """Load Silver quarterly aggregation"""
    s = load_settings()
    sm_dir = Path(s.silver_dir) / "social_metrics"
    target = sm_dir / "quarterly_metrics.csv"
    if target.exists():
        try:
            return pd.read_csv(target)
        except Exception:
            return None
    return None


def _load_bloom() -> tuple[Optional[BloomFilter], Optional[dict]]:
    """Load Bloom filter for deduplication stats"""
    s = load_settings()
    bin_path = Path(s.artifact_dir) / "bronze_bloom.bin"
    meta_path = Path(s.artifact_dir) / "bronze_bloom.json"
    if bin_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            bloom = BloomFilter.from_bytes(
                size_bits=int(meta["size_bits"]),
                num_hashes=int(meta["num_hashes"]),
                payload=bin_path.read_bytes(),
                count=int(meta.get("count", 0)),
            )
            return bloom, meta
        except Exception:
            return None, None
    return None, None


def main() -> None:
    st.set_page_config(
        page_title="K-POP 아티스트 분기별 분석",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("🎵 K-POP 아티스트 분기별 인기도 분석")
    st.caption("2024년 분기별 데이터 기반 아티스트 트렌드 분석")

    # Load data
    feats = _load_gold_features_csv()
    quarterly = _load_quarterly_silver_csv()
    bloom, bloom_meta = _load_bloom()

    # Sidebar
    with st.sidebar:
        st.header("📊 데이터 상태")

        # Status cards
        st.markdown("### 시스템 상태")

        if feats is not None and not feats.empty:
            st.success(f"✅ Gold: {len(feats)} 행")
        else:
            st.error("❌ Gold 데이터 없음")

        if quarterly is not None and not quarterly.empty:
            st.success(f"✅ Silver: {len(quarterly)} 분기 집계")
        else:
            st.error("❌ Silver 데이터 없음")

        if bloom is not None:
            st.success(f"✅ Bloom Filter: {bloom_meta.get('count', 0):,} items")
        else:
            st.warning("⚠️ Bloom Filter 없음")

        st.divider()

        # Refresh button
        if st.button("🔄 데이터 새로고침", use_container_width=True):
            st.rerun()

        st.divider()

        # Quarter selection
        st.markdown("### 📅 분석 설정")
        if quarterly is not None and not quarterly.empty:
            available_quarters = sorted(quarterly['quarter'].unique())
            selected_quarter = st.selectbox(
                "분기 선택",
                options=available_quarters,
                index=len(available_quarters) - 1  # Default to latest quarter
            )
        else:
            selected_quarter = None

        # Artist selection
        if feats is not None and not feats.empty:
            available_artists = sorted(feats['artist'].unique())
            selected_artist = st.selectbox(
                "아티스트 선택",
                options=["전체"] + available_artists,
                index=0
            )
        else:
            selected_artist = "전체"

        st.divider()

        # User input for prediction
        st.markdown("### 🔍 실시간 분석")
        user_input = st.text_input(
            "키워드 또는 영상ID",
            placeholder="예: v123456 또는 BLACKPINK",
            help="Bloom Filter 체크 및 예측을 위한 입력"
        )

        analyze_button = st.button("📊 데이터 조회", use_container_width=True)

        # Bloom Filter check
        bloom_result = None
        if analyze_button and user_input and bloom is not None:
            # Check if input looks like video_id (starts with 'v' or is alphanumeric)
            check_key = f"search_{user_input}" if not user_input.startswith('v') else user_input
            is_present = bloom.contains(check_key)

            # Calculate probability based on Bloom filter FPR
            fpr = bloom_meta.get('fpr', 0.01) if bloom_meta else 0.01
            if is_present:
                # True positive probability = 1 - FPR
                probability_pct = (1 - fpr) * 100
                probability_str = f"{probability_pct:.1f}%"
            else:
                # True negative is very reliable
                probability_pct = fpr * 100
                probability_str = f"< {probability_pct:.2f}%"

            bloom_result = {
                "input": user_input,
                "present": is_present,
                "probability": probability_str,
                "probability_value": probability_pct
            }

    # Main content
    if feats is None or feats.empty:
        st.error("⚠️ Gold 테이블이 비어있습니다. 03 Gold 단계를 먼저 실행하세요.")
        return

    if quarterly is None or quarterly.empty:
        st.error("⚠️ Silver 분기별 데이터가 없습니다. 02 Silver 단계를 먼저 실행하세요.")
        return

    # Filter Gold data by selected quarter and artist
    if selected_quarter:
        feats_quarter = feats[feats['quarter'] == selected_quarter].copy()
    else:
        feats_quarter = feats.copy()

    if selected_artist != "전체":
        feats_quarter = feats_quarter[feats_quarter['artist'] == selected_artist].copy()

    # Bloom Filter Result Section
    if bloom_result is not None:
        st.markdown("## 🔍 Bloom Filter 분석 결과")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("입력값", bloom_result["input"])

        with col2:
            status = "✅ 존재함" if bloom_result["present"] else "❌ 없음"
            st.metric("데이터 존재 여부", status)

        with col3:
            st.metric("신뢰도", bloom_result["probability"])

        with col4:
            prob_val = bloom_result.get("probability_value", 0) / 100
            st.metric("확률 (수치)", f"{prob_val:.1%}")

        # Visual probability bar
        if bloom_result["present"]:
            st.success(f"💡 **{bloom_result['input']}**는 최근 데이터에 포함되어 있을 확률 **{bloom_result['probability']}**입니다.")
            st.progress(bloom_result.get("probability_value", 99) / 100, text="존재 확률")
        else:
            st.info(f"ℹ️ **{bloom_result['input']}**는 최근 데이터에 없을 가능성이 높습니다.")
            st.progress(bloom_result.get("probability_value", 1) / 100, text="False Positive 확률")

        st.divider()

    # Overview metrics (for selected quarter)
    filter_text = f"{selected_quarter if selected_quarter else '전체'}"
    if selected_artist != "전체":
        filter_text += f" - {selected_artist}"
    st.markdown(f"## 📈 분기별 개요 ({filter_text})")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_engagement = feats_quarter['total_engagement'].sum()
        st.metric("총 Engagement", f"{total_engagement:,.0f}")

    with col2:
        total_videos = feats_quarter['total_videos'].sum()
        st.metric("총 영상 수", f"{total_videos:,.0f}")

    with col3:
        num_artists = len(feats_quarter['artist'].unique())
        st.metric("아티스트 수", f"{num_artists}")

    with col4:
        avg_growth = feats_quarter['growth_qoq'].mean()
        st.metric("평균 QoQ 성장률", f"{avg_growth:.1f}%")

    st.divider()

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 전체 분포",
        "🏆 Top-K 순위",
        "📈 성장률 분석",
        "📋 상세 통계",
        "🔮 실시간 예측 & CDF"
    ])

    with tab1:
        st.subheader(f"분기별 시장 점유율 분포 ({selected_quarter})")

        if not feats_quarter.empty:
            # Market share pie chart
            top10_market = feats_quarter.nlargest(10, 'total_engagement')[['artist', 'market_share', 'tier']]
            pie_chart = alt.Chart(top10_market).mark_arc().encode(
                theta=alt.Theta('market_share:Q', title='시장 점유율 (%)'),
                color=alt.Color('artist:N', legend=alt.Legend(title='아티스트')),
                tooltip=['artist:N', alt.Tooltip('market_share:Q', format='.2f', title='점유율 (%)'), 'tier:N']
            ).properties(height=400)
            st.altair_chart(pie_chart, use_container_width=True)

            # Tier distribution
            st.markdown("#### Tier별 분포")
            tier_dist = feats_quarter['tier'].value_counts().sort_index().reset_index()
            tier_dist.columns = ['tier', 'count']

            tier_chart = alt.Chart(tier_dist).mark_bar().encode(
                x=alt.X('tier:O', title='Tier'),
                y=alt.Y('count:Q', title='아티스트 수'),
                color=alt.Color('tier:N', scale=alt.Scale(scheme='category10')),
                tooltip=['tier:O', 'count:Q']
            ).properties(height=300)
            st.altair_chart(tier_chart, use_container_width=True)

    with tab2:
        st.subheader(f"Top-10 아티스트 순위 ({selected_quarter})")

        if not feats_quarter.empty:
            top10 = feats_quarter.nlargest(10, 'total_engagement')

            # Display table
            display_df = top10[[
                "artist", "total_engagement", "total_videos", "unique_authors",
                "tier", "growth_qoq", "market_share", "trend_direction"
            ]].rename(columns={
                "artist": "아티스트",
                "total_engagement": "총 Engagement",
                "total_videos": "영상 수",
                "unique_authors": "채널 수",
                "tier": "Tier",
                "growth_qoq": "QoQ 성장률(%)",
                "market_share": "시장 점유율(%)",
                "trend_direction": "트렌드"
            })

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )

            # Bar chart
            st.markdown("#### Engagement 순위 차트")
            rank_chart = alt.Chart(top10).mark_bar().encode(
                x=alt.X('artist:N', sort='-y', title='아티스트'),
                y=alt.Y('total_engagement:Q', title='총 Engagement'),
                color=alt.Color('tier:N', legend=alt.Legend(title='Tier'), scale=alt.Scale(scheme='category10')),
                tooltip=['artist:N', 'total_engagement:Q', 'tier:N', 'growth_qoq:Q']
            ).properties(height=400)
            st.altair_chart(rank_chart, use_container_width=True)

    with tab3:
        st.subheader("분기별 성장률 분석")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(f"#### QoQ 성장률 Top 10 ({selected_quarter})")

            if not feats_quarter.empty:
                top_growth = feats_quarter.nlargest(10, 'growth_qoq')[['artist', 'growth_qoq', 'tier']]

                if not top_growth.empty:
                    growth_chart = alt.Chart(top_growth).mark_bar().encode(
                        x=alt.X('artist:N', sort='-y', title='아티스트'),
                        y=alt.Y('growth_qoq:Q', title='QoQ 성장률 (%)'),
                        color=alt.condition(
                            alt.datum.growth_qoq > 0,
                            alt.value('steelblue'),
                            alt.value('coral')
                        ),
                        tooltip=['artist:N', alt.Tooltip('growth_qoq:Q', format='.1f', title='성장률 (%)'), 'tier:N']
                    ).properties(height=400)
                    st.altair_chart(growth_chart, use_container_width=True)
                else:
                    st.info("성장률 데이터가 없습니다")

        with col2:
            st.markdown("#### 주요 아티스트 분기별 추이")

            if not quarterly.empty:
                # Get top 5 artists by total engagement across all quarters
                top5_artists = (
                    quarterly.groupby('artist')['total_engagement']
                    .sum()
                    .nlargest(5)
                    .index.tolist()
                )

                top5_data = quarterly[quarterly['artist'].isin(top5_artists)].copy()

                if not top5_data.empty:
                    trend_chart = alt.Chart(top5_data).mark_line(point=True).encode(
                        x=alt.X('quarter:N', title='분기', sort=None),
                        y=alt.Y('total_engagement:Q', title='총 Engagement'),
                        color=alt.Color('artist:N', legend=alt.Legend(title='아티스트')),
                        tooltip=['artist:N', 'quarter:N', 'total_engagement:Q', alt.Tooltip('growth_qoq:Q', format='.1f', title='QoQ 성장률(%)')]
                    ).properties(height=400)
                    st.altair_chart(trend_chart, use_container_width=True)

        # Full quarterly comparison
        st.markdown("#### 전체 분기별 비교 (Top 10 아티스트)")

        if not quarterly.empty:
            # Get overall top 10 artists
            top10_overall = (
                quarterly.groupby('artist')['total_engagement']
                .sum()
                .nlargest(10)
                .index.tolist()
            )

            top10_quarterly = quarterly[quarterly['artist'].isin(top10_overall)].copy()

            comparison_chart = alt.Chart(top10_quarterly).mark_bar().encode(
                x=alt.X('quarter:N', title='분기', sort=None),
                y=alt.Y('total_engagement:Q', title='총 Engagement'),
                color=alt.Color('artist:N', legend=alt.Legend(title='아티스트')),
                xOffset='artist:N',
                tooltip=['artist:N', 'quarter:N', 'total_engagement:Q', alt.Tooltip('growth_qoq:Q', format='.1f')]
            ).properties(height=400)
            st.altair_chart(comparison_chart, use_container_width=True)

    with tab4:
        st.subheader("상세 통계")

        # Tier filter
        tier_filter = st.multiselect(
            "Tier 필터",
            options=sorted(feats['tier'].unique()),
            default=sorted(feats['tier'].unique())
        )

        # Quarter filter
        quarter_filter = st.multiselect(
            "분기 필터",
            options=sorted(feats['quarter'].unique()),
            default=[selected_quarter] if selected_quarter else sorted(feats['quarter'].unique())
        )

        filtered_df = feats[
            (feats['tier'].isin(tier_filter)) &
            (feats['quarter'].isin(quarter_filter))
        ]

        st.dataframe(
            filtered_df[[
                'quarter', 'artist', 'total_engagement', 'total_videos', 'unique_authors',
                'tier', 'growth_qoq', 'market_share', 'percentile', 'trend_direction'
            ]].rename(columns={
                'quarter': '분기',
                'artist': '아티스트',
                'total_engagement': '총 Engagement',
                'total_videos': '영상 수',
                'unique_authors': '채널 수',
                'tier': 'Tier',
                'growth_qoq': 'QoQ 성장률(%)',
                'market_share': '시장 점유율(%)',
                'percentile': '백분위수',
                'trend_direction': '트렌드'
            }),
            use_container_width=True,
            hide_index=True
        )

        # Summary stats
        st.markdown("#### 요약 통계")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("필터링된 행 수", f"{len(filtered_df)}")

        with col2:
            avg_engagement = filtered_df['total_engagement'].mean()
            st.metric("평균 Engagement", f"{avg_engagement:,.0f}")

        with col3:
            avg_market_share = filtered_df['market_share'].mean()
            st.metric("평균 시장 점유율", f"{avg_market_share:.2f}%")

    with tab5:
        st.subheader("🔮 실시간 예측 & CDF/PDF 분석")

        if user_input and analyze_button:
            st.markdown(f"### 분석 대상: `{user_input}`")

            # Show Bloom Filter result
            if bloom_result:
                status_color = "green" if bloom_result["present"] else "orange"
                st.markdown(f"**Bloom Filter 결과**: :{status_color}[{bloom_result['probability']} 신뢰도로 {'존재' if bloom_result['present'] else '없음'}]")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 📊 CDF/PDF 분석")

                if not feats_quarter.empty:
                    # Calculate CDF/PDF for engagement distribution
                    engagement_data = feats_quarter[['artist', 'total_engagement', 'percentile']].copy()
                    engagement_data = engagement_data.sort_values('total_engagement')

                    # Base CDF line
                    base_cdf = alt.Chart(engagement_data).mark_line(strokeWidth=2).encode(
                        x=alt.X('total_engagement:Q', title='총 Engagement'),
                        y=alt.Y('percentile:Q', title='누적 분포 (CDF)', scale=alt.Scale(domain=[0, 1])),
                        tooltip=['artist:N', 'total_engagement:Q', alt.Tooltip('percentile:Q', format='.2%', title='백분위수')]
                    )

                    # Reference lines for percentiles
                    percentile_lines = alt.Chart(pd.DataFrame({
                        'percentile': [0.95, 0.85, 0.60],
                        'label': ['상위 5% (Tier 1)', '상위 15% (Tier 2)', '상위 40% (Tier 3)']
                    })).mark_rule(strokeDash=[5, 5], opacity=0.5).encode(
                        y='percentile:Q',
                        color=alt.value('gray')
                    )

                    # Highlight selected artist
                    if selected_artist != "전체" and selected_artist in engagement_data['artist'].values:
                        artist_point_data = engagement_data[engagement_data['artist'] == selected_artist]
                        artist_point = alt.Chart(artist_point_data).mark_point(size=200, filled=True, color='red').encode(
                            x='total_engagement:Q',
                            y='percentile:Q',
                            tooltip=['artist:N', 'total_engagement:Q', alt.Tooltip('percentile:Q', format='.2%')]
                        )
                        cdf_chart = (base_cdf + percentile_lines + artist_point).properties(height=300, title="Engagement 누적 분포 함수 (CDF)")

                        user_row = artist_point_data.iloc[0]
                        percentile_value = user_row['percentile'] * 100
                        st.info(f"💡 **{selected_artist}**는 상위 **{100 - percentile_value:.1f}%** 구간에 위치합니다 (빨간 점)")
                    else:
                        cdf_chart = (base_cdf + percentile_lines).properties(height=300, title="Engagement 누적 분포 함수 (CDF)")

                    st.altair_chart(cdf_chart, use_container_width=True)

                    # PDF (histogram approximation)
                    pdf_chart = alt.Chart(engagement_data).mark_bar().encode(
                        x=alt.X('total_engagement:Q', bin=alt.Bin(maxbins=20), title='Engagement 구간'),
                        y=alt.Y('count()', title='아티스트 수 (빈도)'),
                        tooltip=['count()']
                    ).properties(height=250, title="Engagement 확률 밀도 함수 (PDF)")

                    st.altair_chart(pdf_chart, use_container_width=True)
                else:
                    st.warning("선택한 필터에 데이터가 없습니다.")

            with col2:
                st.markdown("#### 🎯 맥락 기반 예측")

                if not feats_quarter.empty:
                    # Show quarterly context
                    if selected_artist != "전체" and selected_artist in feats_quarter['artist'].values:
                        artist_data = feats_quarter[feats_quarter['artist'] == selected_artist].iloc[0]

                        st.markdown(f"##### {selected_artist} - {selected_quarter}")

                        # Metrics
                        met_col1, met_col2 = st.columns(2)
                        with met_col1:
                            st.metric("총 Engagement", f"{artist_data['total_engagement']:,.0f}")
                            st.metric("Tier", f"{artist_data['tier']}")
                            st.metric("시장 점유율", f"{artist_data['market_share']:.2f}%")

                        with met_col2:
                            st.metric("QoQ 성장률", f"{artist_data['growth_qoq']:.1f}%")
                            st.metric("트렌드", artist_data['trend_direction'])
                            st.metric("백분위수", f"{artist_data['percentile']*100:.1f}%")

                        # Prediction context
                        st.markdown("---")
                        st.markdown("##### 📈 예측 맥락")

                        if artist_data['trend_direction'] == 'UP':
                            st.success(f"✅ {selected_artist}는 상승 트렌드입니다. (QoQ +{artist_data['growth_qoq']:.1f}%)")
                        elif artist_data['trend_direction'] == 'DOWN':
                            st.warning(f"⚠️ {selected_artist}는 하락 트렌드입니다. (QoQ {artist_data['growth_qoq']:.1f}%)")
                        else:
                            st.info(f"ℹ️ {selected_artist}는 안정적인 트렌드입니다.")

                        # Show historical trend across quarters
                        artist_history = feats[feats['artist'] == selected_artist].sort_values('quarter')
                        if len(artist_history) > 1:
                            st.markdown("##### 📊 분기별 추이")

                            trend_line = alt.Chart(artist_history).mark_line(point=True).encode(
                                x=alt.X('quarter:N', title='분기', sort=None),
                                y=alt.Y('total_engagement:Q', title='총 Engagement'),
                                tooltip=['quarter:N', 'total_engagement:Q', alt.Tooltip('growth_qoq:Q', format='.1f', title='QoQ (%)')]
                            ).properties(height=200)

                            st.altair_chart(trend_line, use_container_width=True)
                    else:
                        st.info("아티스트를 선택하여 상세 분석을 확인하세요.")

                    # General prediction insights
                    st.markdown("---")
                    st.markdown("##### 🔮 분기 전망")

                    top_growth = feats_quarter.nlargest(3, 'growth_qoq')
                    st.markdown("**최근 고성장 아티스트 (Top 3)**")
                    for idx, row in top_growth.iterrows():
                        st.write(f"- **{row['artist']}**: +{row['growth_qoq']:.1f}% (Tier {row['tier']})")

                else:
                    st.warning("선택한 필터에 데이터가 없습니다.")

        else:
            st.info("👈 사이드바에서 '키워드 또는 영상ID'를 입력하고 '데이터 조회' 버튼을 클릭하세요.")


if __name__ == "__main__":
    main()
