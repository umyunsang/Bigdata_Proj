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
    s = load_settings()
    csv = Path(s.gold_dir) / "features.csv"
    if csv.exists():
        try:
            return pd.read_csv(csv)
        except Exception:
            return None
    return None


def _load_latest_silver_csv() -> Optional[pd.DataFrame]:
    s = load_settings()
    sm_dir = Path(s.silver_dir) / "social_metrics"
    target = sm_dir / "latest_metrics.csv"
    if target.exists():
        try:
            return pd.read_csv(target)
        except Exception:
            return None
    return None


def _load_cutoff_artifact() -> Optional[float]:
    s = load_settings()
    art = Path(s.artifact_dir) / "gold_cutoff.json"
    if art.exists():
        try:
            d = json.loads(art.read_text(encoding="utf-8"))
            return float(d.get("cutoff"))
        except Exception:
            return None
    return None


def _load_bloom() -> tuple[Optional[BloomFilter], Optional[dict]]:
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
    st.set_page_config(page_title="Video/Social RTP — UI", layout="wide")
    st.title("실시간 예측/조회 (과제05)")
    st.caption("실습자료 스택(Streamlit, Pandas) 기반 UI 예시")

    # Load data
    feats = _load_gold_features_csv()
    sm = _load_latest_silver_csv()
    cutoff = _load_cutoff_artifact()
    bloom_filter, bloom_meta = _load_bloom()

    with st.sidebar:
        st.header("입력/옵션")
        vid = st.text_input("video_id", value="v0")
        topk = st.slider("Top-K (최근 윈도우)", 1, 20, 5)

    # Existence check (bloom-like)
    exists = False
    if bloom_filter is not None:
        exists = bloom_filter.might_contain(vid)
    elif sm is not None and "video_id" in sm.columns:
        exists = vid in set(sm["video_id"].astype(str))
    st.subheader("Bloom 존재 가능성(근사)")
    st.write("최근 윈도우 데이터 기준 존재 여부(집합 기반 근사)")
    st.metric("최근 존재 가능성", "있음" if exists else "없음")
    if bloom_meta:
        fp = bloom_meta.get("num_hashes", 0)
        st.caption(f"Bloom 필터: size_bits={bloom_meta.get('size_bits')}, hash_functions={fp}, items≈{bloom_meta.get('count')}")

    # CDF/PDF charts from gold features
    st.subheader("Gold 분포(CDF/PDF) 및 컷오프")
    if feats is None or feats.empty or "engagement_pdf" not in feats.columns:
        st.info("Gold features.csv를 찾지 못했거나 비어 있습니다. 03 단계를 먼저 실행하세요.")
    else:
        df = feats.copy()
        col = "total_engagement" if "total_engagement" in df.columns else "engagement_24h"
        df = df.sort_values(col)
        pdf_col = "engagement_pdf" if "engagement_pdf" in df.columns else None
        cdf_col = "engagement_cdf" if "engagement_cdf" in df.columns else None
        if pdf_col and cdf_col:
            st.line_chart(df[[col, cdf_col]].set_index(col), height=240)
            st.bar_chart(df[[col, pdf_col]].set_index(col).head(50), height=240)
        else:
            df["pdf_temp"] = 1 / len(df)
            df["cdf_temp"] = df["pdf_temp"].cumsum()
            st.line_chart(df[[col, "cdf_temp"]].set_index(col), height=240)
            st.bar_chart(df[[col, "pdf_temp"]].set_index(col).head(50), height=240)
        if cutoff is not None:
            st.info(f"현재 컷오프: engagement_24h ≥ {cutoff}")

        if cdf_col:
            df["hot_probability"] = 1.0 - df[cdf_col]
            hot_chart = (
                alt.Chart(df.head(20))
                .mark_bar()
                .encode(x=alt.X("artist:N", sort="-y"), y=alt.Y("hot_probability:Q", title="Hot 콘텐츠 확률"))
            )
            st.altair_chart(hot_chart, use_container_width=True)

        hist_chart = (
            alt.Chart(df)
            .mark_bar()
            .encode(
                alt.X(f"{col}:Q", bin=alt.Bin(maxbins=20), title="총 참여수"),
                alt.Y("count()", title="빈도"),
            )
        )
        st.altair_chart(hist_chart, use_container_width=True)

        # Query row
        target_col = "video_id" if "video_id" in df.columns else "artist"
        if target_col != "video_id":
            st.caption("Gold 피처는 아티스트 기준으로 집계되었습니다. 아티스트명을 입력하면 결과를 확인할 수 있습니다.")
        q = df[df[target_col].astype(str) == str(vid)]
        if not q.empty:
            engagement_display = q[col].iloc[0]
            label_col = "tier" if "tier" in q.columns else q.columns[-1]
            st.success(f"선택 {target_col}={vid}의 {col}={engagement_display}, label={q[label_col].iloc[0]}")
        else:
            st.warning("선택한 식별자가 Gold 테이블에 없습니다.")

    # Top-K 상승률(최근 window count 상위)
    st.subheader("Top-K 상승(최근 윈도우 count 기준)")
    if sm is None or sm.empty or "engagement_count" not in sm.columns:
        st.info("Silver social_metrics CSV가 없습니다. 02 단계를 먼저 실행하세요.")
    else:
        top = sm.sort_values("engagement_count", ascending=False).head(topk)
        st.dataframe(top, use_container_width=True)


if __name__ == "__main__":
    main()
