from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from ..core.config import load_settings


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
    if not sm_dir.exists():
        return None
    csvs = sorted([p for p in sm_dir.glob("*.csv")], key=lambda p: p.stat().st_mtime, reverse=True)
    if not csvs:
        return None
    try:
        return pd.read_csv(csvs[0])
    except Exception:
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


def main() -> None:
    st.set_page_config(page_title="Video/Social RTP — UI", layout="wide")
    st.title("실시간 예측/조회 (과제05)")
    st.caption("실습자료 스택(Streamlit, Pandas) 기반 UI 예시")

    # Load data
    feats = _load_gold_features_csv()
    sm = _load_latest_silver_csv()
    cutoff = _load_cutoff_artifact()

    with st.sidebar:
        st.header("입력/옵션")
        vid = st.text_input("video_id", value="v0")
        topk = st.slider("Top-K (최근 윈도우)", 1, 20, 5)

    # Existence check (bloom-like)
    exists = False
    if sm is not None and "video_id" in sm.columns:
        exists = vid in set(sm["video_id"].astype(str))
    st.subheader("Bloom 존재 가능성(근사)")
    st.write("최근 윈도우 데이터 기준 존재 여부(집합 기반 근사)")
    st.metric("최근 존재 가능성", "있음" if exists else "없음")

    # CDF/PDF charts from gold features
    st.subheader("Gold 분포(CDF/PDF) 및 컷오프")
    if feats is None or feats.empty or "engagement_24h" not in feats.columns:
        st.info("Gold features.csv를 찾지 못했거나 비어 있습니다. 03 단계를 먼저 실행하세요.")
    else:
        df = feats.copy()
        col = "engagement_24h"
        df = df.sort_values(col)
        df["pdf"] = 1 / len(df)
        df["cdf"] = df["pdf"].cumsum()
        st.line_chart(df[[col, "cdf"]].set_index(col), height=240)
        st.bar_chart(df[[col, "pdf"]].set_index(col).head(50), height=240)
        if cutoff is not None:
            st.info(f"현재 컷오프: engagement_24h ≥ {cutoff}")

        # Query row
        q = df[df["video_id"].astype(str) == str(vid)]
        if not q.empty:
            st.success(f"선택 video_id={vid}의 engagement_24h={int(q[col].iloc[0])}, label={int(q['label'].iloc[0])}")
        else:
            st.warning("선택한 video_id가 Gold 테이블에 없습니다.")

    # Top-K 상승률(최근 window count 상위)
    st.subheader("Top-K 상승(최근 윈도우 count 기준)")
    if sm is None or sm.empty or "count" not in sm.columns:
        st.info("Silver social_metrics CSV가 없습니다. 02 단계를 먼저 실행하세요.")
    else:
        top = sm.sort_values("count", ascending=False).head(topk)
        st.dataframe(top, use_container_width=True)


if __name__ == "__main__":
    main()

