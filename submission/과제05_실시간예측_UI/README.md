# Assignment 05 · Real-time Prediction UI

This directory showcases the Streamlit dashboard built on top of the streaming/gold artifacts for real-time insight and inference.

## Screenshots
- `Streamlit UI 실행.png`: Proof that the Streamlit server launches successfully for the Step 05 interface.
- `사이드바 입력.png`: Sidebar controls demonstrating how operators supply a `video_id`, adjust Top-K windows, and toggle parameters.
- `Bloom Filter 체크.png`: Bloom-style existence check summarizing whether the selected video appears in the recent streaming window.
- `CDF:PDF 차트.png`: Dual chart rendering (CDF/PDF) of engagement scores, annotated with the gold-layer cut-off.
- `Top-K 테이블.png`: Table of top-K trending videos derived from the latest silver metrics.
- `예측 결과.png`: Highlight of per-video feature/label details returned to the user, simulating real-time scoring output.
