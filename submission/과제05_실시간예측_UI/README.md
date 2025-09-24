## 과제05 — 실시간 예측/조회 UI (Streamlit)

프로젝트 CLI(추천)
- UI 실행: `python -m video_social_rtp.cli ui` (기본 포트 8501)
- 수동 실행: `streamlit run video_social_rtp/serve/ui.py --server.port=8501`

입력/출력 경로
- 입력: `project/data/silver/social_metrics/*.csv`(폴백), `project/data/gold/features.csv`, `project/artifacts/gold_cutoff.json`
- 출력: 스크린샷(대시보드 화면), (선택) 예측 append 결과

권장 스크린샷 체크리스트
- Bloom 존재 가능성(근사) 메트릭
- Gold CDF/PDF 차트 + 컷오프 표시
- Top-K 상승(최근 윈도우 count 기준) 테이블

스니펫 경로(참고)
- `python video-social-rtp-snippets/jobs/50_predict_stream.py`

