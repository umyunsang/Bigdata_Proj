## 과제03 — 피처 엔지니어링 및 라벨링(Gold)

프로젝트 CLI(추천)
- Gold 피처/라벨: `python -m video_social_rtp.cli gold --top-pct 0.9`
- (샌드박스/Spark 불가 시) `python -m video_social_rtp.cli gold --fallback-local`

입력 경로
- Silver 메트릭: `project/data/silver/social_metrics/` (Delta 또는 fallback CSV)

출력 산출물
- Gold 피처: `project/data/gold/features` (Delta; 폴백 시 `project/data/gold/features.csv`)
- 컷오프 아티팩트: `project/artifacts/gold_cutoff.json` (percentile, cutoff 기록)

권장 스크린샷 체크리스트
- gold 실행 콘솔 로그
- Gold 피처 테이블/CSV 앞부분 미리보기
- `gold_cutoff.json` 내용(컷오프 값 확인)

스니펫 경로(참고)
- `python video-social-rtp-snippets/jobs/30_gold_features.py`

