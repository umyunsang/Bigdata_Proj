## 과제02 — 스트리밍 구조화/정제(Silver)

프로젝트 CLI(추천)
- 스트리밍 집계: `python -m video_social_rtp.cli silver`
- (샌드박스/로컬 Spark 불가 시) `python -m video_social_rtp.cli silver --fallback-local`
- 파라미터: `--watermark "10 minutes" --window "1 hour" --slide "5 minutes"`

스니펫 경로(참고)
- `python video-social-rtp-snippets/jobs/20_silver_stream.py`

산출물 경로
- Silver 메트릭: `project/data/silver/social_metrics/`
- 체크포인트: `project/chk/silver/`

권장 스크린샷 체크리스트
- 스트리밍 시작 콘솔 로그(또는 fallback 실행 로그)
- social_metrics 생성 파일(미리보기 포함)
- 체크포인트 폴더 생성 확인

