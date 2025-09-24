## 과제01 — 데이터 수집 및 배치 ETL

실행 명령(루트 기준)
- 경로 스캐폴딩(권장): `cp -n .env.example .env || true && make scaffold`

프로젝트 CLI(추천)
- Fetch → Landing: `python -m video_social_rtp.cli fetch --max-items 50`
- Bronze 적재: `python -m video_social_rtp.cli bronze`
  - (샌드박스/로컬 Spark 불가 시) `python -m video_social_rtp.cli bronze --fallback-local`

스니펫 경로(참고)
- `python video-social-rtp-snippets/jobs/00_fetch_to_landing.py`
- `python video-social-rtp-snippets/jobs/10_bronze_batch.py`

스크린샷은 `screenshots/` 폴더에 `과제01_스크린샷_{설명}.png` 규칙으로 저장합니다.

권장 스크린샷 체크리스트
- 프로젝트 폴더 생성: `project/` 트리(landing/bronze 등)
- fetch 성공 콘솔 + 생성 파일명 표시
- bronze 적재 성공(또는 fallback) 콘솔, 결과 파일/rows 수

