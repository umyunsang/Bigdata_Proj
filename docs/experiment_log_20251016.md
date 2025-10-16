# 2025-10-16 파이프라인 실행 기록

이번 실행은 “BTS official” 실데이터 흐름을 재현하고, 보고서 초안에서 활용할 수 있는 최신 산출물과 로그 경로를 정리하기 위한 목적입니다. 실행 스크린샷은 추후 동일 경로에 채워 넣을 예정입니다.

## 1. 실행 환경
- 실행 일시: 2025-10-16 06:37–06:39 UTC (KST 15:37–15:39)
- 프로젝트 루트: `/home/student_15030/Bigdata_Proj`
- 가상환경 인터프리터: `.venv/bin/python` (Python 3.12)
- 주요 패키지: PySpark 3.5.1, delta-spark 3.2.0
- 기본 쿼리: `BTS official`, 지역 `KR`, 언어 `ko`

## 2. 단계별 명령어 및 요약
| 순서 | 명령어 | 주요 로그/지표 |
|---|---|---|
| 1 | `.venv/bin/python -m video_social_rtp.cli fetch --query "BTS official" --max-items 50 --reservoir-k 64 --region-code KR --relevance-language ko --no-skip-if-exists --no-mock` | landing 저장: `project/data/landing/events_1760596653.json` |
| 2 | `.venv/bin/python -m video_social_rtp.cli bronze` | Bronze 적재 행: 49, Bloom 필터 갱신 (`project/artifacts/bronze_bloom.*`) |
| 3 | `.venv/bin/python -m video_social_rtp.cli silver --once` | Structured Streaming one-shot 완주, 체크포인트 `project/chk/silver` |
| 4 | `.venv/bin/python -m video_social_rtp.cli gold --top-pct 0.9` | Gold Delta/CSV 갱신, 산출 행: 3 |
| 5 | `.venv/bin/python -m video_social_rtp.cli train --no-mlflow` | Pareto 전선 아티팩트: `project/artifacts/pareto.json` (Pareto 후보 1) |

## 3. 핵심 산출물
- Landing 원본: `project/data/landing/events_1760596653.json`
- Bronze Delta: `project/data/bronze` (파티션: `ingest_date`, `artist`)
- Silver Delta: `project/data/silver` (윈도우 집계, 체크포인트 `project/chk/silver`)
- Gold CSV: `project/data/gold/features.csv`
- Gold 아티팩트: `project/artifacts/gold_tiers.json`
- Pareto 결과: `project/artifacts/pareto.json`

## 4. Gold 피처 스냅샷
| artist | total_engagement | total_videos | unique_viewers_est | engagement_cdf | percentile | tier | trend_direction |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BLACKPINK | 926 | 37 | 1 | 1.0 | 1.0 | 1 | STEADY |
| BTS | 281 | 15 | 4 | 0.27543035993740217 | 0.5 | 4 | STEADY |
| OTHER | 71 | 4 | 1 | 0.05555555555555555 | 0.0 | 4 | STEADY |

세부 열(평균·최대, 모멘텀 등)은 `project/data/gold/features.csv` 원본에서 재확인할 수 있습니다.

## 5. 모델 성능 요약 (Pareto 결과)
| 모델 | accuracy | f1 | latency_ms | feat_count | fit_ms | Pareto 포함 |
| --- | --- | --- | --- | --- | --- | --- |
| Logistic Regression | 1.0 | 1.0 | 22.6543 | 8 | 2957.9594 | - |
| Random Forest | 1.0 | 1.0 | 18.8236 | 8 | 742.7564 | ✅ |

- Pareto 전선 후보는 Random Forest 1개로 확인됨.
- `project/artifacts/pareto.json`에서 추가 메타데이터를 확인 가능.

## 6. 재현 체크리스트
- [ ] Streamlit UI: `.venv/bin/python -m video_social_rtp.cli ui --port 8501` 실행 후 화면 캡처 예정
- [ ] Silver/Gold Delta 테이블 스키마 캡처 또는 Spark SQL 결과 스크린샷 수집
- [ ] 보고서 작성 시 본 문서의 표/경로를 인용하고, 최신 값으로 갱신 여부 확인

## 7. 주의 및 향후 작업
- 시스템 전역 `python` 명령이 없으므로, 보고서에는 `.venv/bin/python` 사용을 명시해야 함.
- 필요 시 `--fallback-local` 옵션으로 Spark 미지원 환경 대비 가능함을 보고서에 서술.
- 스크린샷 수집 후 `submission/` 하위 폴더 구조와 연동 계획 작성 예정.
