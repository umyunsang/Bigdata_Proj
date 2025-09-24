## 프로젝트 구축 워크플로우 Rules (YouTube API v3 기반)

목적
- 스니펫(`video-social-rtp-snippets/`)을 참고하되, 실제 제품 수준의 파이프라인을 단계적으로 구축하기 위한 개발 규칙을 정의합니다.
- 각 단계는 과제 01~05에 대응하며, 실행 결과 스크린샷과 요약(및 최종 보고서)을 제출합니다.

핵심 원칙
- 단계적 구축: 한 번에 전부 구현하지 않고, 00→05 순으로 설계도(`docs/00~05_*.md`)를 따라 진행합니다.
- 참조 우선: 스니펫 코드는 참조/가이드로 활용하고, 실제 구현은 `video_social_rtp` 패키지에 작성합니다.
- 프로덕트 지향: 설계 시부터 재현성, 장애 복원력, 관측성(로그/메트릭), 설정 분리(.env)을 고려합니다.
- 단일 런타임 폴더: 모든 산출물(데이터/체크포인트/로그)은 `PROJECT_ROOT`(기본 `project/`) 하위로 모읍니다.
- 증빙 우선: 각 단계 종료 시 산출물 경로, 핵심 지표/로그, 스크린샷을 `submission/`에 정리합니다.

실습자료_2024 기술스택 준수
- 기준 폴더: `실습자료_2024/`
- 스택 구성(핵심 매핑)
  - PySpark/Spark SQL(DataFrames) — 01(Bronze), 02(Silver 스트리밍), 03(Gold 집계)에 기본 적용. 환경 제약 시 Pandas 기반 폴백 허용.
  - Pandas — EDA 및 폴백 집계(03 단계에서 사용). 노트북(05-PySpark.ipynb, 06-SparkDataFrames.ipynb, 07/08-Pandas*) 흐름 참고.
  - YouTube API v3 — `BDA_Hands_on_*YouTube_API.ipynb` 흐름을 참고하여 `googleapiclient` 사용(01 단계 수집에 적용).
  - (선택) MLflow — `mlflow_example.zip` 참조, 04 단계 학습 실험 추적에 사용. 미사용 환경에서는 JSON 아티팩트로 대체.
  - Hadoop/MapReduce — 01~02 단계 설계 사상(파티셔닝/집계/중복제거)에 개념적 매핑. 필요 시 HDFS 경로로 치환 가능하도록 상대/ENV 경로 유지.
  - Streamlit — 05 단계 UI 구성에 적용.
  - Jupyter — 실험/데모는 노트북 기반으로 수행 가능하나, 최종 파이프라인은 패키지/CLI로 실행.
  - 기타 자료: `StreamingAlogrithms_2024.pptx`를 윈도우/워터마크 설계 검토 자료로 활용.

환경/경로 규칙
- `PROJECT_ROOT` 기본값: `./project`
- 파생 경로(ENV로 오버라이드 가능):
  - `LANDING_DIR=${PROJECT_ROOT}/data/landing`
  - `BRONZE_DIR=${PROJECT_ROOT}/data/bronze`
  - `SILVER_DIR=${PROJECT_ROOT}/data/silver`
  - `GOLD_DIR=${PROJECT_ROOT}/data/gold`
  - `CHECKPOINT_DIR=${PROJECT_ROOT}/chk`
  - `LOG_DIR=${PROJECT_ROOT}/logs`
- `.env`에 키/경로를 정의하고, `.env.example`를 제공(비밀 키 커밋 금지).

코드/패키징 규칙
- 실제 구현 패키지: `video_social_rtp/`
  - `ingest/`(YouTube API v3 클라이언트, 페이징/쿼터/리트라이), `bronze/`, `silver/`, `features/`, `train/`, `serve/` 모듈화
  - 공통 설정/유틸: `core/config.py`, `core/logging.py`, `core/io.py`
- CLI: `python -m video_social_rtp.cli [fetch|bronze|silver|gold|train|predict|ui]`
- 의존성은 `requirements.txt`에 고정, 재현을 위해 가상환경 사용.

데이터/스키마 규칙
- 저장 포맷: Delta/Parquet, Bronze 이상부터는 파티션(`ingest_date`, `source`).
- 스키마 버저닝: `conf/schemas/`에 JSON Schema(또는 md)로 관리, 마이그레이션 시 변경 이력 기록.
- 샘플/진단: 대량 인입 전 대표 샘플(Reservoir Sampling) 별도 보관 `${BRONZE_DIR}/_sample`.

YouTube API v3 규칙
- 인증: API Key 또는 OAuth 2.0(보고서에 선택 근거 명시). 키는 `.env`에만 저장.
- 쿼터/리트라이: 지수 백오프, 429/5xx 처리, 페이지네이션/시간창 분할 수집.
- 중복 방지: 최근 ID Bloom Filter + 고유키(post_id/video_id) 기반 업서트/삽입 제어.
- 모의→실계: 초기엔 모의 데이터 + 소량 실데이터 혼합으로 파이프라인 검증 후 확장.

로그/관측성/에러 처리
- 표준 로깅 포맷(JSON 라인 권장), 단계별 타이밍/레코드 수/오류 개수 기록.
- 스트리밍 잡은 체크포인트 `${CHECKPOINT_DIR}/silver` 사용, 장애 시 자동 복구.
- (선택) 지표: 처리율/지연/오류 카운터를 파일 또는 Prometheus로 노출.

제출/증빙 규칙
- 단일 제출 폴더: `submission/과제NN_*` 하위에 스크린샷(`과제NN_스크린샷_*.png`), 요약(`과제NN_요약.md`), (선택) 산출물 일부.
- 보고서: 최종 PDF(5쪽 내외) + 부록에 AI 사용 내역/프롬프트 명시.
- 패키징: `make package`로 코드/제출물 zip 생성(요구 시).

단계별 체크리스트 & 게이트
- 00 준비(경로/환경)
  - 실행: `cp -n .env.example .env || true && make scaffold` (또는 `python scripts/scaffold_project.py`)
  - 결과물: `project/` 트리 생성, `.env`, `make venv && make install` 로그
  - 스크린샷: 폴더 트리, 설치 완료 콘솔
  - 게이트: 다음 단계로 진행 승인
- 01 수집/배치(Bronze)
  - 구현: YouTube API 클라이언트, Reservoir 샘플링, Bloom 1차 필터, NDJSON→Delta 적재
  - 결과물: `${LANDING_DIR}/*.json`, `${BRONZE_DIR}/_delta_log/*`
  - 스크린샷: 파일 생성, 적재 후 카운트 로그
- 02 스트리밍(Silver)
  - 구현: 파일 소스 스트리밍, Watermark/Sliding Window, 중복 제거, 체크포인트
  - 결과물: `${SILVER_DIR}/social_metrics`, `${CHECKPOINT_DIR}/silver/*`
  - 스크린샷: 스트리밍 콘솔/윈도우 집계 결과
- 03 피처/라벨(Gold)
  - 구현: approx_count_distinct(HLL), CDF 컷 기반 라벨링, 피처 테이블
  - 결과물: `${GOLD_DIR}/features`
  - 스크린샷: 컷오프/분포 확인
  - 실행: `python -m video_social_rtp.cli gold --top-pct 0.9` (Spark 불가 시 `--fallback-local`)
- 04 학습/파레토
  - 구현: 다모델 후보 학습, AUC/지연/피처수 메트릭, Pareto Front 산출(JSON)
  - 결과물: `project/artifacts/pareto.json`(권장)
  - 스크린샷: 후보 성능표, Pareto 시각 요약
- 05 서빙/UI
  - 구현: Streamlit 대시보드(CDF/PDF, Bloom 존재 가능성), (선택) 예측 append
  - 결과물: `app` 실행 스크린샷, 쿼리 예시
  - 실행: `python -m video_social_rtp.cli ui` (또는 `streamlit run video_social_rtp/serve/ui.py`)

개발 사이클(각 단계 공통)
- 설계 확인 → 최소 기능 구현 → 로컬 검증(샘플/로그) → 산출물/스크린샷 정리 → README/요약 업데이트 → 게이트 승인.

주의/금지
- API 키/비밀정보 커밋 금지, 대용량 원시데이터 커밋 금지(Git LFS/외부 스토리지 고려).
- 단계 건너뛰기 금지, 증빙/문서 없이 다음 단계 착수 금지.

참고
- 설계 문서 인덱스: `docs/README.md`
- 제출 가이드: `docs/과제제출_가이드.md`
- 스니펫 참조: `video-social-rtp-snippets/`

현재 단계(00~03) 스택 적용 검증
- 01 수집: `googleapiclient` 기반 YouTube API v3 사용(키 없을 시 mock). 실습자료의 YouTube API 노트북과 동일 스택.
- 01 배치: PySpark + Delta Writer(정상), 제한 시 로컬 폴백. 실습자료 PySpark 흐름과 일치, 폴백은 Pandas/파일 I/O 기반.
- 02 스트리밍: PySpark Structured Streaming(정상), 제한 시 로컬 창구간 집계 폴백. 실습자료 스트리밍 알고리즘 슬라이드 개념 반영.
- 03 피처/라벨: PySpark 경로(정상), 제한 시 Pandas로 CDF 컷 산출. 실습자료의 Pandas/Spark 데이터프레임 주제 사용.
