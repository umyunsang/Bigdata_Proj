# Bigdata_Proj · Video/Social Real-Time Pipeline

[![CI](https://github.com/umyunsang/Bigdata_Proj/actions/workflows/ci.yml/badge.svg)](https://github.com/umyunsang/Bigdata_Proj/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.12%2B-blue) [![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE) ![Spark](https://img.shields.io/badge/Spark-Structured%20Streaming-orange) ![YouTube API](https://img.shields.io/badge/YouTube%20Data%20API-v3-critical)

> **유튜브 Data API v3** 기반으로 소셜 비디오 데이터를 수집·정제·분석하는 **빅데이터 파이프라인** 예제입니다. `video-social-rtp-snippets/` 스니펫과 `실습자료_2024/` 학습 노트를 토대로, 실제 프로젝트 패키지(`video_social_rtp/`)를 구축했습니다.

## Table of Contents
- [Repository at a Glance](#repository-at-a-glance)
- [What We Analyze & Predict](#what-we-analyze--predict)
- [Spotlight: Snippets & Labs](#spotlight-snippets--labs)
- [Pipeline Architecture](#pipeline-architecture)
- [Project Flow · 00 → 05](#project-flow--00--05)
- [Getting Started](#getting-started)
- [Automation & Tooling](#automation--tooling)
- [Reports & Submission Assets](#reports--submission-assets)
- [Open Source & Licenses](#open-source--licenses)
- [Community Notes](#community-notes)

## Repository at a Glance
- **End-to-end Spark/Delta pipeline** that fetches YouTube search results, lands raw JSON, and promotes data through Bronze ➜ Silver ➜ Gold tables.
- **Fallback-friendly design** – 모든 단계는 Spark 없는 로컬 환경에서도 Pandas/CSV 기반으로 동작합니다.
- **Artifacts-first workflow** – `project/` 폴더 하위에 Delta Lake 테이블, 체크포인트, MLflow 로그, UI용 CSV를 남깁니다.
- **Dual-language documentation** – `docs/`에 과제 00~05 단계별 한글 설명과 Mermaid 아키텍처가 정리되어 있습니다.

## What We Analyze & Predict

### 🎯 목적: K-POP 비디오의 바이럴 가능성 예측

이 프로젝트는 **YouTube에서 실시간으로 수집되는 K-POP 비디오 데이터**를 분석하여 **어떤 비디오가 바이럴될지 예측**합니다.

#### 📥 수집 데이터 (YouTube Data API v3)
- **비디오 메타데이터**: 제목, ID, 채널 정보
- **검색어**: K-POP, BTS, BLACKPINK, NewJeans, aespa 등 50+ 키워드
- **지역 설정**: 한국(KR), 한국어(ko)
- **수집 빈도**: 매일 2회 (오전 9시, 오후 3시)

#### 🔄 실시간 스트리밍 집계 (Silver)
- **Sliding Window**: 1시간 윈도우, 5분 슬라이드
- **Watermark**: 10분 지연 이벤트 허용
- **집계 지표**: video_id별 시간대별 등장 횟수

#### ⚙️ 피처 엔지니어링 (Gold)
1. **engagement_24h**: 24시간 동안의 총 참여도 (높을수록 인기)
2. **uniq_users_est**: HyperLogLog으로 추정한 고유 사용자 수
3. **label**: CDF 기반 이진 분류
   - **label=1**: 상위 10% 바이럴 비디오 🔥
   - **label=0**: 나머지 90% 일반 비디오

#### 🤖 머신러닝 모델 (Train)
- **예측 문제**: "이 K-POP 비디오가 바이럴될까?" (이진 분류)
- **입력 피처**: engagement_24h, uniq_users_est
- **사용 모델**: Logistic Regression, Random Forest, Gradient Boosting Tree
- **모델 선택**: Pareto Front로 성능·속도·복잡도 최적화

#### 🔮 예측 결과 예시
| Video | Engagement | Unique Users | 예측 | 의미 |
|-------|------------|--------------|------|------|
| IVE 'XOXZ' MV | 150.0 | 120.0 | **1** | 바이럴 가능성 높음 🔥 |
| BTS Quiz | 8.0 | 5.0 | **0** | 일반 성과 예상 |
| BLACKPINK 'JUMP' | 200.0 | 180.0 | **1** | 바이럴 가능성 높음 🔥 |

#### 📊 Streamlit UI 기능
- **Bloom Filter**: 비디오 최근 존재 여부 체크
- **CDF/PDF 차트**: 전체 분포와 cutoff 시각화
- **예측 결과**: 바이럴 가능성 표시
- **Top-K 랭킹**: 최근 인기 비디오 목록

### 💡 활용 시나리오
- K-POP 마케터: 어떤 컨텐츠가 바이럴될지 조기 예측
- 기획사: 프로모션 전략 수립을 위한 데이터 기반 의사결정
- 분석가: K-POP 트렌드 실시간 모니터링

## Spotlight: Snippets & Labs
### `video-social-rtp-snippets/`
- 수업용 **미니 코드 블럭** 모음. Reservoir sampling, Bloom filter, HyperLogLog(HLL), Pareto front 등 핵심 개념이 짧은 스크립트로 정리되어 있습니다.
- `jobs/00~50_*.py` 스크립트는 각 단계의 최소 구현을 제공하며, `cli.py` 단일 진입점으로 연속 실행을 도와줍니다.

### `실습자료_2024/`
- 유튜브 데이터 분석 실습 노트(Jupyter)와 참고 자료(ppt, 데이터셋)가 모여 있습니다.
- Spark, Pandas, 텍스트 분석, 지도 시각화(GeoPandas) 등 **프로젝트에 필요한 사전 지식**을 단계별로 학습할 수 있게 구성했습니다.
- `BDA_Hands_on_Numerical_and_Textual_Data_Analytics_using_Youtube_API.ipynb`는 YouTube Data API 활용 예제를 포함하고, 프로젝트 코드와 직접 연결됩니다.

## Pipeline Architecture
```
YouTube Data API v3 ─┐
Social/Text Streams ─┼──► [Landing]
                     │       │  Reservoir Sampling
                     │       ▼
                     │    [Bronze] ── Bloom Filter & Delta Lake
                     │       │
                     │       ▼
                     │    [Silver] ── Structured Streaming (window + watermark)
                     │       │
                     │       ▼
                     │    [Gold] ── CDF/PDF labeling + HLL unique counts
                     │       │
                     │       ▼
                     │   [Train] ── Pareto front multi-metric model selection
                     │       │
                     │       ▼
                     └──► [Serve] ── Streamlit dashboard & API-ready artifacts
```

> 상세 설계는 `docs/00_프로젝트_개요_및_아키텍처.md`와 `docs/architecture.mmd`를 참고하세요.

## Project Flow · 00 → 05
| Step | Description | Snippet Runner | Project CLI |
|------|-------------|----------------|-------------|
| 00 | 경로 스캐폴딩 및 환경 변수 로드 | `make scaffold` 또는 `python scripts/scaffold_project.py` | `python -m video_social_rtp.cli scaffold` |
| 01 | YouTube fetch → Landing → Bronze Delta 적재 | `make fetch` → `make bronze` | `python -m video_social_rtp.cli fetch` → `... bronze` |
| 02 | Structured Streaming 윈도우 집계(Silver) | `make silver` | `python -m video_social_rtp.cli silver --once` |
| 03 | Gold 피처/라벨 생성(CDF 컷 적용) | `make gold` | `python -m video_social_rtp.cli gold --top-pct 0.9` |
| 04 | 다중모델 학습 + Pareto Front 산출 | `make train` | `python -m video_social_rtp.cli train [--no-mlflow]` |
| 05 | 실시간 예측 대시보드(Streamlit) | `make ui` | `python -m video_social_rtp.cli ui --port 8501` |

- **Spark 미사용 환경**: `*_FALLBACK_LOCAL` 환경 변수 또는 CLI 옵션(`--fallback-local`)로 Pandas 기반 대체 경로를 활성화할 수 있습니다.
- **MLflow**는 로컬 파일 기반(`project/artifacts/mlruns/`)으로 설정되며, 필요 시 `--no-mlflow` 옵션으로 비활성화합니다.

## Getting Started
### 1. Prerequisites
- Python 3.12+
- (옵션) Java 8+ 및 Apache Spark 3.5.x (Structured Streaming & Delta Lake 실행용)
- YouTube Data API v3 키 (실제 호출 시)

### 2. 환경 구성
```bash
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp -n .env.example .env 2>/dev/null || true  # 필요 시 수동 생성
```

`.env` 예시 키
```
PROJECT_ROOT=project
YT_API_KEY=your_youtube_api_key
LANDING_DIR=${PROJECT_ROOT}/data/landing
BRONZE_DIR=${PROJECT_ROOT}/data/bronze
SILVER_DIR=${PROJECT_ROOT}/data/silver
GOLD_DIR=${PROJECT_ROOT}/data/gold
CHECKPOINT_DIR=${PROJECT_ROOT}/chk
LOG_DIR=${PROJECT_ROOT}/logs
ARTIFACT_DIR=${PROJECT_ROOT}/artifacts
```
- API 키가 없으면 `fetch` 단계가 **자동으로 mock 데이터**를 생성합니다.
- Google API 클라이언트를 사용할 때는 `google-api-python-client`가 설치되어 있어야 하며, 이미 requirements에 포함되어 있습니다.

### 3. 빠른 실행
```bash
make scaffold
make fetch bronze silver gold train ui  # 원하는 단계까지 순차 실행
# 또는
python -m video_social_rtp.cli scaffold
python -m video_social_rtp.cli fetch --query "데이터 엔지니어링" --max-items 100 --reservoir-k 64
python -m video_social_rtp.cli bronze
python -m video_social_rtp.cli silver --once
python -m video_social_rtp.cli gold --top-pct 0.9
python -m video_social_rtp.cli train --no-mlflow
python -m video_social_rtp.cli ui --port 8501
```

## Automation & Tooling
- **Makefile**: `venv`, `install`, `fetch`, `gold_cli` 등 반복 작업을 단축합니다.
- **Scripts**: `scripts/scaffold_project.py`는 Step 00을 수행하며 `.env`를 읽어 디렉터리를 생성합니다.
- **Logging**: `video_social_rtp/core/logging.py`에서 JSON 포맷 로그를 남기고, `project/logs/`에서 확인할 수 있습니다.
- **Artifacts**: Gold 컷오프(`project/artifacts/gold_cutoff.json`), Pareto 결과(`project/artifacts/pareto.json`), MLflow 런 등이 자동 저장됩니다.

### 자동화 시스템 (30일간 데이터 축적)
- **매일 자동 실행**: cron 작업을 통해 매일 오전 9시, 오후 3시에 자동으로 YouTube API 데이터 수집 및 Spark 처리
- **30일간 지속**: 2025-09-30부터 30일간 매일 자동으로 K-POP 관련 데이터 축적
- **백그라운드 실행**: SSH 연결이 끊어져도 nohup을 통해 백그라운드에서 계속 실행
- **주간 분석**: 매주 일요일 오전 10시에 자동으로 주간 분석 리포트 생성
- **API 할당량 관리**: YouTube Data API v3 일일 할당량(10,000 units) 내에서 안전하게 운영

#### 자동화 설정 방법
```bash
# Cron 작업 설정
crontab -e

# 다음 내용 추가:
# 매일 오전 9시에 데이터 축적 실행
0 9 * * * nohup /home/student_15030/Bigdata_Proj/scripts/daily_automation.sh >/dev/null 2>&1

# 매주 일요일 오전 10시에 주간 분석 실행  
0 10 * * 0 nohup /home/student_15030/Bigdata_Proj/scripts/weekly_analysis.sh >/dev/null 2>&1
```

#### 자동화 스크립트
- `scripts/daily_automation.sh`: 매일 YouTube API 데이터 수집 및 Spark 파이프라인 실행
- `scripts/weekly_analysis.sh`: 주간 데이터 분석 및 리포트 생성
- `scripts/setup_cron.sh`: Cron 작업 자동 설정 스크립트

## Reports & Submission Assets
- `report.md`, `보고서_초안.md`, `submission/보고서_최종.md`에 배경 조사와 설계 근거가 정리되어 있습니다.
- `submission/` 폴더는 과제별 산출물(스크린샷, 실행 로그, 설명)을 단계별로 모아둔 제출 패키지입니다.
- `docs/과제제출_가이드.md`는 제출 포맷과 naming rule을 안내합니다.

## Open Source & Licenses
리포지터리는 **Apache License 2.0** (`LICENSE`)을 따릅니다. 주요 의존성 라이선스는 다음과 같습니다.

| Library | Purpose in Project | License |
|---------|-------------------|---------|
| `pyspark` 3.5.x | 배치/스트리밍 ETL, ML 파이프라인 | Apache License 2.0 |
| `delta-spark` | Delta Lake 액세스 및 ACID 테이블 | Apache License 2.0 |
| `google-api-python-client` | YouTube Data API v3 호출 | Apache License 2.0 |
| `google-auth`, `google-auth-httplib2`, `google-auth-oauthlib` | Google API 인증 | Apache License 2.0 |
| `pandas`, `numpy`, `scipy`, `seaborn` | 피처 엔지니어링, 통계 분석 | BSD 3-Clause |
| `streamlit` | 실시간 UI 대시보드 | Apache License 2.0 |
| `mlflow` | 실험 추적 및 메트릭 로깅 | Apache License 2.0 |
| `click`, `typer` | CLI 구축 | BSD 3-Clause |
| `rich` | CLI 출력 포매팅 | MIT License |
| `faker`, `textblob`, `beautifulsoup4`, `youtube-transcript-api` | Mock 데이터, NLP 실습, 자막 처리 | MIT License |
| `spacy`, `spacytextblob` | 텍스트 파이프라인 고급 예제 | MIT License |
| `geopandas`, `folium`, `shapely` | Geo 데이터 실습 노트 | BSD 3-Clause / MIT |
| `requests`, `httpx` | 외부 API 통신 | Apache License 2.0 |
| `jupyter`, `ipython`, `notebook`, `ipywidgets` | 실습 노트 실행 환경 | BSD 3-Clause |

> 전체 목록은 `requirements.txt`에서 확인할 수 있으며, 각 패키지의 PyPI 페이지에서 최신 라이선스를 반드시 검증하세요.

## Community Notes
- **이 저장소는 교육 목적**으로 제작되었으며, 실시간 운용 시에는 YouTube API 이용약관과 쿼터 제한을 준수해야 합니다.
- Spark/Delta 실행이 어려운 환경에서는 제공된 Pandas 기반 대체 경로로 개념을 검증한 뒤, 클러스터 환경에서 확장하는 것을 권장합니다.
- 개선 아이디어나 질문은 Issue/PR로 자유롭게 공유해 주세요.

행복한 데이터 엔지니어링 되세요! 🚀
