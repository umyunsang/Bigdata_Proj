# Bigdata_Proj

[![CI](https://github.com/umyunsang/Bigdata_Proj/actions/workflows/ci.yml/badge.svg)](https://github.com/umyunsang/Bigdata_Proj/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.12%2B-blue) [![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

소셜 비디오 빅데이터 실시간 처리 플랫폼을 위한 프로젝트입니다. 유튜브 등 소셜 미디어 데이터의 수집·전처리·분석을 스트리밍/배치 파이프라인으로 설계하고, 핵심 확률적 자료구조(Reservoir Sampling, Bloom Filter, HyperLogLog)와 다목적 최적화(Pareto Front)를 활용하는 것이 목표입니다.

## 주요 기능
- 데이터 수집: YouTube API v3 등에서 메타데이터/댓글/자막 스트리밍 또는 배치 수집
- 배치/스트리밍 처리: Spark/Structured Streaming 기반 ETL 파이프라인
- 중복/유니크 처리: Bloom Filter, HyperLogLog 적용
- 샘플링: Vitter's Algorithm R 기반 Reservoir Sampling
- 모델링: 다중 지표 기반 Pareto Front 최적화

## 저장소 구조
- `docs/`: 아키텍처, ETL/스트리밍 설계, 피처 엔지니어링, 학습/최적화, 실시간 예측 설계 문서
- `video-social-rtp-snippets/`: RTP/스트리밍 관련 코드/스니펫 모음
- `실습자료_2024/`: 유튜브 데이터 분석 실습 자료(텍스트/자막/라이브챗 등)
- `report.md`, `보고서_초안.md`: 설계/배경 정리 문서

## 빠른 시작
사전 준비: Python 3.12+, (옵션) Java/Spark, Git

설치
```
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

스니펫 실행 예시 및 프로젝트 스캐폴딩
```
# 0) 프로젝트 경로 스캐폴딩(권장)
cp -n .env.example .env || true
make scaffold   # 혹은: python scripts/scaffold_project.py
                 # 생성 경로 기본값: project/{data,chk,logs,artifacts}

# 1) 모의 이벤트 생성(landing)
python video-social-rtp-snippets/jobs/00_fetch_to_landing.py

# 2) 배치 적재 -> Bronze(Delta)
python video-social-rtp-snippets/jobs/10_bronze_batch.py

# 3) 스트리밍 윈도우 집계 -> Silver (블로킹 실행)
python video-social-rtp-snippets/jobs/20_silver_stream.py

# 4) Gold 피처/라벨 생성(CDF 컷 적용)
python video-social-rtp-snippets/jobs/30_gold_features.py

# 5) 다중모델 학습 및 Pareto 프론트 출력
python video-social-rtp-snippets/jobs/40_train_pareto.py

# (옵션) 예측값 적재 및 대시보드 실행
python video-social-rtp-snippets/jobs/50_predict_stream.py
python video-social-rtp-snippets/app/app.py
```

과제별 실행(00→05 단계)
```
# 00단계: 경로 스캐폴딩
cp -n .env.example .env || true
make scaffold   # project/ 하위 디렉토리 생성

# 01단계: 수집→배치 ETL
python video-social-rtp-snippets/jobs/00_fetch_to_landing.py
python video-social-rtp-snippets/jobs/10_bronze_batch.py

# 02단계: 스트리밍 집계(Silver)
python video-social-rtp-snippets/jobs/20_silver_stream.py

# 03단계: Gold 피처/라벨(CDF 컷)
python -m video_social_rtp.cli gold --top-pct 0.9   # Spark 불가 시: --fallback-local

# 04단계: 다중모델 학습/Pareto 프론트
python -m video_social_rtp.cli train            # Spark 불가 시: --fallback-local, MLflow 해제: --no-mlflow

# 05단계: (옵션) 예측/대시보드
python video-social-rtp-snippets/jobs/50_predict_stream.py   # (선택)
python -m video_social_rtp.cli ui                            # Streamlit UI 실행
```

## 문서
핵심 설계 문서들은 `docs/`에 정리되어 있습니다.
- 인덱스: `docs/README.md`
- 제출 가이드: `docs/과제제출_가이드.md`
- 상세 설계: `docs/00~05_*.md` 일련 파일들
- 실습자료 스택 준수: `docs/WORKFLOW_RULES.md`의 "실습자료_2024 기술스택 준수" 섹션 참고 (PySpark/Pandas/YouTube API/Streamlit 등).

## 데이터 경로(기본값)
스니펫은 `video-social-rtp-snippets/data` 및 `video-social-rtp-snippets/chk` 하위 경로를 사용합니다. 이미 샘플 데이터가 포함되어 있어 빠르게 전체 흐름을 확인할 수 있습니다. 환경변수로 경로를 바꾸려면 아래를 설정하세요.

```
export LANDING_DIR=path/to/data/landing
export BRONZE_DIR=path/to/data/bronze
export SILVER_DIR=path/to/data/silver
export GOLD_DIR=path/to/data/gold
export CHECKPOINT_DIR=path/to/chk
```

## 개발 노트
- 가상환경 디렉터리 `bigdata_env/`는 리포에서 제외합니다.
- 대용량 바이너리/아카이브는 Git LFS 사용을 권장합니다.

## 배지/메타
- Python: 3.12+
- 라이선스: 추후 명시


## 제출 폴더
모든 제출 산출물은 단일 폴더 `submission/` 하위에 정리합니다.
- `submission/README.md`
- `submission/과제01_데이터수집_ETL/` ~ `submission/과제05_실시간예측_UI/`
- 스크린샷 파일명 규칙: `과제NN_스크린샷_{설명}.png`
자세한 내용은 `docs/과제제출_가이드.md`를 참고하세요.

## 디렉터리 개요(요약)
```
합계 88
drwxrwxr-x  9 student_15030 student_15030  4096  9월 24 19:05 .
drwxr-x--- 32 student_15030 student_15030  4096  9월 24 19:03 ..
-rw-rw-r--  1 student_15030 student_15030  8196  9월 23 15:21 .DS_Store
drwxrwxr-x  8 student_15030 student_15030  4096  9월 24 19:03 .git
drwxrwxr-x  3 student_15030 student_15030  4096  9월 24 19:05 .github
-rw-rw-r--  1 student_15030 student_15030   194  9월 24 18:59 .gitignore
-rw-rw-r--  1 student_15030 student_15030   686  9월 24 19:05 LICENSE
-rw-rw-r--  1 student_15030 student_15030  2295  9월 24 19:03 README.md
drwxrwxr-x  7 student_15030 student_15030  4096  9월 23 15:26 bigdata_env
drwxrwxr-x  2 student_15030 student_15030  4096  9월 24 16:58 docs
drwxrwxr-x  2 student_15030 student_15030  4096  9월 24 19:05 examples
-rw-rw-r--  1 student_15030 student_15030  6120  9월 23 16:34 report.md
-rw-rw-r--  1 student_15030 student_15030  3330  9월 24 19:02 requirements.txt
drwxrwxr-x  9 student_15030 student_15030  4096  9월 24 18:11 video-social-rtp-snippets
-rw-rw-r--  1 student_15030 student_15030 14147  9월 24 18:20 보고서_초안.md
drwxrwxr-x  5 student_15030 student_15030  4096  9월 24 18:39 실습자료_2024

```
