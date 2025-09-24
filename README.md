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
사전 준비: Python 3.12+, Java(스파크 사용 시), Git

설치
```
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

실행 예시(개발용)
```
# 예) 데이터 수집/전처리 스크립트 실행
python path/to/your_script.py --config config.yaml
```

## 문서
핵심 설계 문서들은 `docs/` 디렉터리에 정리되어 있습니다.
- 00_프로젝트_개요_및_아키텍처.md: 전체 아키텍처 개요
- 01_데이터_수집_및_배치_ETL_설계.md: 배치 수집/ETL 설계
- 02_스트리밍_데이터_처리_설계.md: 실시간 파이프라인 설계
- 03_피처_엔지니어링_및_라벨링_설계.md: 피처/라벨링 전략
- 04_모델_학습_및_최적화_설계.md: 학습/평가/최적화(파레토 프론트)
- 05_실시간_예측_시스템_설계.md: 서빙/모니터링

## 개발 노트
- 가상환경 디렉터리 `bigdata_env/`는 리포에서 제외합니다.
- 대용량 바이너리/아카이브는 Git LFS 사용을 권장합니다.

## 배지/메타
- Python: 3.12+
- 라이선스: 추후 명시


## 예시 실행
```
python examples/stream_algorithms_demo.py
```

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
