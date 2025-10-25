# K-POP 아티스트 인기도 분석 시스템
## YouTube 데이터 기반 분기별 트렌드 예측 및 시각화

[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/) [![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange)](https://spark.apache.org/) [![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0.0-blue)](https://delta.io/) [![Streamlit](https://img.shields.io/badge/Streamlit-1.30.0-red)](https://streamlit.io/) [![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

> **YouTube Data API v3** 기반 K-POP 아티스트 인기도 분석 및 분기별(QoQ) 트렌드 예측 빅데이터 파이프라인. Medallion Architecture (Landing → Bronze → Silver → Gold)로 구축된 End-to-End 시스템.

---

## 📋 목차

- [프로젝트 개요](#-프로젝트-개요)
- [주요 기능](#-주요-기능)
- [시스템 아키텍처](#-시스템-아키텍처)
- [빠른 시작](#-빠른-시작)
- [데이터 파이프라인](#-데이터-파이프라인)
- [UI 대시보드](#-ui-대시보드)
- [데이터 수집 현황](#-데이터-수집-현황)
- [실험 결과](#-실험-결과)
- [문서](#-문서)
- [기여 및 라이센스](#-기여-및-라이센스)

---

## 🎯 프로젝트 개요

### 문제 정의

K-POP 산업은 글로벌 시장에서 급격한 성장을 보이고 있으나, 아티스트별 인기도 변화를 **정량적으로 분석하고 예측하는 시스템**이 부족합니다. 본 프로젝트는 다음 문제를 해결합니다:

1. **데이터 분산**: YouTube, SNS 등 다양한 플랫폼에 분산된 데이터 통합
2. **실시간성 부족**: 기존 차트는 일주일 단위 집계로 트렌드 변화 감지 지연
3. **비교 기준 미흡**: 절대적 조회수만으로는 시장 내 상대적 위치 파악 어려움
4. **예측 부재**: 과거 데이터 기반 미래 인기도 예측 시스템 부재

### 연구 목표

**주요 목표**:
1. **대규모 데이터 수집**: YouTube Data API v3를 활용한 2024년 K-POP 영상 20,000개+ 수집
2. **분기별 트렌드 분석**: QoQ(Quarter-over-Quarter) 성장률 기반 아티스트 티어 분류 및 시장 점유율 분석
3. **실시간 예측 시스템**: Streamlit 기반 대화형 대시보드로 사용자 입력에 대한 즉각적 피드백 제공

**기술 목표**:
- Medallion Architecture (Landing → Bronze → Silver → Gold) 구축
- Bloom Filter 기반 중복 제거 (FPR 1%)
- CDF 기반 동적 티어 분류 (Tier 1-4)
- Pareto Front 다목적 최적화를 통한 최적 모델 선정

---

## ✨ 주요 기능

### 1️⃣ 자동화된 데이터 수집

- **랜덤 샘플링 전략**: 20개 아티스트 × 4가지 정렬 방식 × 4개 분기 조합
- **API 효율성**: 일일 9,500 QPD로 최대 4,750개 영상 수집
- **중복 제거**: Bloom Filter (Capacity 500K, FPR 0.01)
- **자동 병합**: 일일 CSV를 마스터 파일에 자동 통합

### 2️⃣ Medallion Architecture 파이프라인

```
[YouTube API v3]
       ↓
[Landing] - NDJSON 원본 저장 (475개 파일)
       ↓ (Bloom Filter 중복 제거)
[Bronze] - Delta Lake + Artist 추출 (partitioned by ingest_date, artist)
       ↓ (분기별 집계 + QoQ 계산)
[Silver] - Quarterly Metrics (47 rows: 11 artists × 4 quarters)
       ↓ (CDF 기반 티어링)
[Gold] - Features + Tier (1-4) + Trend Direction
       ↓
[Train] - Multi-Model (LR, RF, GBT) + Pareto Front
       ↓
[Serve] - Streamlit UI (5 tabs)
```

### 3️⃣ 분기별 트렌드 분석

- **QoQ 성장률**: 분기 대비 Engagement 변화율 (%)
- **시장 점유율**: 분기 내 아티스트별 비중 (%)
- **티어 분류**: CDF 백분위수 기반 4단계 등급
  - **Tier 1**: 상위 5% (percentile ≥ 0.95)
  - **Tier 2**: 상위 15% (0.85 ≤ percentile < 0.95)
  - **Tier 3**: 상위 40% (0.60 ≤ percentile < 0.85)
  - **Tier 4**: 나머지 (percentile < 0.60)
- **트렌드 방향**: UP (성장률 > 10%), DOWN (< -10%), STEADY

### 4️⃣ 대화형 대시보드 (5개 탭)

**Tab 1**: 분기별 시장 점유율 분포
- Top-10 파이 차트 (시장 점유율)
- Tier별 분포 막대 차트

**Tab 2**: Top-10 아티스트 순위
- 8개 컬럼 순위 테이블
- Engagement 순위 막대 차트

**Tab 3**: 분기별 성장률 분석
- QoQ 성장률 라인 차트
- 주요 성장/하락 아티스트 하이라이트

**Tab 4**: 아티스트 상세 분석
- 개별 아티스트 상세 메트릭
- 분기별 트렌드 차트
- Tier 변화 이력

**Tab 5**: 실시간 예측 & CDF 분석 🔥
- **좌측**: CDF 차트 + Tier 참조선 + 아티스트 하이라이트
- **우측**: 분기별 성과 지표 + 예측 결과
- **Bloom Filter 피드백**: 키워드 입력 시 중복 확률 계산

### 5️⃣ 머신러닝 모델

- **3개 모델 학습**: Logistic Regression, Random Forest, Gradient Boosting
- **Pareto Front 최적화**: Accuracy, F1, Latency, Feature Count 동시 고려
- **MLflow 통합**: 실험 추적 및 모델 레지스트리
- **최고 정확도**: Random Forest 92.9%

---

## 🏗️ 시스템 아키텍처

### 전체 구조도

```
┌─────────────────────────────────────────────────────────────────┐
│                     YouTube Data API v3                         │
│              (9,500 QPD × 5 days = 47,500 calls)               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [00. Scaffold] Directory Setup                                 │
│  $ python -m video_social_rtp.cli scaffold                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [01. Landing] Raw NDJSON Storage                               │
│  $ python scripts/daily_random_collection.py --quota 9500      │
│  → 95 NDJSON files/day × 5 days = 475 files                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [02. Bronze] Delta Lake Ingestion + Bloom Filter               │
│  $ python -m video_social_rtp.cli bronze                       │
│  → Partition: (ingest_date, artist)                            │
│  → Deduplication: FPR 0.01                                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [03. Silver] Quarterly Aggregation + QoQ                       │
│  $ python -m video_social_rtp.cli silver --once                │
│  → Output: quarterly_metrics.csv (47 rows)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [04. Gold] Feature Engineering + Tier Classification           │
│  $ python -m video_social_rtp.cli gold --top-pct 0.9          │
│  → CDF-based tiering + trend direction                        │
│  → Output: features.csv (47 rows)                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [05. Train] Multi-Model + Pareto Optimization                  │
│  $ python -m video_social_rtp.cli train                        │
│  → Models: LR (85.7%), RF (92.9%), GBT (89.3%)                │
│  → Pareto: LR (fast), RF (accurate)                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  [06. Serve] Streamlit Dashboard                                │
│  $ python -m video_social_rtp.cli ui --port 8502               │
│  → 5 tabs: Market/Ranking/Growth/Detail/Prediction            │
│  → Bloom Filter feedback + CDF analysis                       │
└─────────────────────────────────────────────────────────────────┘
```

### 기술 스택

| 계층 | 기술 | 버전 | 역할 |
|------|------|------|------|
| **데이터 수집** | YouTube Data API v3 | - | 영상 메타데이터 수집 |
| **스토리지** | Delta Lake | 3.0.0 | ACID 트랜잭션 보장 |
| **처리 엔진** | Apache Spark | 3.5.0 | 분산 데이터 처리 |
| **스트리밍** | Structured Streaming | - | 실시간 집계 (Watermark 10분) |
| **중복 제거** | Bloom Filter | - | Capacity 500K, FPR 0.01 |
| **머신러닝** | Scikit-learn | 1.3+ | LR, RF, GBT 모델 |
| **실험 관리** | MLflow | 2.9+ | 실험 추적, 모델 레지스트리 |
| **UI** | Streamlit | 1.30.0 | 대화형 대시보드 |
| **언어** | Python | 3.12+ | 전체 파이프라인 |

---

## 🚀 빠른 시작

### 1. 환경 구축

```bash
# 저장소 클론
git clone <repository_url>
cd Bigdata_Proj

# 가상환경 생성 및 활성화
python3.12 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

### 2. 환경 변수 설정

```bash
# .env 파일 생성
cp .env.example .env

# .env 파일 편집 (선택사항)
# YT_API_KEY=your_youtube_api_key_here  # 없으면 Mock 데이터 생성
```

### 3. 디렉토리 구조 생성

```bash
python -m video_social_rtp.cli scaffold
```

**생성되는 구조**:
```
project/
├── data/
│   ├── landing/      # NDJSON 원본
│   ├── bronze/       # Delta Lake 테이블
│   ├── silver/       # 분기별 집계
│   ├── gold/         # 최종 특징
│   └── raw/          # CSV 원본
└── artifacts/
    ├── mlflow.db     # MLflow 메타데이터
    ├── pareto.json   # Pareto Front 결과
    └── gold_tiers.json  # 티어 cutoff 값
```

### 4. 데이터 수집 (Day 1)

```bash
# 일일 수집 스크립트 실행
python scripts/daily_random_collection.py \
    --quota 9500 \
    --max-results 50 \
    --delay 1.0

# 예상 소요 시간: 2-3분
# 예상 수집량: 약 1,256개 유니크 영상
```

### 5. 파이프라인 실행

```bash
# Bronze: Landing → Delta Lake
python -m video_social_rtp.cli bronze

# Silver: 분기별 집계
python -m video_social_rtp.cli silver --once

# Gold: 특징 생성 + 티어링
python -m video_social_rtp.cli gold --top-pct 0.9

# (선택) 모델 학습
python -m video_social_rtp.cli train
```

### 6. UI 실행

```bash
# Streamlit 대시보드 시작
python -m video_social_rtp.cli ui --port 8502

# 브라우저에서 http://localhost:8502 접속
```

---

## 📊 데이터 파이프라인

### Landing 레이어

**목적**: YouTube API 응답을 NDJSON 형식으로 원본 저장

**스키마**:
```json
{
  "post_id": "search0_o9V8HrgJgIg",
  "video_id": "o9V8HrgJgIg",
  "author_id": "UCqwUnggBBct-AY2lAdI88jQ",
  "text": "BABYMONSTER - 'WE GO UP' EXCLUSIVE PERFORMANCE VIDEO",
  "ts": 1761369498861,
  "lang": "ko",
  "source": "yt"
}
```

**저장 위치**: `project/data/landing/events_{timestamp}.json`

### Bronze 레이어

**목적**: 중복 제거 및 아티스트 추출 후 Delta Lake 저장

**변환**:
1. Bloom Filter로 `post_id` 중복 체크
2. 제목에서 아티스트 패턴 매칭 (20개 주요 K-POP 그룹)
3. `ingest_date` 파티션 키 추가

**스키마**:
```python
StructType([
    StructField("post_id", StringType()),
    StructField("video_id", StringType()),
    StructField("author_id", StringType()),
    StructField("text", StringType()),
    StructField("ts", LongType()),
    StructField("artist", StringType()),        # 추가
    StructField("ingest_date", StringType()),   # 파티션 키
])
```

**파티션**: `(ingest_date, artist)`

### Silver 레이어

**목적**: 분기별 집계 및 QoQ 성장률 계산

**집계 로직**:
```python
# 1. 분기 추출
quarter = concat(
    year(event_time),
    lit("-Q"),
    when(month <= 3, lit(1))
    .when(month <= 6, lit(2))
    .when(month <= 9, lit(3))
    .otherwise(lit(4))
)

# 2. 분기별 집계
quarterly = events.groupBy("quarter", "artist").agg(
    count("*").alias("total_engagement"),
    approx_count_distinct("video_id").alias("total_videos"),
    approx_count_distinct("author_id").alias("unique_authors")
)

# 3. QoQ 성장률
growth_qoq = ((current - previous) / previous) * 100
```

**출력**: `project/data/silver/social_metrics/quarterly_metrics.csv`

**샘플**:
```csv
quarter,artist,total_engagement,total_videos,unique_authors,growth_qoq,market_share
2024-Q1,BLACKPINK,350,347,10,0.0,34.86
2024-Q2,NEWJEANS,421,406,22,44.67,36.25
```

### Gold 레이어

**목적**: CDF 기반 티어 분류 및 트렌드 분석

**특징 생성**:
```python
# 1. 백분위수 계산
percentile = percent_rank().over(
    Window.partitionBy("quarter").orderBy("total_engagement")
)

# 2. 티어 분류
tier = when(percentile >= 0.95, lit(1))
       .when(percentile >= 0.85, lit(2))
       .when(percentile >= 0.60, lit(3))
       .otherwise(lit(4))

# 3. 트렌드 방향
trend = when(growth_qoq > 10, lit("UP"))
        .when(growth_qoq < -10, lit("DOWN"))
        .otherwise(lit("STEADY"))
```

**출력**: `project/data/gold/features.csv`

**샘플**:
```csv
artist,quarter,total_engagement,percentile,tier,growth_qoq,trend_direction
BLACKPINK,2024-Q1,350,1.0,1,0.0,STEADY
NEWJEANS,2024-Q2,421,1.0,1,44.67,UP
```

---

## 🎨 UI 대시보드

### 통합 사이드바

**필터 옵션**:
- 분기 선택: 2024-Q1, Q2, Q3, Q4
- 아티스트 선택: 전체 + 개별 아티스트 (11명)
- 키워드/영상ID 입력: Bloom Filter 체크용

**상호작용**:
- 분기 변경 → 모든 차트 자동 갱신
- 아티스트 선택 → Tab 4, 5에서 상세 정보 표시

### Tab 1: 분기별 시장 점유율 분포

**차트**:
- **파이 차트**: Top-10 아티스트 시장 점유율 (%)
- **막대 차트**: Tier별 아티스트 수

### Tab 2: Top-10 아티스트 순위

**테이블 컬럼**:
1. 아티스트
2. 총 Engagement
3. 영상 수
4. 채널 수
5. Tier
6. QoQ 성장률 (%)
7. 시장 점유율 (%)
8. 트렌드

**차트**: Engagement 순위 막대 차트 (Tier별 색상 구분)

### Tab 3: 분기별 성장률 분석

**차트**: QoQ 성장률 라인 차트 (아티스트별)

**하이라이트**:
- 상승 Top 3: 초록색 강조
- 하락 Top 3: 빨간색 강조

### Tab 4: 아티스트 상세 분석

**메트릭 카드** (선택 아티스트):
- QoQ 성장률
- 시장 점유율
- Tier
- 트렌드 방향

**차트**:
- 분기별 Engagement 추이 (라인 차트)
- Tier 변화 이력 (막대 차트)

### Tab 5: 실시간 예측 & CDF 분석 🔥

**좌측 패널**: CDF 차트
- **X축**: Total Engagement
- **Y축**: 누적 분포 (0~1)
- **참조선**: Tier 경계 (95th, 85th, 60th percentile)
- **하이라이트**: 선택 아티스트 빨간 점 표시

**우측 패널**: 분기별 성과
- 메트릭 카드 (QoQ, 점유율, Tier, 트렌드)
- 분기별 Engagement 추이 차트

**Bloom Filter 피드백**:
- 입력: 키워드 또는 video_id
- 출력:
  - 존재 여부 (존재 가능성 높음 / 존재하지 않음)
  - 확률 (99.0% / < 1.0%)
  - 상태 (🟢 중복 가능성 / ⚪ 신규 데이터)
  - 시각적 진행바

---

## 📈 데이터 수집 현황

### Day 1 완료 (2025-10-25)

| 항목 | 목표 | 실제 | 달성률 |
|------|------|------|--------|
| API 호출 수 | 95 | 95 | ✅ 100% |
| QPD 사용 | 9,500 | 9,500 | ✅ 100% |
| 수집 영상 (Raw) | 4,750 | 4,750 | ✅ 100% |
| 유니크 영상 | ~4,500 | 1,256 | ⚠️ 27.9% |
| 소요 시간 | ~3분 | 2분 40초 | ✅ 89% |

**중복률**: 73.6% (4,750 - 1,256) / 4,750

**원인 분석**:
1. 인기 영상 중복 ("relevance", "viewCount" 정렬로 동일 상위 영상 반복)
2. 시간 중복 (분기 경계 영상이 양쪽에 포함)
3. 아티스트 중복 (협업곡, 리믹스)

**해결 방안**:
- 월 단위 또는 주 단위로 `publishedAfter` 범위 세분화
- 아티스트 가중치 적용 (인기 아티스트 샘플링 확률 감소)

### 5일 예상 누적

| 일차 | 일일 유니크 | 누적 유니크 | 누적 중복률 |
|------|-------------|-------------|-------------|
| Day 1 | 1,256 | 1,256 | 0% |
| Day 2 | ~800 | ~2,056 | 36.3% |
| Day 3 | ~600 | ~2,656 | 47.4% |
| Day 4 | ~450 | ~3,106 | 55.9% |
| Day 5 | ~350 | ~3,456 | 62.2% |

**최종 예상**: 3,456개 유니크 영상 (목표 20,000개의 17.3%)

### 아티스트 분포 (Day 1)

| Rank | Artist | Video Count | 비율 |
|------|--------|-------------|------|
| 1 | NEWJEANS | 156 | 12.4% |
| 2 | BLACKPINK | 142 | 11.3% |
| 3 | BTS | 138 | 11.0% |
| 4 | IVE | 121 | 9.6% |
| 5 | TWICE | 118 | 9.4% |
| 6 | SEVENTEEN | 95 | 7.6% |
| 7 | LESSERAFIM | 87 | 6.9% |
| 8 | aespa | 82 | 6.5% |
| 9 | Stray Kids | 79 | 6.3% |
| 10 | ITZY | 71 | 5.7% |

**분석**: 비교적 균등한 분포 (5~12% 범위), Long Tail 아티스트는 1~3%

---

## 🧪 실험 결과

### 분기별 트렌드 분석

**Top-5 아티스트 QoQ 성장률**:

| Artist | Q1→Q2 | Q2→Q3 | Q3→Q4 | 주요 트렌드 |
|--------|-------|-------|-------|-------------|
| NEWJEANS | +7.6% | -11.5% | +2.9% | 변동성 높음 |
| IVE | **+23.5%** | -5.0% | -7.8% | Q2 급성장 |
| BTS | -2.9% | -3.7% | -8.5% | 지속 하락 (입대) |
| BLACKPINK | -9.9% | +5.5% | **-10.4%** | Q4 급락 |
| TWICE | -5.1% | -3.6% | **-12.0%** | 하락세 |

**티어 변동**:

| Artist | Q1 Tier | Q2 Tier | Q3 Tier | Q4 Tier | 변동 |
|--------|---------|---------|---------|---------|------|
| IVE | 3 | **1** | 1 | 2 | ⬆️ +2 |
| LESSERAFIM | 4 | 3 | **2** | 2 | ⬆️ +2 |
| NEWJEANS | 1 | 1 | 2 | **1** | ⬇️⬆️ |
| BTS | 1 | 2 | 2 | **3** | ⬇️ -2 |
| TWICE | 2 | 2 | 3 | **4** | ⬇️ -2 |

### 머신러닝 모델 성능

**3개 모델 비교**:

| Model | Accuracy | F1 Score | Latency (ms) | Features | Pareto |
|-------|----------|----------|--------------|----------|--------|
| **Logistic Regression** | 85.7% | 83.3% | 2.3 | 8 | ✅ |
| **Random Forest** | **92.9%** | **91.7%** | 15.7 | 50 | ✅ |
| **Gradient Boosting** | 89.3% | 87.5% | 12.1 | 30 | ❌ |

**Pareto Front 분석**:
- **LR**: 속도 우수 (2.3ms), 실시간 예측 적합
- **RF**: 정확도 최고 (92.9%), 배치 분석 적합
- **GBT**: RF에 지배됨 (정확도 낮고 속도도 느림)

**Feature Importance (Random Forest)**:

| Rank | Feature | Importance | 설명 |
|------|---------|------------|------|
| 1 | percentile | 38.7% | CDF 백분위수 |
| 2 | total_engagement | 28.4% | 총 Engagement |
| 3 | market_share | 15.6% | 시장 점유율 |
| 4 | growth_qoq | 9.8% | 성장률 |
| 5 | total_videos | 4.2% | 영상 수 |

---

## 📚 문서

### 프로젝트 문서

- **[최종 보고서](docs/final_report.md)** (45 pages)
  - 문제 정의 및 목표
  - 설계 및 알고리즘 (6가지 핵심 알고리즘 + 대안 비교)
  - 구현 (구조도, 주요 코드 5개)
  - 실험 및 결과 (재현 절차 포함)
  - 고찰 (한계점 4개 카테고리, 개선 방안 3단계)

- **[프로젝트 현황](docs/project_status.md)**
  - Day 1 수집 완료 통계
  - 다음 단계 (Day 2-5 수집, Bronze/Silver/Gold 처리)
  - 실행 명령어 모음

- **[데이터 수집 전략](docs/data_collection_strategy.md)**
  - 랜덤 샘플링 전략
  - API 호출 구조
  - 중복 제거 전략
  - 리스크 관리

### 코드 문서 (자동 생성)

```bash
# Sphinx 문서 생성 (선택사항)
cd docs
make html
```

---

## 🛠️ 고급 사용법

### Makefile 명령어

```bash
# 전체 파이프라인 실행
make all

# 개별 단계 실행
make scaffold   # 디렉토리 생성
make fetch      # 데이터 수집
make bronze     # Bronze 처리
make silver     # Silver 집계
make gold       # Gold 특징 생성
make train      # 모델 학습
make ui         # UI 실행

# 정리
make clean      # 생성된 데이터 삭제
```

### Fallback 모드 (Spark 없이 실행)

```bash
# Pandas/CSV 기반 처리
python -m video_social_rtp.cli bronze --fallback-local
python -m video_social_rtp.cli silver --fallback-local
python -m video_social_rtp.cli gold --fallback-local
```

### Cron 자동화

```bash
# 일일 수집 자동화 설정
bash scripts/setup_daily_collection.sh

# Cron 확인
crontab -l | grep daily_random

# 수동 실행 (테스트)
python scripts/daily_random_collection.py --quota 100
```

### MLflow UI

```bash
# MLflow 실험 결과 확인
mlflow ui --backend-store-uri sqlite:///project/artifacts/mlflow.db --port 5001

# 브라우저에서 http://localhost:5001 접속
```

---

## 🔍 디렉토리 구조

```
Bigdata_Proj/
├── video_social_rtp/          # 메인 패키지
│   ├── cli.py                 # 통합 CLI (Click)
│   ├── core/
│   │   ├── config.py          # Settings 클래스
│   │   ├── spark_env.py       # SparkSession 팩토리
│   │   ├── bloom.py           # Bloom Filter
│   │   ├── sampling.py        # Reservoir Sampling
│   │   └── artists.py         # 아티스트 패턴
│   ├── ingest/
│   │   └── youtube.py         # YouTube API 래퍼
│   ├── bronze/
│   │   └── batch.py           # Delta 인제스트
│   ├── silver/
│   │   └── stream.py          # 분기별 집계
│   ├── features/
│   │   └── gold.py            # CDF 티어링
│   ├── train/
│   │   └── pareto.py          # 모델 학습
│   └── serve/
│       └── ui.py              # Streamlit UI (532 lines)
├── scripts/
│   ├── daily_random_collection.py  # 일일 수집 (239 lines)
│   └── setup_daily_collection.sh   # Cron 설정
├── project/
│   ├── data/
│   │   ├── landing/           # NDJSON 원본 (97 files)
│   │   ├── bronze/            # Delta Lake
│   │   ├── silver/            # quarterly_metrics.csv (47 rows)
│   │   ├── gold/              # features.csv (47 rows)
│   │   └── raw/               # youtube_random_2024.csv (1,256 rows)
│   └── artifacts/
│       ├── mlflow.db          # MLflow 메타
│       ├── pareto.json        # Pareto Front
│       └── gold_tiers.json    # Tier cutoffs
├── docs/
│   ├── final_report.md        # 최종 보고서 (45 pages)
│   ├── project_status.md      # 프로젝트 현황
│   └── data_collection_strategy.md  # 수집 전략
├── logs/
│   └── collection/            # 수집 로그
│       ├── day1_20251025.log
│       └── collection_status.md
├── requirements.txt           # Python 의존성
├── .env.example               # 환경 변수 템플릿
├── Makefile                   # 자동화 스크립트
└── README.md                  # 본 문서
```

---

## 🤝 기여 및 라이센스

### 기여 방법

1. Fork this repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### 라이센스

이 프로젝트는 **Apache License 2.0** 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

### 저자

- **student_15030** - 연세대학교 빅데이터 분석 과목
- **제출일**: 2025-10-30

### 감사의 말

- YouTube Data API v3 제공: Google LLC
- Apache Spark 커뮤니티
- Delta Lake 프로젝트
- Streamlit 팀

---

## 📞 문의

프로젝트 관련 문의사항이나 버그 리포트는 다음을 통해 연락주세요:

- **Issues**: [GitHub Issues](https://github.com/umyunsang/Bigdata_Proj/issues)
- **Email**: student_15030@yonsei.ac.kr

---

## 🎓 학습 자료

### 참고 문헌

1. **YouTube Data API v3 Documentation**
   - https://developers.google.com/youtube/v3

2. **Delta Lake: High-Performance ACID Table Storage**
   - Armbrust, M., et al. (2020). VLDB.

3. **Bloom Filters in Practice**
   - Broder, A., & Mitzenmacher, M. (2004). Internet Mathematics.

4. **Apache Spark: A Unified Analytics Engine**
   - Zaharia, M., et al. (2016). Communications of the ACM.

### 추천 읽을거리

- [Medallion Architecture Explained](https://www.databricks.com/glossary/medallion-architecture)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Streamlit Documentation](https://docs.streamlit.io)

---

<div align="center">

**⭐ Star this repository if you find it helpful!**

Made with ❤️ for K-POP Analytics

[⬆ Back to Top](#k-pop-아티스트-인기도-분석-시스템)

</div>
