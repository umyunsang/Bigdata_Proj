# Video/Social Realtime Pipeline — K-POP Analytics Snippets

K-POP YouTube 분석을 위한 빅데이터 파이프라인 스니펫 모음.
YouTube Data API v3를 통해 데이터를 수집하고, Spark + Delta Lake 기반 멀티 스테이지 파이프라인으로 처리합니다.

## 주요 기능

- **YouTube API 연동**: 실제 YouTube 데이터 수집 (API 키 없으면 mock 데이터)
- **아티스트 추출**: 30+ K-POP 아티스트 패턴 인식 (한글/영어/스타일라이즈드)
- **Bloom Filter**: 중복 제거를 위한 확률적 자료구조
- **Reservoir Sampling**: Vitter's Algorithm R 기반 대표 샘플링
- **HyperLogLog**: 유니크 유저 추정 (approx_count_distinct)
- **CDF 기반 티어링**: 동적 퍼센타일 기반 Tier 1-4 분류
- **Pareto Front**: 다중 목표 최적화로 최적 모델 선택
- **MLflow**: 실험 추적 및 모델 버저닝

## 파이프라인 아키텍처

```
YouTube API v3
    ↓
[00. Fetch] - NDJSON Landing (Reservoir Sampling)
    ↓
[10. Bronze] - Delta Lake 적재 (Bloom Filter + Artist Extraction)
    ↓
[20. Silver] - Structured Streaming (Window + Watermark + HLL)
    ↓
[30. Gold] - Feature Engineering (CDF Tier Labeling)
    ↓
[40. Train] - Multi-model Training (Pareto Front + MLflow)
    ↓
[50. Predict] - Predictions (Rule-based / ML Model)
```

## 디렉토리 구조

```
video-social-rtp-snippets/
├── jobs/                        # 파이프라인 스크립트
│   ├── 00_fetch_to_landing.py   # YouTube → Landing (NDJSON)
│   ├── 10_bronze_batch.py       # Landing → Bronze (Delta)
│   ├── 20_silver_stream.py      # Bronze → Silver (Streaming)
│   ├── 30_gold_features.py      # Silver → Gold (Features)
│   ├── 40_train_pareto.py       # Gold → Train (Models)
│   └── 50_predict_stream.py     # Predictions
├── conf/                        # 설정 및 유틸리티
│   ├── .env.example             # 환경변수 예제
│   ├── artists.py               # K-POP 아티스트 패턴
│   └── utils.py                 # 공통 유틸리티
├── app/
│   └── app.py                   # Streamlit UI
├── data/                        # 데이터 (자동 생성)
│   ├── landing/                 # Raw JSON
│   ├── bronze/                  # Delta Lake
│   ├── silver/                  # Delta Lake
│   ├── gold/                    # Features
│   └── artifacts/               # Models, Pareto, Bloom
└── chk/                         # Spark Checkpoints
```

## 빠른 시작

### 1. 환경 설정

```bash
cd video-social-rtp-snippets

# 가상환경 생성
python -m venv .venv
source .venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정 (선택)
cp conf/.env.example conf/.env
# YT_API_KEY 설정 (없으면 mock 데이터 사용)
```

### 2. 파이프라인 실행

```bash
# 00. 디렉토리 생성
python scaffold_paths.py

# 01. YouTube 데이터 수집
python jobs/00_fetch_to_landing.py --query "NewJeans" --max-items 50 --region KR

# 02. Bronze 적재 (Spark 필요)
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/10_bronze_batch.py

# 03. Silver 스트리밍 (배치 모드)
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/20_silver_stream.py --batch

# 04. Gold 피처 생성
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/30_gold_features.py --top-pct 0.9

# 05. 모델 학습
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/40_train_pareto.py

# 06. 예측 생성
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/50_predict_stream.py
```

### 3. Streamlit UI

```bash
streamlit run app/app.py
```

## 스니펫 상세

### 00_fetch_to_landing.py
- YouTube Data API v3 연동 (google-api-python-client)
- API 키 없으면 자동으로 mock 데이터 생성
- Reservoir Sampling으로 대표 샘플 추출
- 마커 파일로 중복 fetch 방지

```bash
python jobs/00_fetch_to_landing.py --query "BTS" --max-items 50 --region KR --lang ko
```

### 10_bronze_batch.py
- Landing JSON → Bronze Delta Lake
- 아티스트 추출 (30+ K-POP 아티스트 패턴)
- Bloom Filter로 최근 7일 중복 제거
- ingest_date, artist로 파티셔닝

### 20_silver_stream.py
- Structured Streaming (file source)
- Sliding Window (1h window, 5min slide)
- Watermark (10 minutes)
- HyperLogLog로 유니크 유저 추정
- 배치 모드 (`--batch`) 지원

### 30_gold_features.py
- 아티스트별 피처 집계
- CDF 기반 동적 티어 분류 (Tier 1-4)
- 트렌드 분석 (growth rate, momentum, volatility)
- Delta + CSV 이중 출력

### 40_train_pareto.py
- 다중 모델 학습 (LR, RF, GBT)
- Pareto Front로 비지배 해 선택
- MLflow 실험 추적 (선택적)

### 50_predict_stream.py
- 학습된 모델 또는 Rule-based 예측
- 바이럴 잠재력 분류 (HIGH/MEDIUM/LOW)
- Delta 테이블 + CSV 출력

## 지원 아티스트

BTS, BLACKPINK, TWICE, NEWJEANS, AESPA, LESSERAFIM, IVE, ITZY, NMIXX, SEVENTEEN, NCT, STRAYKIDS, ATEEZ, ENHYPEN, GOT7, BIGBANG, EXO, 소녀시대, 2NE1, SHINEE, BABYMONSTER, RIIZE, ZEROBASEONE 등

## 환경 변수

| 변수 | 설명 | 기본값 |
|------|------|--------|
| YT_API_KEY | YouTube API 키 | (mock 모드) |
| LANDING_DIR | Landing 경로 | ./data/landing |
| BRONZE_DIR | Bronze 경로 | ./data/bronze |
| SILVER_DIR | Silver 경로 | ./data/silver |
| GOLD_DIR | Gold 경로 | ./data/gold |
| TOP_PCT | CDF 티어 커트오프 | 0.9 |

## 의존성

- Python 3.9+
- PySpark 3.5+
- Delta Lake 3.2+
- google-api-python-client (선택)
- mlflow (선택)
- streamlit (선택)
