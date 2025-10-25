# TODO: 성장률 계산 문제 해결

## 문제 요약

1년치 데이터(2024-01-01 ~ 2024-10-24, 3,574개 영상)를 수집했으나, **성장률(growth_rate_7d, growth_rate_30d)이 모두 0.0**으로 나타남.

## 근본 원인

### Bronze 데이터: ✅ 정상
```
타임스탬프 범위: 2024-01-01 12:00:38 ~ 2024-10-25 06:37:34
총 행 수: 3,574개
고유 날짜: 298일
아티스트: BLACKPINK, BTS, NEWJEANS, OTHER
```

확인 명령:
```bash
.venv/bin/python -c "
from pyspark.sql import SparkSession
from pathlib import Path
import sys
from datetime import datetime

PROJECT_ROOT = Path.cwd()
sys.path.insert(0, str(PROJECT_ROOT))

from video_social_rtp.core.spark_env import get_spark_session
from video_social_rtp.core.config import load_settings

s = load_settings()
spark, _ = get_spark_session('check_bronze', s)

bronze_df = spark.read.format('delta').load(str(s.bronze_dir))
time_stats = bronze_df.selectExpr('min(ts) as min_ts', 'max(ts) as max_ts', 'count(*) as total').collect()[0]
min_dt = datetime.fromtimestamp(time_stats.min_ts / 1000)
max_dt = datetime.fromtimestamp(time_stats.max_ts / 1000)

print(f'Bronze 타임스탬프 범위:')
print(f'  최소: {min_dt}')
print(f'  최대: {max_dt}')
print(f'  총 행: {time_stats.total}')

spark.stop()
"
```

### Silver 데이터: ❌ 문제 발견
```
타임스탬프 범위: 2025-10-16 14:35:00 ~ 2025-10-16 16:35:00
총 윈도우: 72개
문제: 모든 윈도우가 2시간 내에만 존재!
```

확인 명령:
```bash
.venv/bin/python -c "
from pyspark.sql import SparkSession
from pathlib import Path
import sys
from datetime import datetime

PROJECT_ROOT = Path.cwd()
sys.path.insert(0, str(PROJECT_ROOT))

from video_social_rtp.core.spark_env import get_spark_session
from video_social_rtp.core.config import load_settings

s = load_settings()
spark, _ = get_spark_session('check_silver', s)

silver_path = Path(s.silver_dir) / 'social_metrics'
silver_df = spark.read.format('parquet').load(str(silver_path))

time_range = silver_df.selectExpr('min(window_start_ts) as min_time', 'max(window_end_ts) as max_time').collect()[0]
min_dt = datetime.fromtimestamp(time_range.min_time / 1000)
max_dt = datetime.fromtimestamp(time_range.max_time / 1000)

print(f'Silver 타임스탬프 범위:')
print(f'  최소: {min_dt}')
print(f'  최대: {max_dt}')
print(f'  총 행: {silver_df.count()}')

spark.stop()
"
```

### Gold 데이터: ❌ 결과적으로 문제
```
성장률 계산: growth_rate_7d = 0.0, growth_rate_30d = 0.0
트렌드 방향: 모두 "STEADY" (RISING/FALLING이어야 함)
```

## 문제의 원인

### Structured Streaming 윈도우 문제

**현재 구조 (video_social_rtp/silver/stream.py:134-162):**
```python
raw_stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(str(s.landing_dir))  # Landing JSON 파일 읽기
)

events = (
    raw_stream
    .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000)))
    .withWatermark("event_time", params.watermark)  # "10 minutes"
)

aggregated = (
    events.groupBy(
        F.window("event_time", params.window_size, params.window_slide),  # 1 hour window, 5 min slide
        F.col("artist"),
    )
    .agg(...)
)
```

**문제점:**
1. `--once` 모드로 실행하면 모든 데이터를 한 번에 처리
2. Structured Streaming의 윈도우는 **스트림 처리 시점** 기준으로 생성됨
3. 과거 1년 데이터를 한 번에 읽으면 모든 이벤트가 "지금" 처리되는 것으로 간주
4. 결과: 모든 윈도우가 2시간(2025-10-16 14:35 ~ 16:35) 내에만 생성됨

**Silver가 Bronze 대신 Landing을 읽는 이유:**
- Medallion 아키텍처: Landing → Bronze → Silver → Gold
- Silver는 "실시간 스트리밍" 단계로 설계됨
- 하지만 **배치 과거 데이터 분석**에는 맞지 않음!

## 해결 방안

### 옵션 1: Silver를 Bronze 배치 읽기로 수정 (권장)

**목표:** Silver가 Bronze Delta 테이블을 배치 모드로 읽어서 이벤트 시간 기준 윈도우 생성

**수정 파일:** `video_social_rtp/silver/stream.py`

**변경 사항:**
```python
# 기존 (Streaming from Landing)
raw_stream = spark.readStream.json(str(s.landing_dir))

# 변경 (Batch from Bronze)
bronze_df = spark.read.format('delta').load(str(s.bronze_dir))

# 이벤트 시간 기준 윈도우 생성
events = (
    bronze_df
    .dropna(subset=["post_id", "video_id", "ts"])
    .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000)))
    .dropDuplicates(["post_id"])
)

aggregated = (
    events.groupBy(
        F.window("event_time", "1 day"),  # 일 단위 윈도우로 변경
        F.col("artist"),
    )
    .agg(
        F.count("*").alias("engagement_count"),
        F.approx_count_distinct("video_id").alias("unique_videos"),
        F.approx_count_distinct("author_id").alias("unique_authors"),
    )
)

# 배치 쓰기
aggregated.write.format('delta').mode('overwrite').save(str(silver_path))
```

**장점:**
- Bronze의 정확한 타임스탬프 활용
- 일 단위 윈도우로 298일 데이터가 298개 윈도우로 분산
- 성장률 계산 가능 (7일/30일 간격 비교)

**단점:**
- 실시간 스트리밍 파이프라인 구조 변경

### 옵션 2: Gold에서 Bronze 직접 집계 (더 간단)

**목표:** Gold 단계에서 Silver를 우회하고 Bronze에서 직접 일별 집계

**수정 파일:** `video_social_rtp/features/gold.py`

**변경 사항:**
```python
# Gold에서 Bronze 직접 읽기
bronze_df = spark.read.format('delta').load(str(s.bronze_dir))

# 일별 집계 생성
daily_metrics = (
    bronze_df
    .withColumn("date", F.to_date(F.from_unixtime(F.col("ts") / 1000)))
    .groupBy("date", "artist")
    .agg(
        F.count("*").alias("engagement_count"),
        F.countDistinct("video_id").alias("unique_videos"),
        F.approx_count_distinct("author_id").alias("unique_authors"),
    )
)

# 7일/30일 성장률 계산
# (기존 로직 활용, daily_metrics에서 계산)
```

**장점:**
- Silver 구조 변경 불필요
- 과거 데이터 분석에 최적화
- 실시간 스트리밍과 배치 분석 분리

**단점:**
- Silver가 과거 분석에는 사용되지 않음

### 옵션 3: Silver 실행 방식 변경

**목표:** 실시간 스트리밍 대신 배치 처리 플래그 추가

**변경 사항:**
```python
# CLI에 --batch-mode 플래그 추가
python -m video_social_rtp.cli silver --batch-mode

# batch-mode일 때 Bronze에서 읽기
if params.batch_mode:
    df = spark.read.format('delta').load(str(s.bronze_dir))
else:
    df = spark.readStream.json(str(s.landing_dir))
```

## 실행 계획

### 1단계: Silver 또는 Gold 수정

선택한 옵션에 따라 코드 수정

### 2단계: 데이터 재처리

```bash
# Silver/Gold 데이터 삭제
rm -rf project/data/silver/*
rm -rf project/data/gold/*
rm -rf project/chk/silver/*

# 파이프라인 재실행
python -m video_social_rtp.cli silver --once  # 또는 수정된 명령
python -m video_social_rtp.cli gold --top-pct 0.9
```

### 3단계: 결과 검증

```bash
# 분석 실행
python examples/quick_analysis.py
```

**기대 결과:**
- `growth_rate_7d`: 0이 아닌 값 (양수 또는 음수)
- `growth_rate_30d`: 0이 아닌 값
- `trend_direction`: "RISING", "FALLING", "STEADY" 혼합
- `momentum`: 0이 아닌 값

### 4단계: 시각화 확인

```bash
# Streamlit UI 실행
python -m video_social_rtp.cli ui --port 8501
```

**확인 항목:**
- CDF/PDF 차트에서 성장률 분포 확인
- Top-K 테이블에서 트렌드 방향 확인
- 시계열 차트에서 시간별 변화 확인

## 현재 상태

### 수집된 데이터
- ✅ Landing: 3,574개 이벤트 (2024-01-01 ~ 2024-10-24)
- ✅ Bronze: 3,574개 행 (타임스탬프 정상)
- ❌ Silver: 72개 윈도우 (2시간 내에만 존재)
- ❌ Gold: 3명 아티스트, 성장률 모두 0

### 파일 위치
```
scripts/collect_yearly_data.py  # 1년치 데이터 수집 스크립트
video_social_rtp/silver/stream.py  # 수정 필요: Silver 스트리밍
video_social_rtp/features/gold.py  # 또는 여기 수정: Gold 집계
examples/quick_analysis.py  # 검증용 분석 스크립트
```

### 핵심 코드 위치

**Silver 윈도우 생성:**
- `video_social_rtp/silver/stream.py:134-162`

**Gold 성장률 계산:**
- `video_social_rtp/features/gold.py:187-200`

```python
# 성장률 계산 로직
features_df = features_df.withColumn(
    "growth_rate_7d",
    F.when(F.col("prev_7_engagement") <= 0, F.lit(0.0)).otherwise(
        (F.col("recent_7_engagement") - F.col("prev_7_engagement")) / F.col("prev_7_engagement") * 100.0
    ),
)
```

## 참고 자료

### 관련 문서
- `docs/데이터_흐름_및_분석_가이드.md`: 전체 파이프라인 설명
- `CLAUDE.md`: 프로젝트 구조 및 명령어
- `examples/quick_analysis.py`: 성장률 검증 스크립트

### 실행 로그 확인
```bash
# Bronze 로그
cat project/logs/bronze_*.log | grep "bronze_rows"

# Silver 로그
cat project/logs/silver_*.log | grep "silver_stream"

# Gold 로그
cat project/logs/gold_*.log | grep "gold_delta"
```

## 우선순위

**High Priority:**
1. ✅ 문제 진단 완료
2. ⏳ 솔루션 선택 (옵션 1 또는 2)
3. ⏳ 코드 수정
4. ⏳ 파이프라인 재실행
5. ⏳ 성장률 검증

**Next Steps:**
1. 옵션 1 또는 2 중 선택
2. 해당 파일 수정
3. 데이터 재처리 실행

## 예상 결과 (수정 후)

### Silver 데이터
```
타임스탬프 범위: 2024-01-01 ~ 2024-10-24
윈도우 수: ~298개 (일 단위) 또는 ~6,000개 (1시간 윈도우)
아티스트별 시계열 데이터 생성
```

### Gold 데이터
```
BLACKPINK:
  - growth_rate_7d: 15.3% (예시)
  - growth_rate_30d: 8.7% (예시)
  - trend_direction: "RISING"
  - momentum: 6.6%

BTS:
  - growth_rate_7d: -2.1% (예시)
  - growth_rate_30d: 5.2% (예시)
  - trend_direction: "FALLING"
  - momentum: -7.3%
```

---

**마지막 업데이트:** 2025-10-24
**작성자:** Claude Code
**상태:** 조사 완료, 솔루션 대기 중
