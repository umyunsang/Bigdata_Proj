# 01. 데이터 수집 및 배치 ETL 설계

## 1. 개요

### 1.1 목표
- YouTube API v3를 통한 대용량 비디오 데이터 수집
- Reservoir Sampling을 활용한 효율적인 데이터 샘플링
- Bloom Filter 기반 중복 데이터 필터링
- Bronze Layer 데이터 레이크 구축

### 1.2 핵심 요구사항
- **데이터 수집량**: 일일 100만 건 이상 비디오 메타데이터
- **처리 지연시간**: 배치당 5분 이내 처리 완료
- **데이터 품질**: 99.9% 이상 데이터 무결성 보장
- **확장성**: 수평 확장 가능한 아키텍처

## 2. 데이터 소스 분석

### 2.1 YouTube API v3 엔드포인트

#### 2.1.1 주요 API 엔드포인트
```python
# 비디오 검색 API
SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"
COMMENT_URL = "https://www.googleapis.com/youtube/v3/commentThreads"
```

#### 2.1.2 수집 데이터 스키마
```json
{
  "video_id": "string",
  "title": "string",
  "description": "string", 
  "channel_id": "string",
  "channel_title": "string",
  "published_at": "datetime",
  "duration": "string",
  "view_count": "integer",
  "like_count": "integer", 
  "dislike_count": "integer",
  "comment_count": "integer",
  "category_id": "integer",
  "tags": ["string"],
  "thumbnails": {
    "default": {"url": "string", "width": "integer", "height": "integer"},
    "medium": {"url": "string", "width": "integer", "height": "integer"},
    "high": {"url": "string", "width": "integer", "height": "integer"}
  }
}
```

### 2.2 API 제한사항 및 대응 전략

#### 2.2.1 할당량 제한
- **일일 할당량**: 10,000 units/day
- **초당 요청 제한**: 100 requests/second
- **대응 전략**: 
  - 배치 크기 조절 (50개씩 처리)
  - 요청 간격 조절 (1초 간격)
  - 멀티 API 키 로테이션

#### 2.2.2 데이터 품질 보장
- **중복 데이터**: video_id 기반 중복 제거
- **누락 데이터**: 재시도 로직 및 에러 핸들링
- **데이터 검증**: 스키마 검증 및 타입 체크

## 3. Reservoir Sampling 설계

### 3.1 알고리즘 개요

#### 3.1.1 Vitter's Algorithm R
```python
class ReservoirSampler:
    def __init__(self, k: int):
        self.k = k  # 샘플 크기
        self.reservoir = []
        self.n = 0  # 전체 데이터 수
        
    def add_item(self, item):
        self.n += 1
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            # 확률 k/n으로 교체
            j = random.randint(0, self.n - 1)
            if j < self.k:
                self.reservoir[j] = item
```

#### 3.1.2 알고리즘 선택 이유
- **메모리 효율성**: O(k) 메모리 사용
- **균등 분포**: 각 아이템이 동일한 확률로 선택
- **스트리밍 처리**: 데이터 스트림에서 실시간 샘플링 가능

### 3.2 구현 세부사항

#### 3.2.1 카테고리별 샘플링
```python
class CategoryReservoirSampler:
    def __init__(self, k_per_category: int):
        self.k_per_category = k_per_category
        self.samplers = {}  # category_id -> ReservoirSampler
        
    def add_video(self, video_data):
        category_id = video_data['category_id']
        if category_id not in self.samplers:
            self.samplers[category_id] = ReservoirSampler(self.k_per_category)
        self.samplers[category_id].add_item(video_data)
```

#### 3.2.2 시간 윈도우 기반 샘플링
```python
class TimeWindowSampler:
    def __init__(self, k: int, window_hours: int = 24):
        self.k = k
        self.window_hours = window_hours
        self.samplers = {}  # hour -> ReservoirSampler
        
    def add_video(self, video_data):
        hour = video_data['published_at'].hour
        if hour not in self.samplers:
            self.samplers[hour] = ReservoirSampler(self.k)
        self.samplers[hour].add_item(video_data)
```

## 4. Bloom Filter 설계

### 4.1 Bloom Filter 개요

#### 4.1.1 기본 원리
- **비트 배열**: m 크기의 비트 배열 사용
- **해시 함수**: k개의 독립적인 해시 함수 사용
- **False Positive**: 존재하지 않는 데이터를 존재한다고 판단할 확률

#### 4.1.2 파라미터 최적화
```python
def optimal_bloom_filter_params(n: int, p: float):
    """
    n: 예상 데이터 수
    p: 허용 가능한 False Positive 확률
    """
    m = -(n * math.log(p)) / (math.log(2) ** 2)  # 비트 배열 크기
    k = (m / n) * math.log(2)  # 해시 함수 개수
    return int(m), int(k)
```

### 4.2 구현 세부사항

#### 4.2.1 동적 Bloom Filter
```python
class DynamicBloomFilter:
    def __init__(self, initial_capacity: int, error_rate: float = 0.01):
        self.capacity = initial_capacity
        self.error_rate = error_rate
        self.m, self.k = optimal_bloom_filter_params(initial_capacity, error_rate)
        self.bit_array = [0] * self.m
        self.hash_functions = self._create_hash_functions()
        self.count = 0
        
    def add(self, item: str):
        for i in range(self.k):
            hash_val = self.hash_functions[i](item) % self.m
            self.bit_array[hash_val] = 1
        self.count += 1
        
    def might_contain(self, item: str) -> bool:
        for i in range(self.k):
            hash_val = self.hash_functions[i](item) % self.m
            if self.bit_array[hash_val] == 0:
                return False
        return True
```

#### 4.2.2 시간 기반 Bloom Filter
```python
class TimeBasedBloomFilter:
    def __init__(self, window_days: int = 7):
        self.window_days = window_days
        self.filters = {}  # date -> BloomFilter
        
    def add_with_timestamp(self, item: str, timestamp: datetime):
        date = timestamp.date()
        if date not in self.filters:
            self.filters[date] = DynamicBloomFilter(100000, 0.01)
        self.filters[date].add(item)
        
    def might_contain_recent(self, item: str, current_time: datetime) -> bool:
        cutoff_date = (current_time - timedelta(days=self.window_days)).date()
        for date, filter_obj in self.filters.items():
            if date >= cutoff_date:
                if filter_obj.might_contain(item):
                    return True
        return False
```

## 5. Bronze Layer 설계

### 5.1 데이터 레이크 구조

#### 5.1.1 파티셔닝 전략
```
data/
├── bronze/
│   ├── videos/
│   │   ├── ingest_date=2025-01-01/
│   │   │   ├── source=youtube/
│   │   │   │   ├── part-00000.parquet
│   │   │   │   └── part-00001.parquet
│   │   │   └── source=social/
│   │   │       └── part-00000.parquet
│   │   └── ingest_date=2025-01-02/
│   └── samples/
│       ├── reservoir_samples/
│       └── bloom_filters/
```

#### 5.1.2 스키마 진화
```python
# Bronze Layer 스키마 정의
bronze_schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("published_at", TimestampType(), True),
    StructField("duration", StringType(), True),
    StructField("view_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("dislike_count", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("thumbnails", MapType(StringType(), StringType()), True),
    StructField("ingest_timestamp", TimestampType(), False),
    StructField("source", StringType(), False),
    StructField("data_quality_score", DoubleType(), True)
])
```

### 5.2 데이터 품질 관리

#### 5.2.1 데이터 검증 규칙
```python
def validate_bronze_data(df: DataFrame) -> DataFrame:
    """Bronze Layer 데이터 검증"""
    
    # 필수 필드 검증
    required_fields = ["video_id", "title", "channel_id", "published_at"]
    for field in required_fields:
        df = df.filter(col(field).isNotNull())
    
    # 데이터 타입 검증
    df = df.filter(col("view_count").cast("long").isNotNull())
    df = df.filter(col("like_count").cast("long").isNotNull())
    df = df.filter(col("comment_count").cast("long").isNotNull())
    
    # 데이터 범위 검증
    df = df.filter(col("view_count") >= 0)
    df = df.filter(col("like_count") >= 0)
    df = df.filter(col("comment_count") >= 0)
    
    # 중복 제거
    df = df.dropDuplicates(["video_id"])
    
    return df
```

#### 5.2.2 데이터 품질 점수 계산
```python
def calculate_data_quality_score(row) -> float:
    """데이터 품질 점수 계산 (0-1)"""
    score = 1.0
    
    # 필수 필드 존재 여부
    required_fields = ["title", "description", "channel_title"]
    for field in required_fields:
        if not row[field] or row[field].strip() == "":
            score -= 0.2
    
    # 통계 데이터 완성도
    if row["view_count"] is None:
        score -= 0.3
    if row["like_count"] is None:
        score -= 0.1
    if row["comment_count"] is None:
        score -= 0.1
    
    # 태그 정보 존재 여부
    if not row["tags"] or len(row["tags"]) == 0:
        score -= 0.1
    
    return max(0.0, score)
```

## 6. 배치 ETL 파이프라인

### 6.1 파이프라인 구조

#### 6.1.1 주요 단계
1. **데이터 수집**: YouTube API 호출
2. **샘플링**: Reservoir Sampling 적용
3. **중복 필터링**: Bloom Filter 적용
4. **데이터 검증**: 스키마 및 품질 검증
5. **Bronze 저장**: Delta Lake 저장

#### 6.1.2 에러 처리 및 재시도
```python
class YouTubeDataCollector:
    def __init__(self, api_key: str, max_retries: int = 3):
        self.api_key = api_key
        self.max_retries = max_retries
        self.session = requests.Session()
        
    def collect_videos(self, query: str, max_results: int = 50):
        for attempt in range(self.max_retries):
            try:
                response = self._make_api_request(query, max_results)
                return self._parse_response(response)
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # 지수 백오프
```

### 6.2 성능 최적화

#### 6.2.1 병렬 처리
```python
def parallel_data_collection(queries: List[str], max_workers: int = 5):
    """병렬 데이터 수집"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(collect_videos, query) for query in queries]
        results = [future.result() for future in futures]
    return results
```

#### 6.2.2 배치 크기 최적화
```python
def optimize_batch_size(api_quota: int, processing_time: float) -> int:
    """API 할당량과 처리 시간을 고려한 최적 배치 크기 계산"""
    max_batch_size = min(50, api_quota // 10)  # API 제한 고려
    optimal_batch_size = max_batch_size
    
    # 처리 시간 기반 조정
    if processing_time > 300:  # 5분 초과 시
        optimal_batch_size = max_batch_size // 2
    
    return optimal_batch_size
```

## 7. 모니터링 및 로깅

### 7.1 수집 메트릭
- **수집량**: 시간당 수집된 비디오 수
- **성공률**: API 호출 성공률
- **처리 시간**: 배치당 평균 처리 시간
- **에러율**: 실패한 요청 비율

### 7.2 알림 설정
- **임계값 기반 알림**: 에러율 5% 초과 시 알림
- **할당량 모니터링**: API 할당량 90% 사용 시 알림
- **데이터 품질**: 품질 점수 0.8 미만 시 알림

## 8. 구현 코드 예시

### 8.1 메인 수집 스크립트
```python
def main():
    # 설정 로드
    config = load_config()
    
    # Reservoir Sampler 초기화
    reservoir_sampler = CategoryReservoirSampler(k_per_category=1000)
    
    # Bloom Filter 초기화
    bloom_filter = TimeBasedBloomFilter(window_days=7)
    
    # YouTube API 수집기 초기화
    collector = YouTubeDataCollector(config.api_key)
    
    # 데이터 수집 및 처리
    for query in config.search_queries:
        videos = collector.collect_videos(query)
        
        for video in videos:
            # 중복 체크
            if not bloom_filter.might_contain_recent(video['video_id'], datetime.now()):
                # Reservoir Sampling
                reservoir_sampler.add_video(video)
                
                # Bloom Filter 업데이트
                bloom_filter.add_with_timestamp(video['video_id'], datetime.now())
    
    # Bronze Layer 저장
    save_to_bronze_layer(reservoir_sampler.get_all_samples())
```

---

**이전 문서**: [00_프로젝트_개요_및_아키텍처.md](./00_프로젝트_개요_및_아키텍처.md)  
**다음 문서**: [02_스트리밍_데이터_처리_설계.md](./02_스트리밍_데이터_처리_설계.md)

