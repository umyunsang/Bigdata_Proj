# 데이터 수집 전략 (2024년 K-POP YouTube 영상)

## 📅 일정

- **시작일**: 2024년 10월 25일
- **종료일**: 2024년 10월 30일 (제출 마감)
- **수집 기간**: 5일
- **최종 처리**: 10월 29~30일

## 🎯 목표

- **대상 기간**: 2024년 1월 1일 ~ 2024년 12월 31일
- **목표 영상 수**: 20,000 ~ 25,000개
- **일일 할당량**: YouTube Data API v3 10,000 QPD
- **일일 사용**: 약 9,500 쿼터/일
- **일일 수집량**: 약 4,500 ~ 5,000개 영상

## 📊 수집 방법

### 1. 랜덤 샘플링 전략

**쿼리 파라미터 랜덤화**:

```python
# 쿼리 키워드 (q)
artists = [
    "BLACKPINK", "NewJeans", "BTS", "IVE", "aespa",
    "TWICE", "LESSERAFIM", "ITZY", "Stray Kids", "NMIXX",
    "SEVENTEEN", "NCT", "ENHYPEN", "ATEEZ", "BABYMONSTER",
    "2NE1", "Red Velvet", "TREASURE", "TXT", "G-IDLE"
]
# 매 요청마다 랜덤 선택

# 정렬 방식 (order)
order_types = ["relevance", "viewCount", "date", "rating"]
# 매 요청마다 랜덤 선택

# 발행 기간 (publishedAfter/Before)
quarters = [
    ("2024-01-01T00:00:00Z", "2024-03-31T23:59:59Z"),  # Q1
    ("2024-04-01T00:00:00Z", "2024-06-30T23:59:59Z"),  # Q2
    ("2024-07-01T00:00:00Z", "2024-09-30T23:59:59Z"),  # Q3
    ("2024-10-01T00:00:00Z", "2024-12-31T23:59:59Z"),  # Q4
]
# 매 요청마다 랜덤 분기 선택
```

### 2. API 호출 전략

**비용 계산**:
- `search.list`: 100 쿼터/호출
- `maxResults`: 50 (최대값)
- 일일 9,500 쿼터 = 95회 호출 = 4,750개 영상

**요청 구조**:
```python
youtube.search().list(
    part="snippet",
    q=random.choice(artists),
    type="video",
    maxResults=50,
    order=random.choice(order_types),
    publishedAfter=random.choice(quarters)[0],
    publishedBefore=random.choice(quarters)[1],
    regionCode="KR",
    relevanceLanguage="ko"
)
```

### 3. 중복 제거 전략

- **Bloom Filter 사용**: Bronze 단계에서 `video_id` 기반 중복 체크
- **일일 CSV 병합**: 기존 데이터와 병합 시 `video_id` 중복 제거
- **최종 데이터셋**: 순수 유니크 영상만 포함

## 📁 데이터 저장 구조

```
project/
└── data/
    └── raw/
        ├── youtube_random_2024.csv          # 전체 병합 파일
        ├── youtube_random_2024_1025.csv     # 일별 수집 파일
        ├── youtube_random_2024_1026.csv
        ├── youtube_random_2024_1027.csv
        ├── youtube_random_2024_1028.csv
        └── youtube_random_2024_1029.csv
```

### CSV 스키마

```csv
video_id,channel_id,title,description,published_at,view_count,like_count,comment_count,artist,quarter,collected_at
v123456,UC123,NewJeans - OMG MV,...,2024-01-15T10:00:00Z,1000000,50000,5000,NEWJEANS,2024-Q1,2024-10-25T09:00:00Z
```

## 🔄 일일 프로세스

### 수집 단계 (10월 25~29일)

1. **08:00 AM**: 스크립트 시작
   ```bash
   python scripts/daily_random_collection.py --quota 9500
   ```

2. **처리 과정**:
   - 95회 랜덤 API 호출 (각 50개 결과)
   - 실시간 Bloom Filter 중복 체크
   - CSV 파일 저장 (`youtube_random_2024_MMDD.csv`)

3. **병합**:
   ```bash
   python scripts/merge_daily_data.py
   ```
   - 일일 CSV를 `youtube_random_2024.csv`에 병합
   - `video_id` 기준 중복 제거

4. **로그 기록**:
   - 수집 건수, 중복 건수, 순수 증가량 기록
   - QPD 사용량 모니터링

### 최종 처리 단계 (10월 29~30일)

#### 10월 29일: 데이터 정제 및 Bronze

```bash
# 1. 최종 데이터 검증
python scripts/validate_data.py --input project/data/raw/youtube_random_2024.csv

# 2. Bronze 단계 실행
python -m video_social_rtp.cli bronze --batch-mode
```

**Bronze 출력**:
- Delta Lake: `project/data/bronze/`
- 스키마: `post_id, video_id, author_id, text, ts, artist, ingest_date`
- 파티션: `(ingest_date, artist)`

#### 10월 30일: Silver/Gold 집계 및 UI 반영

```bash
# 1. Silver 분기별 집계
python -m video_social_rtp.cli silver --batch-mode

# 2. Gold 특징 생성
python -m video_social_rtp.cli gold --top-pct 0.9

# 3. UI 확인
python -m video_social_rtp.cli ui --port 8502
```

**분석 결과**:
- Silver: 분기별(Q1~Q4) 집계, QoQ 성장률, 시장 점유율
- Gold: 아티스트별 Tier, 백분위수, 트렌드 방향
- UI: 5개 탭(분포/순위/성장률/상세/예측&CDF)

## 📈 예상 결과

### 데이터 통계

| 항목 | 예상값 |
|------|--------|
| 총 영상 수 | 20,000 ~ 25,000 |
| 유니크 아티스트 | 15 ~ 20 |
| 분기별 데이터 | Q1, Q2, Q3, Q4 |
| 평균 분기당 영상 | 5,000 ~ 6,000 |

### 분석 지표

- **QoQ 성장률**: 분기별 Engagement 변화율
- **시장 점유율**: 분기별 아티스트 비중
- **Tier 분류**: Percentile 기반 1~4 Tier
- **트렌드**: UP/DOWN/STEADY (±10% 기준)

## 🚨 리스크 관리

### QPD 초과 방지

- **Rate Limiting**: 요청 간 1초 지연
- **일일 모니터링**: 9,500 쿼터 도달 시 자동 중단
- **예비 계획**: QPD 초과 시 다음날로 이월

### API 오류 처리

- **Retry 로직**: 3회 재시도 (exponential backoff)
- **오류 로그**: API 오류 별도 기록
- **Fallback**: Mock 데이터 생성 (API 장애 시)

### 데이터 품질 관리

- **필수 필드 검증**: `video_id`, `published_at`, `title` 필수
- **중복 제거**: Bloom Filter + CSV dedupe
- **아티스트 추출**: 제목 기반 패턴 매칭 (20개 주요 아티스트)

## 📝 실행 체크리스트

### 수집 전 (10월 25일)

- [ ] YouTube Data API v3 키 유효성 확인
- [ ] `scripts/daily_random_collection.py` 작성 완료
- [ ] `project/data/raw/` 디렉터리 생성
- [ ] Bloom Filter 초기화

### 수집 중 (10월 25~29일)

- [ ] Day 1: 4,500+ 영상 수집
- [ ] Day 2: 누적 9,000+ 영상
- [ ] Day 3: 누적 13,500+ 영상
- [ ] Day 4: 누적 18,000+ 영상
- [ ] Day 5: 누적 22,000+ 영상

### 최종 처리 (10월 29~30일)

- [ ] 10월 29일: Bronze 단계 완료
- [ ] 10월 30일: Silver/Gold 완료
- [ ] 10월 30일: Streamlit UI 최종 확인
- [ ] 제출 직전: 문서화 업데이트

## 🎓 학술적 근거

### 랜덤 샘플링의 타당성

- **대표성**: 아티스트, 정렬 방식, 시간 구간 랜덤화로 편향 최소화
- **효율성**: QPD 한도 내 최대 데이터 확보
- **재현성**: 동일 전략 반복 시 유사한 분포 획득

### 분기별 분석의 의의

- **계절성 파악**: K-POP은 분기별 컴백 주기 존재
- **성장 추세**: QoQ 지표로 장기 트렌드 파악
- **비교 가능성**: 분기 단위는 표준 비즈니스 분석 단위

## 📚 참고 문헌

- YouTube Data API v3 Documentation
- Vitter's Algorithm R (Reservoir Sampling)
- Bloom Filter for Large-Scale Deduplication
- Delta Lake Best Practices

---

**문서 버전**: 1.0
**최종 수정**: 2024-10-24
**작성자**: student_15030
