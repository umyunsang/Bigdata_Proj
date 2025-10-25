# 데이터 수집 진행 상황

## Day 1: 2025-10-25

### 실행 정보
- **시작 시간**: 2025-10-25 05:18:27 (KST)
- **프로세스 ID**: 1113387
- **로그 파일**: `logs/collection/day1_20251025.log`

### 목표
- API 호출: 95회
- QPD 사용: 9,500
- 예상 수집량: 약 4,750개 영상

### 현재 진행 상황 (05:18:53 기준)
- ✅ **프로세스 상태**: Running
- 📊 **API 호출**: 16 / 95 (16.8%)
- 🎬 **수집된 영상**: 607개 (유니크)
- 📈 **QPD 사용**: 1,600 / 9,500 (16.8%)

### 예상 완료 시간
- 1회 호출 평균 시간: 약 2초 (API 호출 + 1초 delay)
- 남은 호출: 79회
- 예상 소요 시간: 약 158초 (약 2.6분)
- **예상 완료**: 2025-10-25 05:21:31 (KST)

### 출력 파일
수집 완료 후 생성될 파일:
1. **Landing**: `project/data/landing/events_*.json` (95개 NDJSON 파일)
2. **Daily CSV**: `project/data/raw/youtube_random_2024_20251025.csv`
3. **Master CSV**: `project/data/raw/youtube_random_2024.csv`

### 모니터링 명령어
```bash
# 실시간 로그 확인
tail -f logs/collection/day1_20251025.log

# 진행 상황 확인
echo "=== Progress ===" && \
grep "Unique videos:" logs/collection/day1_20251025.log | tail -1 && \
grep "quota used:" logs/collection/day1_20251025.log | tail -1
```

---

## 다음 단계

### Day 2-5 수집 (10월 26-29일)
각 날짜에 동일한 명령어 실행:
```bash
.venv/bin/python scripts/daily_random_collection.py \
    --quota 9500 \
    --max-results 50 \
    --delay 1.0 \
    > logs/collection/day${N}_$(date +%Y%m%d).log 2>&1 &
```

### 최종 처리 (10월 29-30일)
```bash
# 10월 29일: Bronze 처리
python -m video_social_rtp.cli bronze

# 10월 30일: Silver/Gold 처리
python -m video_social_rtp.cli silver --once
python -m video_social_rtp.cli gold --top-pct 0.9

# UI 최종 확인
python -m video_social_rtp.cli ui --port 8502
```

---

**문서 업데이트**: 2025-10-25 05:19:00
**다음 업데이트**: Day 1 완료 시
